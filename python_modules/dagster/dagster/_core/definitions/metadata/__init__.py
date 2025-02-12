import os
from abc import ABC, abstractmethod
from datetime import datetime
from typing import (
    AbstractSet,
    Any,
    Callable,
    Dict,
    Generic,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)

from pydantic import Field
from typing_extensions import Self, TypeAlias, TypeVar

import dagster._check as check
import dagster._seven as seven
from dagster._annotations import PublicAttr, deprecated, deprecated_param, experimental, public
from dagster._core.definitions.asset_key import AssetKey
from dagster._core.errors import DagsterInvalidMetadata
from dagster._model import DagsterModel
from dagster._model.pydantic_compat_layer import model_fields
from dagster._serdes import whitelist_for_serdes
from dagster._serdes.serdes import (
    FieldSerializer,
    PackableValue,
    UnpackContext,
    WhitelistMap,
    pack_value,
)
from dagster._utils.warnings import (
    deprecation_warning,
    normalize_renamed_param,
)

from .table import (  # re-exported
    TableColumn as TableColumn,
    TableColumnConstraints as TableColumnConstraints,
    TableColumnDep as TableColumnDep,
    TableColumnLineage as TableColumnLineage,
    TableConstraints as TableConstraints,
    TableRecord as TableRecord,
    TableSchema as TableSchema,
)

ArbitraryMetadataMapping: TypeAlias = Mapping[str, Any]

RawMetadataValue = Union[
    "MetadataValue",
    TableSchema,
    AssetKey,
    os.PathLike,
    Dict[Any, Any],
    float,
    int,
    List[Any],
    str,
    datetime,
    None,
]

MetadataMapping: TypeAlias = Mapping[str, "MetadataValue"]
RawMetadataMapping: TypeAlias = Mapping[str, RawMetadataValue]

T_Packable = TypeVar("T_Packable", bound=PackableValue, default=PackableValue, covariant=True)


# ########################
# ##### NORMALIZATION
# ########################


def normalize_metadata(
    metadata: Mapping[str, RawMetadataValue],
    allow_invalid: bool = False,
) -> Mapping[str, "MetadataValue"]:
    # This is a stopgap measure to deal with unsupported metadata values, which occur when we try
    # to convert arbitrary metadata (on e.g. OutputDefinition) to a MetadataValue, which is required
    # for serialization. This will cause unsupported values to be silently replaced with a
    # string placeholder.
    normalized_metadata: Dict[str, MetadataValue] = {}
    for k, v in metadata.items():
        try:
            normalized_value = normalize_metadata_value(v)
        except DagsterInvalidMetadata as e:
            if allow_invalid:
                deprecation_warning(
                    "Support for arbitrary metadata values",
                    "2.0.0",
                    additional_warn_text=(
                        "In the future, all user-supplied metadata values must be one of"
                        f" {RawMetadataValue}"
                    ),
                    stacklevel=4,  # to get the caller of `normalize_metadata`
                )
                normalized_value = TextMetadataValue(f"[{v.__class__.__name__}] (unserializable)")
            else:
                raise DagsterInvalidMetadata(
                    f'Could not resolve the metadata value for "{k}" to a known type. {e}'
                ) from None
        normalized_metadata[k] = normalized_value

    return normalized_metadata


def has_corresponding_metadata_value_class(obj: Any) -> bool:
    return isinstance(obj, (str, float, bool, int, list, dict, os.PathLike, AssetKey, TableSchema))


def normalize_metadata_value(raw_value: RawMetadataValue) -> "MetadataValue[Any]":
    if isinstance(raw_value, MetadataValue):
        return raw_value
    elif isinstance(raw_value, str):
        return MetadataValue.text(raw_value)
    elif isinstance(raw_value, float):
        return MetadataValue.float(raw_value)
    elif isinstance(raw_value, bool):
        return MetadataValue.bool(raw_value)
    elif isinstance(raw_value, int):
        return MetadataValue.int(raw_value)
    elif isinstance(raw_value, (list, dict)):
        return MetadataValue.json(raw_value)
    elif isinstance(raw_value, os.PathLike):
        return MetadataValue.path(raw_value)
    elif isinstance(raw_value, AssetKey):
        return MetadataValue.asset(raw_value)
    elif isinstance(raw_value, TableSchema):
        return MetadataValue.table_schema(raw_value)
    elif isinstance(raw_value, TableColumnLineage):
        return MetadataValue.column_lineage(raw_value)
    elif raw_value is None:
        return MetadataValue.null()

    raise DagsterInvalidMetadata(
        f"Its type was {type(raw_value)}. Consider wrapping the value with the appropriate "
        "MetadataValue type."
    )


# ########################
# ##### METADATA VALUE
# ########################


class MetadataValue(ABC, Generic[T_Packable]):
    """Utility class to wrap metadata values passed into Dagster events so that they can be
    displayed in the Dagster UI and other tooling.

    .. code-block:: python

        @op
        def emit_metadata(context, df):
            yield AssetMaterialization(
                asset_key="my_dataset",
                metadata={
                    "my_text_label": "hello",
                    "dashboard_url": MetadataValue.url("http://mycoolsite.com/my_dashboard"),
                    "num_rows": 0,
                },
            )
    """

    @public
    @property
    @abstractmethod
    def value(self) -> T_Packable:
        """The wrapped value."""
        raise NotImplementedError()

    @public
    @staticmethod
    def text(text: str) -> "TextMetadataValue":
        """Static constructor for a metadata value wrapping text as
        :py:class:`TextMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context, df):
                    yield AssetMaterialization(
                        asset_key="my_dataset",
                        metadata={
                            "my_text_label": MetadataValue.text("hello")
                        },
                    )

        Args:
            text (str): The text string for a metadata entry.
        """
        return TextMetadataValue(text)

    @public
    @staticmethod
    def url(url: str) -> "UrlMetadataValue":
        """Static constructor for a metadata value wrapping a URL as
        :py:class:`UrlMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context):
                    yield AssetMaterialization(
                        asset_key="my_dashboard",
                        metadata={
                            "dashboard_url": MetadataValue.url("http://mycoolsite.com/my_dashboard"),
                        }
                    )

        Args:
            url (str): The URL for a metadata entry.
        """
        return UrlMetadataValue(url)

    @public
    @staticmethod
    def path(path: Union[str, os.PathLike]) -> "PathMetadataValue":
        """Static constructor for a metadata value wrapping a path as
        :py:class:`PathMetadataValue`.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context):
                    yield AssetMaterialization(
                        asset_key="my_dataset",
                        metadata={
                            "filepath": MetadataValue.path("path/to/file"),
                        }
                    )

        Args:
            path (str): The path for a metadata entry.
        """
        return PathMetadataValue(path)

    @public
    @staticmethod
    def notebook(path: Union[str, os.PathLike]) -> "NotebookMetadataValue":
        """Static constructor for a metadata value wrapping a notebook path as
        :py:class:`NotebookMetadataValue`.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context):
                    yield AssetMaterialization(
                        asset_key="my_dataset",
                        metadata={
                            "notebook_path": MetadataValue.notebook("path/to/notebook.ipynb"),
                        }
                    )

        Args:
            path (str): The path to a notebook for a metadata entry.
        """
        return NotebookMetadataValue(path)

    @public
    @staticmethod
    def json(data: Union[Sequence[Any], Mapping[str, Any]]) -> "JsonMetadataValue":
        """Static constructor for a metadata value wrapping a json-serializable list or dict
        as :py:class:`JsonMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context):
                    yield ExpectationResult(
                        success=not missing_things,
                        label="is_present",
                        metadata={
                            "about my dataset": MetadataValue.json({"missing_columns": missing_things})
                        },
                    )

        Args:
            data (Union[Sequence[Any], Mapping[str, Any]]): The JSON data for a metadata entry.
        """
        return JsonMetadataValue(data)

    @public
    @staticmethod
    def md(data: str) -> "MarkdownMetadataValue":
        """Static constructor for a metadata value wrapping markdown data as
        :py:class:`MarkdownMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events.


        Example:
            .. code-block:: python

                @op
                def emit_metadata(context, md_str):
                    yield AssetMaterialization(
                        asset_key="info",
                        metadata={
                            'Details': MetadataValue.md(md_str)
                        },
                    )

        Args:
            md_str (str): The markdown for a metadata entry.
        """
        return MarkdownMetadataValue(data)

    @public
    @staticmethod
    def python_artifact(python_artifact: Callable[..., Any]) -> "PythonArtifactMetadataValue":
        """Static constructor for a metadata value wrapping a python artifact as
        :py:class:`PythonArtifactMetadataValue`. Can be used as the value type for the
        `metadata` parameter for supported events.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context, df):
                    yield AssetMaterialization(
                        asset_key="my_dataset",
                        metadata={
                            "class": MetadataValue.python_artifact(MyClass),
                            "function": MetadataValue.python_artifact(my_function),
                        }
                    )

        Args:
            value (Callable): The python class or function for a metadata entry.
        """
        check.callable_param(python_artifact, "python_artifact")
        return PythonArtifactMetadataValue(python_artifact.__module__, python_artifact.__name__)

    @public
    @staticmethod
    def float(value: float) -> "FloatMetadataValue":
        """Static constructor for a metadata value wrapping a float as
        :py:class:`FloatMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context, df):
                    yield AssetMaterialization(
                        asset_key="my_dataset",
                        metadata={
                            "size (bytes)": MetadataValue.float(calculate_bytes(df)),
                        }
                    )

        Args:
            value (float): The float value for a metadata entry.
        """
        return FloatMetadataValue(value)

    @public
    @staticmethod
    def int(value: int) -> "IntMetadataValue":
        """Static constructor for a metadata value wrapping an int as
        :py:class:`IntMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context, df):
                    yield AssetMaterialization(
                        asset_key="my_dataset",
                        metadata={
                            "number of rows": MetadataValue.int(len(df)),
                        },
                    )

        Args:
            value (int): The int value for a metadata entry.
        """
        return IntMetadataValue(value)

    @public
    @staticmethod
    def bool(value: bool) -> "BoolMetadataValue":
        """Static constructor for a metadata value wrapping a bool as
        :py:class:`BoolMetadataValuye`. Can be used as the value type for the `metadata`
        parameter for supported events.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context, df):
                    yield AssetMaterialization(
                        asset_key="my_dataset",
                        metadata={
                            "num rows > 1000": MetadataValue.bool(len(df) > 1000),
                        },
                    )

        Args:
            value (bool): The bool value for a metadata entry.
        """
        return BoolMetadataValue(value)

    @public
    @staticmethod
    def timestamp(value: Union["float", datetime]) -> "TimestampMetadataValue":
        """Static constructor for a metadata value wrapping a UNIX timestamp as a
        :py:class:`TimestampMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events.

        Args:
            value (Union[float, datetime]): The unix timestamp value for a metadata entry. If a
                datetime is provided, the timestamp will be extracted. datetimes without timezones
                are not accepted, because their timestamps can be ambiguous.
        """
        if isinstance(value, float):
            return TimestampMetadataValue(value)
        elif isinstance(value, datetime):
            if value.tzinfo is None:
                check.failed(
                    "Datetime values provided to MetadataValue.timestamp must have timezones, "
                    f"but {value.isoformat()} does not"
                )
            return TimestampMetadataValue(value.timestamp())
        else:
            check.failed(f"Expected either a float or a datetime, but received a {type(value)}")

    @public
    @staticmethod
    def dagster_run(run_id: str) -> "DagsterRunMetadataValue":
        """Static constructor for a metadata value wrapping a reference to a Dagster run.

        Args:
            run_id (str): The ID of the run.
        """
        return DagsterRunMetadataValue(run_id)

    @public
    @staticmethod
    def asset(asset_key: "AssetKey") -> "DagsterAssetMetadataValue":
        """Static constructor for a metadata value referencing a Dagster asset, by key.

        For example:

        .. code-block:: python

            @op
            def validate_table(context, df):
                yield AssetMaterialization(
                    asset_key=AssetKey("my_table"),
                    metadata={
                        "Related asset": MetadataValue.asset(AssetKey('my_other_table')),
                    },
                )

        Args:
            asset_key (AssetKey): The asset key referencing the asset.
        """
        from dagster._core.definitions.events import AssetKey

        check.inst_param(asset_key, "asset_key", AssetKey)
        return DagsterAssetMetadataValue(asset_key)

    @public
    @staticmethod
    def job(
        job_name: str,
        location_name: str,
        *,
        repository_name: Optional[str] = None,
    ) -> "DagsterJobMetadataValue":
        """Static constructor for a metadata value referencing a Dagster job, by name.

        For example:

        .. code-block:: python

            @op
            def emit_metadata(context, df):
                yield AssetMaterialization(
                    asset_key="my_dataset"
                    metadata={
                        "Producing job": MetadataValue.job('my_other_job'),
                    },
                )

        Args:
            job_name (str): The name of the job.
            location_name (Optional[str]): The code location name for the job.
            repository_name (Optional[str]): The repository name of the job, if different from the
                default.
        """
        return DagsterJobMetadataValue(
            job_name=check.str_param(job_name, "job_name"),
            location_name=check.str_param(location_name, "location_name"),
            repository_name=check.opt_str_param(repository_name, "repository_name"),
        )

    @public
    @staticmethod
    @experimental
    def table(
        records: Sequence[TableRecord], schema: Optional[TableSchema] = None
    ) -> "TableMetadataValue":
        """Static constructor for a metadata value wrapping arbitrary tabular data as
        :py:class:`TableMetadataValue`. Can be used as the value type for the `metadata`
        parameter for supported events.

        Example:
            .. code-block:: python

                @op
                def emit_metadata(context):
                    yield ExpectationResult(
                        success=not has_errors,
                        label="is_valid",
                        metadata={
                            "errors": MetadataValue.table(
                                records=[
                                    TableRecord(code="invalid-data-type", row=2, col="name"),
                                ],
                                schema=TableSchema(
                                    columns=[
                                        TableColumn(name="code", type="string"),
                                        TableColumn(name="row", type="int"),
                                        TableColumn(name="col", type="string"),
                                    ]
                                )
                            ),
                        },
                    )
        """
        return TableMetadataValue(records, schema)

    @public
    @staticmethod
    def table_schema(
        schema: TableSchema,
    ) -> "TableSchemaMetadataValue":
        """Static constructor for a metadata value wrapping a table schema as
        :py:class:`TableSchemaMetadataValue`. Can be used as the value type
        for the `metadata` parameter for supported events.

        Example:
            .. code-block:: python

                schema = TableSchema(
                    columns = [
                        TableColumn(name="id", type="int"),
                        TableColumn(name="status", type="bool"),
                    ]
                )

                DagsterType(
                    type_check_fn=some_validation_fn,
                    name='MyTable',
                    metadata={
                        'my_table_schema': MetadataValue.table_schema(schema),
                    }
                )

        Args:
            schema (TableSchema): The table schema for a metadata entry.
        """
        return TableSchemaMetadataValue(schema)

    @public
    @staticmethod
    def column_lineage(
        lineage: TableColumnLineage,
    ) -> "TableColumnLineageMetadataValue":
        """Static constructor for a metadata value wrapping a column lineage as
        :py:class:`TableColumnLineageMetadataValue`. Can be used as the value type
        for the `metadata` parameter for supported events.

        Args:
            lineage (TableColumnLineage): The column lineage for a metadata entry.
        """
        return TableColumnLineageMetadataValue(lineage)

    @public
    @staticmethod
    def null() -> "NullMetadataValue":
        """Static constructor for a metadata value representing null. Can be used as the value type
        for the `metadata` parameter for supported events.
        """
        return NullMetadataValue()


# ########################
# ##### METADATA VALUE TYPES
# ########################

# NOTE: We have `type: ignore` in a few places below because mypy complains about an instance method
# (e.g. `text`) overriding a static method on the superclass of the same name. This is not a concern
# for us because these static methods should never be called on instances.

# NOTE: `XMetadataValue` classes are serialized with a storage name of `XMetadataEntryData` to
# maintain backward compatibility. See docstring of `whitelist_for_serdes` for more info.


@whitelist_for_serdes(storage_name="TextMetadataEntryData")
class TextMetadataValue(DagsterModel, MetadataValue[str]):
    """Container class for text metadata entry data.

    Args:
        text (Optional[str]): The text data.
    """

    text_inner: Optional[str] = Field(..., alias="text")

    def __init__(self, text: Optional[str]):
        super().__init__(text=text or "")

    @public
    @property
    def text(self) -> Optional[str]:
        return self.text_inner

    @public
    @property
    def value(self) -> Optional[str]:
        """Optional[str]: The wrapped text data."""
        return self.text_inner


@whitelist_for_serdes(storage_name="UrlMetadataEntryData")
class UrlMetadataValue(DagsterModel, MetadataValue[str]):
    """Container class for URL metadata entry data.

    Args:
        url (Optional[str]): The URL as a string.
    """

    url_inner: Optional[str] = Field(..., alias="url")

    def __init__(self, url: Optional[str]):
        super().__init__(url=url or "")

    @public
    @property
    def value(self) -> Optional[str]:
        """Optional[str]: The wrapped URL."""
        return self.url_inner

    @public
    @property
    def url(self) -> Optional[str]:
        """Optional[str]: The wrapped URL."""
        return self.url_inner


@whitelist_for_serdes(storage_name="PathMetadataEntryData")
class PathMetadataValue(DagsterModel, MetadataValue[str]):
    """Container class for path metadata entry data.

    Args:
        path (Optional[str]): The path as a string or conforming to os.PathLike.
    """

    path_inner: Optional[str] = Field(..., alias="path")

    def __init__(self, path: Optional[Union[str, os.PathLike]]):
        super().__init__(path=check.opt_path_param(path, "path", default=""))

    @public
    @property
    def path(self) -> Optional[str]:
        return self.path_inner

    @public
    @property
    def value(self) -> Optional[str]:
        """Optional[str]: The wrapped path."""
        return self.path_inner


@whitelist_for_serdes(storage_name="NotebookMetadataEntryData")
class NotebookMetadataValue(DagsterModel, MetadataValue[str]):
    """Container class for notebook metadata entry data.

    Args:
        path (Optional[str]): The path to the notebook as a string or conforming to os.PathLike.
    """

    path_inner: Optional[str] = Field(..., alias="path")

    def __init__(self, path: Optional[Union[str, os.PathLike]]):
        super().__init__(path=check.opt_path_param(path, "path", default=""))

    @public
    @property
    def path(self) -> Optional[str]:
        return self.path_inner

    @public
    @property
    def value(self) -> Optional[str]:
        """Optional[str]: The wrapped path to the notebook as a string."""
        return self.path_inner


@whitelist_for_serdes(storage_name="JsonMetadataEntryData")
class JsonMetadataValue(DagsterModel, MetadataValue[Union[Sequence[Any], Mapping[str, Any]]]):
    """Container class for JSON metadata entry data.

    Args:
        data (Union[Sequence[Any], Dict[str, Any]]): The JSON data.
    """

    data: PublicAttr[Optional[Union[Sequence[Any], Mapping[str, Any]]]]

    def __init__(self, data: Optional[Union[Sequence[Any], Mapping[str, Any]]]):
        data = check.opt_inst_param(data, "data", (Sequence, Mapping))
        try:
            # check that the value is JSON serializable
            seven.dumps(data)
        except TypeError:
            raise DagsterInvalidMetadata("Value is not JSON serializable.")
        super().__init__(data=data)

    @public
    @property
    def value(self) -> Optional[Union[Sequence[Any], Mapping[str, Any]]]:
        """Optional[Union[Sequence[Any], Dict[str, Any]]]: The wrapped JSON data."""
        return self.data


@whitelist_for_serdes(storage_name="MarkdownMetadataEntryData")
class MarkdownMetadataValue(DagsterModel, MetadataValue[str]):
    """Container class for markdown metadata entry data.

    Args:
        md_str (Optional[str]): The markdown as a string.
    """

    md_str: PublicAttr[Optional[str]]

    def __init__(self, md_str: Optional[str]):
        super().__init__(md_str=md_str or "")

    @public
    @property
    def value(self) -> Optional[str]:
        """Optional[str]: The wrapped markdown as a string."""
        return self.md_str


# This should be deprecated or fixed so that `value` does not return itself.
@whitelist_for_serdes(storage_name="PythonArtifactMetadataEntryData")
class PythonArtifactMetadataValue(DagsterModel, MetadataValue["PythonArtifactMetadataValue"]):
    """Container class for python artifact metadata entry data.

    Args:
        module (str): The module where the python artifact can be found
        name (str): The name of the python artifact
    """

    module: PublicAttr[str]
    name: PublicAttr[str]

    def __init__(self, module: str, name: str):
        super().__init__(module=module, name=name)

    @public
    @property
    def value(self) -> Self:
        """PythonArtifactMetadataValue: Identity function."""
        return self


@whitelist_for_serdes(storage_name="FloatMetadataEntryData")
class FloatMetadataValue(DagsterModel, MetadataValue[float]):
    """Container class for float metadata entry data.

    Args:
        value (Optional[float]): The float value.
    """

    value_inner: Optional[float] = Field(..., alias="value")

    def __init__(self, value: Optional[float]):
        super().__init__(value=value)

    @public
    @property
    def value(self) -> Optional[float]:
        return self.value_inner


@whitelist_for_serdes(storage_name="IntMetadataEntryData")
class IntMetadataValue(DagsterModel, MetadataValue[int]):
    """Container class for int metadata entry data.

    Args:
        value (Optional[int]): The int value.
    """

    value_inner: Optional[int] = Field(..., alias="value")

    def __init__(self, value: Optional[int]):
        super().__init__(value=value)

    @public
    @property
    def value(self) -> Optional[int]:
        return self.value_inner


@whitelist_for_serdes(storage_name="BoolMetadataEntryData")
class BoolMetadataValue(DagsterModel, MetadataValue[bool]):
    """Container class for bool metadata entry data.

    Args:
        value (Optional[bool]): The bool value.
    """

    value_inner: Optional[bool] = Field(..., alias="value")

    def __init__(self, value: Optional[bool]):
        super().__init__(value=value)

    @public
    @property
    def value(self) -> Optional[bool]:
        return self.value_inner


@whitelist_for_serdes
class TimestampMetadataValue(DagsterModel, MetadataValue[float]):
    """Container class for metadata value that's a unix timestamp.

    Args:
        value (float): Seconds since the unix epoch.
    """

    value_inner: Optional[float] = Field(..., alias="value")

    def __init__(self, value: float):
        super().__init__(value=value)

    @public
    @property
    def value(self) -> Optional[float]:
        return self.value_inner


@whitelist_for_serdes(storage_name="DagsterPipelineRunMetadataEntryData")
class DagsterRunMetadataValue(DagsterModel, MetadataValue[str]):
    """Representation of a dagster run.

    Args:
        run_id (str): The run id
    """

    run_id: PublicAttr[str]

    def __init__(self, run_id: str):
        super().__init__(run_id=run_id)

    @public
    @property
    def value(self) -> str:
        """str: The wrapped run id."""
        return self.run_id


@whitelist_for_serdes
class DagsterJobMetadataValue(DagsterModel, MetadataValue["DagsterJobMetadataValue"]):
    """Representation of a dagster run.

    Args:
        job_name (str): The job's name
        location_name (str): The job's code location name
        repository_name (Optional[str]): The job's repository name. If not provided, the job is
            assumed to be in the same repository as this object.
    """

    job_name: PublicAttr[str]
    location_name: PublicAttr[str]
    repository_name: PublicAttr[Optional[str]]

    def __init__(
        self,
        job_name: str,
        location_name: str,
        repository_name: Optional[str] = None,
    ):
        super().__init__(
            job_name=job_name,
            location_name=location_name,
            repository_name=repository_name,
        )

    @public
    @property
    def value(self) -> Self:
        return self


@whitelist_for_serdes(storage_name="DagsterAssetMetadataEntryData")
class DagsterAssetMetadataValue(DagsterModel, MetadataValue[AssetKey]):
    """Representation of a dagster asset.

    Args:
        asset_key (AssetKey): The dagster asset key
    """

    asset_key: PublicAttr[AssetKey]

    def __init__(self, asset_key: AssetKey):
        super().__init__(asset_key=asset_key)

    @public
    @property
    def value(self) -> "AssetKey":
        """AssetKey: The wrapped :py:class:`AssetKey`."""
        return self.asset_key


# This should be deprecated or fixed so that `value` does not return itself.
@experimental
@whitelist_for_serdes(storage_name="TableMetadataEntryData")
class TableMetadataValue(DagsterModel, MetadataValue["TableMetadataValue"]):
    """Container class for table metadata entry data.

    Args:
        records (TableRecord): The data as a list of records (i.e. rows).
        schema (Optional[TableSchema]): A schema for the table.

    Example:
        .. code-block:: python

            from dagster import TableMetadataValue, TableRecord

            TableMetadataValue(
                schema=None,
                records=[
                    TableRecord({"column1": 5, "column2": "x"}),
                    TableRecord({"column1": 7, "column2": "y"}),
                ]
            )
    """

    records: PublicAttr[Sequence[TableRecord]]
    schema_inner: TableSchema = Field(..., alias="schema")

    @public
    @staticmethod
    def infer_column_type(value: object) -> str:
        """str: Infer the :py:class:`TableSchema` column type that will be used for a value."""
        if isinstance(value, bool):
            return "bool"
        elif isinstance(value, int):
            return "int"
        elif isinstance(value, float):
            return "float"
        else:
            return "string"

    def __init__(self, records: Sequence[TableRecord], schema: Optional[TableSchema] = None):
        check.sequence_param(records, "records", of_type=TableRecord)
        check.opt_inst_param(schema, "schema", TableSchema)

        if len(records) == 0:
            schema = check.not_none(schema, "schema must be provided if records is empty")
        else:
            columns = set(records[0].data.keys())
            for record in records[1:]:
                check.invariant(
                    set(record.data.keys()) == columns, "All records must have the same fields"
                )
            schema = schema or TableSchema(
                columns=[
                    TableColumn(name=k, type=TableMetadataValue.infer_column_type(v))
                    for k, v in records[0].data.items()
                ]
            )

        super().__init__(records=records, schema=schema)

    @public
    @property
    def schema(self) -> TableSchema:
        return self.schema_inner

    @public
    @property
    def value(self) -> Self:
        """TableMetadataValue: Identity function."""
        return self


@whitelist_for_serdes(storage_name="TableSchemaMetadataEntryData")
class TableSchemaMetadataValue(DagsterModel, MetadataValue[TableSchema]):
    """Representation of a schema for arbitrary tabular data.

    Args:
        schema (TableSchema): The dictionary containing the schema representation.
    """

    schema_inner: TableSchema = Field(..., alias="schema")

    def __init__(self, schema: TableSchema):
        super().__init__(schema=schema)

    @public
    @property
    def value(self) -> TableSchema:
        """TableSchema: The wrapped :py:class:`TableSchema`."""
        return self.schema_inner

    @public
    @property
    def schema(self) -> TableSchema:
        return self.schema_inner


@whitelist_for_serdes
class TableColumnLineageMetadataValue(DagsterModel, MetadataValue[TableColumnLineage]):
    """Representation of the lineage of column inputs to column outputs of arbitrary tabular data.

    Args:
        column_lineage (TableColumnLineage): The lineage of column inputs to column outputs
            for the table.
    """

    column_lineage_inner: TableColumnLineage = Field(..., alias="column_lineage")

    def __init__(self, column_lineage: TableColumnLineage):
        super().__init__(column_lineage=column_lineage)

    @public
    @property
    def value(self) -> TableColumnLineage:
        """TableSpec: The wrapped :py:class:`TableSpec`."""
        return self.column_lineage_inner


@whitelist_for_serdes(storage_name="NullMetadataEntryData")
class NullMetadataValue(DagsterModel, MetadataValue[None]):
    """Representation of null."""

    @public
    @property
    def value(self) -> None:
        """None: The wrapped null value."""
        return None


# ########################
# ##### METADATA BACKCOMPAT
# ########################

# Metadata used to be represented as a `List[MetadataEntry]`, but that class has been deleted. But
# we still serialize metadata dicts to the serialized representation of `List[MetadataEntry]` for
# backcompat purposes.


class MetadataFieldSerializer(FieldSerializer):
    """Converts between metadata dict (new) and metadata entries list (old)."""

    storage_name = "metadata_entries"
    loaded_name = "metadata"

    def pack(
        self,
        metadata_dict: Mapping[str, MetadataValue],
        whitelist_map: WhitelistMap,
        descent_path: str,
    ) -> Sequence[Mapping[str, Any]]:
        return [
            {
                "__class__": "EventMetadataEntry",
                "label": k,
                # MetadataValue itself can't inherit from NamedTuple and so isn't a PackableValue,
                # but one of its subclasses will always be returned here.
                "entry_data": pack_value(v, whitelist_map, descent_path),  # type: ignore
                "description": None,
            }
            for k, v in metadata_dict.items()
        ]

    def unpack(
        self,
        metadata_entries: List["MetadataEntry"],
        whitelist_map: WhitelistMap,
        context: UnpackContext,
    ) -> Mapping[str, MetadataValue]:
        return {e.label: e.entry_data for e in metadata_entries}


T_MetadataValue = TypeVar("T_MetadataValue", bound=MetadataValue, covariant=True)


# NOTE: MetadataEntry is no longer accessible via the public API-- all metadata APIs use metadata
# dicts. This clas shas only been preserved to adhere strictly to our backcompat guarantees. It is
# still instantiated in the above `MetadataFieldSerializer` but that can easily be changed.
@deprecated(
    breaking_version="2.0",
    additional_warn_text="Please use a dict with `MetadataValue` values instead.",
)
@deprecated_param(
    param="entry_data", breaking_version="2.0", additional_warn_text="Use `value` instead."
)
@whitelist_for_serdes(storage_name="EventMetadataEntry")
class MetadataEntry(
    NamedTuple(
        "_MetadataEntry",
        [
            ("label", PublicAttr[str]),
            ("description", PublicAttr[Optional[str]]),
            ("entry_data", PublicAttr[MetadataValue]),
        ],
    ),
    Generic[T_MetadataValue],
):
    """A structure for describing metadata for Dagster events.

    .. note:: This class is no longer usable in any Dagster API, and will be completely removed in 2.0.

    Lists of objects of this type can be passed as arguments to Dagster events and will be displayed
    in the Dagster UI and other tooling.

    Should be yielded from within an IO manager to append metadata for a given input/output event.
    For other event types, passing a dict with `MetadataValue` values to the `metadata` argument
    is preferred.

    Args:
        label (str): Short display label for this metadata entry.
        description (Optional[str]): A human-readable description of this metadata entry.
        value (MetadataValue): Typed metadata entry data. The different types allow
            for customized display in tools like the Dagster UI.
    """

    def __new__(
        cls,
        label: str,
        description: Optional[str] = None,
        entry_data: Optional["RawMetadataValue"] = None,
        value: Optional["RawMetadataValue"] = None,
    ):
        value = cast(
            RawMetadataValue,
            normalize_renamed_param(
                new_val=value,
                new_arg="value",
                old_val=entry_data,
                old_arg="entry_data",
            ),
        )
        value = normalize_metadata_value(value)

        return super(MetadataEntry, cls).__new__(
            cls,
            check.str_param(label, "label"),
            check.opt_str_param(description, "description"),
            check.inst_param(value, "value", MetadataValue),
        )

    @property
    def value(self):
        """Alias of `entry_data`."""
        return self.entry_data


T_NamespacedMetadataSet = TypeVar("T_NamespacedMetadataSet", bound="NamespacedMetadataSet")


class NamespacedMetadataSet(ABC, DagsterModel):
    """Extend this class to define a set of metadata fields in the same namespace.

    Supports splatting to a dictionary that can be placed inside a metadata argument along with
    other dictionary-structured metadata.

    .. code-block:: python

        my_metadata: NamespacedMetadataSet = ...
        return MaterializeResult(metadata={**my_metadata, ...})
    """

    @classmethod
    @abstractmethod
    def namespace(cls) -> str:
        raise NotImplementedError()

    @classmethod
    def _namespaced_key(cls, key: str) -> str:
        return f"{cls.namespace()}/{key}"

    @staticmethod
    def _strip_namespace_from_key(key: str) -> str:
        return key.split("/", 1)[1]

    def keys(self) -> AbstractSet[str]:
        return {
            self._namespaced_key(key)
            for key in model_fields(self).keys()
            # getattr returns the pydantic property on the subclass
            if getattr(self, key) is not None
        }

    def __getitem__(self, key: str) -> Any:
        # getattr returns the pydantic property on the subclass
        return getattr(self, self._strip_namespace_from_key(key))

    @classmethod
    def extract(
        cls: Type[T_NamespacedMetadataSet], metadata: Mapping[str, Any]
    ) -> T_NamespacedMetadataSet:
        """Extracts entries from the provided metadata dictionary into an instance of this class.

        Ignores any entries in the metadata dictionary whose keys don't correspond to fields on this
        class.

        In general, the following should always pass:

        .. code-block:: python

            class MyMetadataSet(NamedspacedMetadataSet):
                ...

            metadata: MyMetadataSet  = ...
            assert MyMetadataSet.extract(dict(metadata)) == metadata

        Args:
            metadata (Mapping[str, Any]): A dictionary of metadata entries.
        """
        kwargs = {}
        for namespaced_key, value in metadata.items():
            splits = namespaced_key.split("/")
            if len(splits) == 2:
                namespace, key = splits
                if namespace == cls.namespace() and key in model_fields(cls):
                    kwargs[key] = value.value if isinstance(value, MetadataValue) else value

        return cls(**kwargs)


class TableMetadataSet(NamespacedMetadataSet):
    """Metadata entries that apply to definitions, observations, or materializations of assets that
    are tables.

    Args:
        column_schema (Optional[TableSchema]): The schema of the columns in the table.
        column_lineage (Optional[TableColumnLineage]): The lineage of column inputs to column
            outputs for the table.
    """

    column_schema: Optional[TableSchema] = None
    column_lineage: Optional[TableColumnLineage] = None

    @classmethod
    def namespace(cls) -> str:
        return "dagster"
