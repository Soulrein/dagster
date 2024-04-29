from typing import cast

from dagster._core.definitions.events import AssetMaterialization
from dagster._core.definitions.metadata import (
    DEFAULT_SOURCE_FILE_KEY,
    LocalFileSource,
    SourceDataMetadataSet,
    SourceMetadataValue,
)


def test_source_metadata_set() -> None:
    source_metadata = SourceDataMetadataSet(
        source_data=SourceMetadataValue(
            sources={
                DEFAULT_SOURCE_FILE_KEY: LocalFileSource(
                    file_path="/Users/dagster/Documents/my_module/assets/my_asset.py",
                    line_number=12,
                )
            }
        )
    )

    dict_source_metadata = dict(source_metadata)
    assert dict_source_metadata == {"dagster/source_data": source_metadata.source_data}
    source_data = cast(SourceMetadataValue, dict_source_metadata["dagster/source_data"])
    assert len(source_data.sources) == 1
    assert isinstance(
        source_data.sources[DEFAULT_SOURCE_FILE_KEY],
        LocalFileSource,
    )
    AssetMaterialization(asset_key="a", metadata=dict_source_metadata)

    splat_source_metadata = {**source_metadata}
    assert splat_source_metadata == {"dagster/source_data": source_metadata.source_data}
    source_data = cast(SourceMetadataValue, splat_source_metadata["dagster/source_data"])
    assert len(source_data.sources) == 1
    assert isinstance(
        source_data.sources[DEFAULT_SOURCE_FILE_KEY],
        LocalFileSource,
    )
    AssetMaterialization(asset_key="a", metadata=splat_source_metadata)

    assert dict(SourceDataMetadataSet(source_data=SourceMetadataValue())) == {
        "dagster/source_data": SourceMetadataValue()
    }
    assert SourceDataMetadataSet.extract(
        dict(SourceDataMetadataSet(source_data=SourceMetadataValue()))
    ) == SourceDataMetadataSet(source_data=SourceMetadataValue())
