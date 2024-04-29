import inspect
import os
import sys
from types import ModuleType
from typing import (
    Any,
    Callable,
    NamedTuple,
    Optional,
)

import dagster._check as check
from dagster._annotations import PublicAttr, experimental
from dagster._serdes.serdes import (
    whitelist_for_serdes,
)


@experimental
@whitelist_for_serdes
class SourcePathMetadataSet(
    NamedTuple(
        "_SourcePathMetadataSet",
        [
            ("file_path", PublicAttr[str]),
            ("line_number", PublicAttr[int]),
        ],
    )
):
    """Metadata entries that apply to asset definitions and which specify a source
    filepath and line number for the asset.
    """

    file_path: str
    line_number: int


def _get_root_module(module: ModuleType) -> ModuleType:
    module_name_split = module.__name__.split(".")
    for i in range(1, len(module_name_split)):
        try:
            return sys.modules[".".join(module_name_split[:i])]
        except KeyError:
            continue
    return module


def source_path_from_fn(fn: Callable[..., Any]) -> Optional[SourcePathMetadataSet]:
    cwd = os.getcwd()
    origin_file: Optional[str] = None
    origin_line = None
    try:
        origin_file = os.path.abspath(os.path.join(cwd, inspect.getsourcefile(fn)))  # type: ignore
        origin_file = check.not_none(origin_file)
        origin_line = inspect.getsourcelines(fn)[1]
    except TypeError:
        return None

    return SourcePathMetadataSet(
        file_path=origin_file,
        line_number=origin_line,
    )
