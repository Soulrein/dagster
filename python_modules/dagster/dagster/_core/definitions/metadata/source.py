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
            ("path_to_module", PublicAttr[str]),
            ("path_from_module", PublicAttr[str]),
            ("line_number", PublicAttr[int]),
        ],
    )
):
    """Metadata entries that apply to asset definitions and which specify a source
    filepath and line number for the asset.
    """

    path_to_module: str
    path_from_module: str
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

        # Get the base module that the op function is defined in
        # and find the filepath to that module
        module = inspect.getmodule(fn)
        root_module = _get_root_module(module) if module else None
        path_to_module_root = (
            os.path.abspath(os.path.dirname(os.path.dirname(root_module.__file__)))
            if root_module and root_module.__file__
            else "/"
        )

        # Figure out where in the module the op function is defined
        path_from_module_root = os.path.relpath(origin_file, path_to_module_root)
    except TypeError:
        return None

    return SourcePathMetadataSet(
        path_to_module=path_to_module_root,
        path_from_module=path_from_module_root,
        line_number=origin_line,
    )
