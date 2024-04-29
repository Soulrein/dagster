import os
from typing import cast

from dagster import AssetsDefinition, load_assets_from_modules
from dagster._core.definitions.metadata import (
    DEFAULT_SOURCE_FILE_KEY,
    LocalFileSource,
    with_code_source,
)
from dagster._utils import file_relative_path


def test_asset_code_origins() -> None:
    from dagster_tests.asset_defs_tests import asset_package

    from .asset_package import module_with_assets

    collection = load_assets_from_modules([asset_package, module_with_assets])

    for asset in collection:
        if isinstance(asset, AssetsDefinition):
            for key in asset.keys:
                assert "dagster/source_paths" not in asset.metadata_by_key[key]

    collection_with_source_metadata = with_code_source(collection)

    # path of the `dagster` module on the filesystem
    dagster_module_path = os.path.normpath(file_relative_path(__file__, "../../"))

    # path of the current file relative to the `dagster` module root
    path_in_module = "/dagster_tests/asset_defs_tests/"

    # {path to module}:{path to file relative to module root}:{line number}
    expected_origins = {
        "james_brown": dagster_module_path + path_in_module + "asset_package/__init__.py:12",
        "chuck_berry": (
            dagster_module_path + path_in_module + "asset_package/module_with_assets.py:11"
        ),
        "little_richard": (dagster_module_path + path_in_module + "asset_package/__init__.py:4"),
        "fats_domino": dagster_module_path + path_in_module + "asset_package/__init__.py:16",
        "miles_davis": (
            dagster_module_path
            + path_in_module
            + "asset_package/asset_subpackage/another_module_with_assets.py:6"
        ),
    }

    for asset in collection_with_source_metadata:
        if isinstance(asset, AssetsDefinition):
            op_name = asset.op.name
            assert op_name in expected_origins, f"Missing expected origin for op {op_name}"

            expected_file_path, expected_line_number = expected_origins[op_name].split(":")

            for key in asset.keys:
                assert "dagster/source_data" in asset.metadata_by_key[key]
                assert len(asset.metadata_by_key[key]["dagster/source_data"].sources) == 1
                assert isinstance(
                    asset.metadata_by_key[key]["dagster/source_data"].sources[
                        DEFAULT_SOURCE_FILE_KEY
                    ],
                    LocalFileSource,
                )
                meta = cast(
                    LocalFileSource,
                    asset.metadata_by_key[key]["dagster/source_data"].sources[
                        DEFAULT_SOURCE_FILE_KEY
                    ],
                )

                assert meta.file_path == expected_file_path
                assert meta.line_number == int(expected_line_number)
