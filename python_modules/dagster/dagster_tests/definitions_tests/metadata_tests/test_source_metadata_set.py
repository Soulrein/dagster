from dagster._core.definitions.events import AssetMaterialization
from dagster._core.definitions.metadata import (
    DEFAULT_SOURCE_FILE_KEY,
    SourceDataMetadataSet,
    SourcePathMetadataSet,
)


def test_source_metadata_set() -> None:
    source_metadata = SourceDataMetadataSet(
        source_paths={
            DEFAULT_SOURCE_FILE_KEY: SourcePathMetadataSet(
                file_path="/Users/dagster/Documents/my_module/assets/my_asset.py",
                line_number=12,
            )
        }
    )

    dict_source_metadata = dict(source_metadata)
    assert dict_source_metadata == {"dagster/source_paths": source_metadata.source_paths}
    assert len(dict_source_metadata["dagster/source_paths"]) == 1
    assert isinstance(
        dict_source_metadata["dagster/source_paths"][DEFAULT_SOURCE_FILE_KEY], SourcePathMetadataSet
    )
    AssetMaterialization(asset_key="a", metadata=dict_source_metadata)

    splat_source_metadata = {**source_metadata}
    assert splat_source_metadata == {"dagster/source_paths": source_metadata.source_paths}
    assert len(splat_source_metadata["dagster/source_paths"]) == 1
    assert isinstance(
        splat_source_metadata["dagster/source_paths"][DEFAULT_SOURCE_FILE_KEY],
        SourcePathMetadataSet,
    )
    AssetMaterialization(asset_key="a", metadata=splat_source_metadata)

    assert dict(SourceDataMetadataSet()) == {"dagster/source_paths": {}}
    assert SourceDataMetadataSet.extract(dict(SourceDataMetadataSet())) == SourceDataMetadataSet()
