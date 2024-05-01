import os
from typing import Any, Dict, cast

from dagster import (
    AssetExecutionContext,
    materialize,
)
from dagster._core.definitions.metadata import IntMetadataValue
from dagster_dbt.asset_decorator import dbt_assets
from dagster_dbt.core.resources_v2 import DbtCliResource, RowCountPostprocessingTask

from ...dbt_projects import test_jaffle_shop_path


def test_no_row_count(test_jaffle_shop_manifest: Dict[str, Any]) -> None:
    @dbt_assets(manifest=test_jaffle_shop_manifest)
    def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(["build"], context=context).stream()

    result = materialize(
        [my_dbt_assets],
        resources={"dbt": DbtCliResource(project_dir=os.fspath(test_jaffle_shop_path))},
    )

    assert result.success

    assert not any(
        "dagster/row_count" in event.materialization.metadata
        for event in result.get_asset_materialization_events()
    )


def test_row_count(test_jaffle_shop_manifest: Dict[str, Any]) -> None:
    @dbt_assets(manifest=test_jaffle_shop_manifest)
    def my_dbt_assets(context: AssetExecutionContext, dbt: DbtCliResource):
        yield from dbt.cli(
            ["build"], context=context, postprocessing_tasks=[RowCountPostprocessingTask()]
        ).stream()

    result = materialize(
        [my_dbt_assets],
        resources={"dbt": DbtCliResource(project_dir=os.fspath(test_jaffle_shop_path))},
    )

    assert result.success

    assert all(
        "dagster/row_count" in event.materialization.metadata
        for event in result.get_asset_materialization_events()
    )

    row_counts = [
        cast(IntMetadataValue, event.materialization.metadata["dagster/row_count"]).value
        for event in result.get_asset_materialization_events()
    ]
    assert all(row_count and row_count > 0 for row_count in row_counts), row_counts
