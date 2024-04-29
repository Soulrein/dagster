from dagster._core.definitions.asset_key import AssetKey
from dagster._core.definitions.declarative_scheduling.asset_condition import (
    AssetCondition,
)
from dagster._core.definitions.events import AssetKeyPartitionKey

from ..scenario_specs import (
    daily_partitions_def,
    one_asset,
    time_partitions_start_datetime,
    two_partitions_def,
)
from .asset_condition_scenario import AssetConditionScenarioState


def test_in_latest_time_window_unpartitioned() -> None:
    state = AssetConditionScenarioState(
        one_asset, asset_condition=AssetCondition.in_latest_time_window()
    )

    state, result = state.evaluate("A")
    assert result.true_subset.size == 1


def test_in_latest_time_window_static_partitioned() -> None:
    state = AssetConditionScenarioState(
        one_asset, asset_condition=AssetCondition.in_latest_time_window()
    ).with_asset_properties(partitions_def=two_partitions_def)

    state, result = state.evaluate("A")
    assert result.true_subset.size == 2


def test_in_latest_time_window_time_partitioned() -> None:
    state = AssetConditionScenarioState(
        one_asset, asset_condition=AssetCondition.in_latest_time_window()
    ).with_asset_properties(partitions_def=daily_partitions_def)

    # no partitions exist yet
    state = state.with_current_time(time_partitions_start_datetime)
    state, result = state.evaluate("A")
    assert result.true_subset.size == 0

    state = state.with_current_time("2020-02-02T01:00:00")
    state, result = state.evaluate("A")
    assert result.true_subset.size == 1
    assert result.true_subset.asset_partitions == {
        AssetKeyPartitionKey(AssetKey("A"), "2020-02-01")
    }

    state = state.with_current_time_advanced(days=5)
    state, result = state.evaluate("A")
    assert result.true_subset.size == 1
    assert result.true_subset.asset_partitions == {
        AssetKeyPartitionKey(AssetKey("A"), "2020-02-06")
    }
