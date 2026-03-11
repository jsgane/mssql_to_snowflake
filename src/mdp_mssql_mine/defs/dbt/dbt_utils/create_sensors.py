from typing import List
from dagster import (
    AssetKey,
    multi_asset_sensor,
    MultiAssetSensorEvaluationContext,
    RunRequest,
    SkipReason,
)


def create_multi_asset_sensor(
    sensor_name: str,
    monitored_asset_names: List[str],
    job_name: str,
    minimum_interval_seconds: int = 30,
):
    """
    Factory function to create a multi-asset sensor with proper cursor management.

    Creates a sensor that monitors multiple upstream assets and triggers
    a job when all dependencies have been materialized.

    Uses Dagster's native cursor management to avoid cursor corruption.

    Args:
        sensor_name: Name of the sensor
        monitored_asset_names: List of asset names to monitor
        job_name: Name of the job to trigger
        minimum_interval_seconds: Minimum interval between sensor evaluations

    Returns:
        A configured multi_asset_sensor function

    Example:
        >>> sensor = create_multi_asset_sensor(
        ...     sensor_name="silver_trigger",
        ...     monitored_asset_names=["bronze_table1", "bronze_table2"],
        ...     job_name="refresh_silver",
        ... )
    """
    monitored_keys = [AssetKey(name) for name in monitored_asset_names]

    @multi_asset_sensor(
        name=sensor_name,
        monitored_assets=monitored_keys,
        job_name=job_name,
        minimum_interval_seconds=minimum_interval_seconds,
    )
    def sensor(context: MultiAssetSensorEvaluationContext):
        """Multi-asset sensor with automatic cursor tracking."""
        asset_events = context.latest_materialization_records_by_key()

        if not asset_events:
            return SkipReason("No materializations yet")

        # Check if all monitored assets have been materialized
        missing_assets = []
        for asset_key in monitored_keys:
            if asset_key not in asset_events or not asset_events[asset_key]:
                missing_assets.append(asset_key.path[-1])

        if missing_assets:
            return SkipReason(f"Waiting for assets: {', '.join(missing_assets)}")

        # Use Dagster's built-in cursor tracking
        # Build unique run key from storage IDs
        storage_ids = [
            str(asset_events[asset_key].storage_id) for asset_key in monitored_keys
        ]
        run_key = f"{sensor_name}_{'_'.join(storage_ids)}"

        context.advance_all_cursors()

        return RunRequest(run_key=run_key)

    return sensor


def create_bronze_to_silver_sensor(
    silver_asset_name: str,
    bronze_dependencies: List[str],
    minimum_interval_seconds: int = 30,
):
    """
    Create a sensor to trigger silver layer asset when bronze dependencies are ready.

    Args:
        silver_asset_name: Name of the silver asset
        bronze_dependencies: List of bronze asset names to monitor
        minimum_interval_seconds: Minimum interval between evaluations

    Returns:
        Configured sensor function

    Example:
        >>> silver_sensor = create_bronze_to_silver_sensor(
        ...     silver_asset_name="silver_customers",
        ...     bronze_dependencies=["customers", "addresses"],
        ... )
    """
    sensor_name = f"{silver_asset_name}_sensor"
    job_name = f"refresh_{silver_asset_name}"

    return create_multi_asset_sensor(
        sensor_name=sensor_name,
        monitored_asset_names=bronze_dependencies,
        job_name=job_name,
        minimum_interval_seconds=minimum_interval_seconds,
    )


def create_silver_to_gold_sensor(
    gold_asset_name: str,
    silver_dependencies: List[str],
    minimum_interval_seconds: int = 30,
):
    """
    Create a sensor to trigger gold layer asset when silver dependencies are ready.

    Args:
        gold_asset_name: Name of the gold asset (data product)
        silver_dependencies: List of silver/bronze asset names to monitor
        minimum_interval_seconds: Minimum interval between evaluations

    Returns:
        Configured sensor function

    Example:
        >>> gold_sensor = create_silver_to_gold_sensor(
        ...     gold_asset_name="gold_customer_360",
        ...     silver_dependencies=["silver_customers", "silver_orders"],
        ... )
    """
    sensor_name = f"{gold_asset_name}_sensor"
    job_name = f"refresh_{gold_asset_name}"

    return create_multi_asset_sensor(
        sensor_name=sensor_name,
        monitored_asset_names=silver_dependencies,
        job_name=job_name,
        minimum_interval_seconds=minimum_interval_seconds,
    )