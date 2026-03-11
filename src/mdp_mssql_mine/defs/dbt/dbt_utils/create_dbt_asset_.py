from typing import Dict, Any, List, Optional
from pathlib import Path
from dagster import (
    asset,
    AssetExecutionContext,
    Output,
    MetadataValue,
    define_asset_job,
    AssetSelection,
)

from .execute_dbt_job import execute_dbt


def create_dbt_asset(
    asset_name: str,
    dbt_model_name: str,
    dbt_project_name: str,
    schema_name: str,
    group_name: str,
    layer: str,  # "silver" or "gold"
    dependencies: Optional[List[str]] = None,
    metadata_queries: Optional[Dict[str, str]] = None,
    description: Optional[str] = None,
    dbt_project_dir: Optional[Path] = None,
):
    """
    Factory for creating dbt assets with automatic execution and metadata collection.

    Args:
        asset_name: Dagster asset name
        dbt_model_name: Name of the dbt model to run (e.g., "silver_company_active_currencies")
        dbt_project_name: Name of dbt project in Snowflake
        schema_name: Snowflake schema (e.g., "params")
        group_name: Dagster asset group
        layer: Layer designation ("silver" or "gold")
        dependencies: List of upstream asset names
        metadata_queries: Dict of {metric_name: SQL_query} for metadata collection
        description: Asset description
        dbt_project_dir: Optional path to dbt project directory (auto-detected if not provided)

    Returns:
        Dagster asset function

    Example:
        >>> silver_asset = create_dbt_asset(
        ...     asset_name="silver_company_active_currencies",
        ...     dbt_model_name="silver_company_active_currencies",
        ...     dbt_project_name="mdp_params",
        ...     schema_name="params",
        ...     group_name="params_silver",
        ...     layer="silver",
        ...     dependencies=["reference_codes", "currencies"],
        ...     metadata_queries={
        ...         "row_count": "SELECT COUNT(*) FROM params.silver_company_active_currencies",
        ...         "active_count": "SELECT COUNT(*) FROM params.silver_company_active_currencies WHERE is_active = 1",
        ...     },
        ... )
    """
    deps = dependencies or []
    metadata_queries = metadata_queries or {}

    @asset(
        name=asset_name,
        description=description
        or f"{layer.capitalize()} layer dbt model: {dbt_model_name}",
        group_name=group_name,
        kinds={"snowflake", "dbt"},
        deps=deps,
        required_resource_keys={"snowflake"},
    )
    def dbt_asset(context: AssetExecutionContext) -> Output[Dict[str, Any]]:
        """Execute dbt model and collect metadata."""

        result = execute_dbt(
            context=context,
            dbt_project_name=dbt_project_name,
            dbt_args=f"run --select {dbt_model_name}",
            dbt_project_dir=dbt_project_dir,
        )

        # Collect metadata if queries provided
        metadata = {
            "dbt_project": dbt_project_name,
            "dbt_model": dbt_model_name,
            "schema": schema_name,
            "layer": layer,
            "execution_mode": result.get("execution_mode", "unknown"),
        }

        if metadata_queries:
            context.log.info("📊 Fetching metadata...")
            with context.resources.snowflake.get_connection() as conn:
                cursor = conn.cursor()
                try:
                    for metric_name, query in metadata_queries.items():
                        try:
                            cursor.execute(query)
                            value = cursor.fetchone()[0]
                            metadata[metric_name] = value
                            context.log.info(f"  {metric_name}: {value}")
                        except Exception as e:
                            context.log.warning(f"  Failed to fetch {metric_name}: {e}")
                finally:
                    cursor.close()

        return Output(
            value=result,
            metadata={
                k: (
                    MetadataValue.int(v)
                    if isinstance(v, int)
                    else MetadataValue.text(str(v))
                )
                for k, v in metadata.items()
            },
        )

    return dbt_asset


def create_dbt_job(
    asset_name: str,
    job_name: Optional[str] = None,
    description: Optional[str] = None,
):
    """
    Create a simple dbt asset refresh job.

    Args:
        asset_name: Name of the dbt asset
        job_name: Optional custom job name (defaults to f"refresh_{asset_name}")
        description: Optional job description

    Returns:
        Dagster asset job

    Example:
        >>> refresh_silver = create_dbt_job(
        ...     asset_name="silver_company_active_currencies",
        ...     job_name="refresh_silver_currencies",  # Custom name
        ...     description="Refresh silver currency asset",
        ... )
    """
    final_job_name = job_name or f"refresh_{asset_name}"

    return define_asset_job(
        name=final_job_name,
        description=description or f"Refresh {asset_name} asset",
        selection=AssetSelection.assets(asset_name),
    )