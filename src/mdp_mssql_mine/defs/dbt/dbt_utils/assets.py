from dagster import AssetExecutionContext, Output, MetadataValue
from typing import Dict, Any
from pathlib import Path

from mdp_mssql_mine.defs.dbt.dbt_utils.create_dbt_asset import execute_dbt, create_dbt_asset


GROUP_SILVER = "data_for_mine_silver"
GROUP_GOLD = "data_for_mine_gold"

#### Silver

dbt_customer_type_performance_target_asset = create_dbt_asset(
    asset_name="customer_type_performance_target",
    dbt_model_name="silver_customer_type_performance_target",
    dbt_project_name="mdp_mssql_mine",
    schema_name="mines",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["event","eventtype","business_unit", "business_unit_type"],
    metadata_queries={
        "total_entetes":"SELECT count(*) FROM mines.customer_type_performance_target",

    },
    description="",
    dbt_project_dir=Path(__file__).parent /"project",
)



