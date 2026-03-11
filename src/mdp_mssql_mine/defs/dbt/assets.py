from dagster import AssetExecutionContext, Output, MetadataValue
from typing import Dict, Any
from pathlib import Path

from mdp_mssql_mine.defs.dbt_utils.create_dbt_asset import execute_dbt, create_dbt_asset


GROUP_SILVER = "data_for_mine_silver"
GROUP_GOLD = "data_for_mine_gold"

#### Silver

dbt_silver_quote_work_order= create_dbt_asset(
    asset_name="silver_quote_work_order",
    dbt_model_name="silver_quote_work_order",
    dbt_project_name="mdp_service",
    schema_name="services",
    group_name=GROUP_SILVER,
    layer="silver",
    dependencies=["service_repair_order_header","service_repair_order_line", "service_intervention", "customized_field"],
    metadata_queries={
        "total_entetes":"SELECT count(*) FROM services.B_SILVER_quote_work_order",

    },
    description="",
    dbt_project_dir=Path(__file__).parent /"project",
)



