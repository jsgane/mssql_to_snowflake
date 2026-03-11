from dagster_dbt import dbt_assets, DbtCliResource
from pathlib import Path
import dagster as dg
from dagster import AssetExecutionContext, AssetKey
from dagster_dbt import DbtCliResource

### dbt assets

dbt_project_dir = Path(__file__).parent / "project"

@dg.asset(
    name="customer_type_performance_target",
    group_name="data_for_mine_silver",
    kinds={"snowflake", "dbt", "silver"},
    deps=[
        AssetKey(["EVENT"]),
        AssetKey(["EVENTTYPE"]),
        AssetKey(["BUSINESS_UNIT"]),
        AssetKey(["BUSINESS_UNIT_TYPE"]),
    ],
    description="customer_type_performance_target — silver layer transformed via dbt",
)
def customer_type_performance_target_asset(context: AssetExecutionContext, dbt: DbtCliResource) -> dg.MaterializeResult:
    results = list(
        dbt.cli(["run", "--select", "silver_customer_type_performance_target"], context=context).stream()
    )
    return dg.MaterializeResult(
        metadata={"dbt_results": dg.MetadataValue.int(len(results))}
    )
##@dbt_assets(
##    manifest=dbt_project_dir / "target" / "manifest.json",
##)
##def mdp_mssql_mine_dbt_assets(context, dbt: DbtCliResource):
##    yield from dbt.cli(["build"], context=context).stream()