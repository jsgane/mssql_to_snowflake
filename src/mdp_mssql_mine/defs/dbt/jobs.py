from  mdp_mssql_mine.defs.dbt_utils import create_dbt_job


refresh_silver_quote_work_order = create_dbt_job(
    asset_name="silver_quote_work_order",
    job_name="refresh_silver_quote_work_order",
    description="Refresh silver quote work order headers and lines",
)


########## GOLD ############
refresh_gold_quote_work_order = create_dbt_job(
    asset_name="gold_quote_work_order",
    job_name="refresh_gold_quote_work_order",
    description="Refresh gold quote work order headers and lines",
)




