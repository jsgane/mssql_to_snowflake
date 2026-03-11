from  mdp_mssql_mine.defs.dbt.dbt_utils.create_dbt_asset import create_dbt_job


refresh_customer_type_performance_target_job = create_dbt_job(
    asset_name="customer_type_performance_target",
    job_name="refresh_customer_type_performance_target",
    description="Refresh silver customer_type_performance_target model",
)


########## GOLD ############





