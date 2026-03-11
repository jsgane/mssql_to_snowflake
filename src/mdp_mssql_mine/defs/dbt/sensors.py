"""
DBT Asset Sensors.

Auto-trigger dbt assets when their dependencies are ready.
"""

from  mdp_mssql_mine.defs.dbt_utils.create_sensor import create_multi_asset_sensor


# Bronze → Silver work order headers and lines
silver_quote_work_order_sensor = create_multi_asset_sensor(
    sensor_name="silver_quote_work_order_sensor",
    monitored_asset_names=["silver_quote_work_order"],
    job_name="refresh_silver_quote_work_order",
    minimum_interval_seconds=30,
)


######### GOLD ##########
gold_quote_work_order_sensor = create_multi_asset_sensor(
    sensor_name="gold_quote_work_order_sensor",
    monitored_asset_names=["silver_quote_work_order"],
    job_name="refresh_gold_quote_work_order",
    minimum_interval_seconds=30,
)



