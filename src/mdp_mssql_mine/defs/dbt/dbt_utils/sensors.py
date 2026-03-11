from  mdp_mssql_mine.defs.dbt.dbt_utils.create_sensors import create_multi_asset_sensor
"""
DBT Asset Sensors.

Auto-trigger dbt assets when their dependencies are ready.
"""

# Bronze → Silver work order headers and lines
customer_type_performance_target_sensor = create_multi_asset_sensor(
    sensor_name="customer_type_performance_target_sensor",
    monitored_asset_names=["event","eventtype","business_unit", "business_unit_type"],
    job_name="refresh_customer_type_performance_target",
    minimum_interval_seconds=30,
)


######### GOLD ##########




