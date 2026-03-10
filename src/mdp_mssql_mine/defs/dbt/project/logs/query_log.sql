-- created_at: 2026-03-10T12:46:25.665882413+00:00
-- finished_at: 2026-03-10T12:46:25.850024543+00:00
-- elapsed: 184ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ef3e-0209-71d5-0002-f322009aab9e
-- desc: execute adapter call
show terse schemas in database NEEMBA
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "mssql_to_snowflake", "target_name": "dev"} */;
-- created_at: 2026-03-10T12:46:26.603376825+00:00
-- finished_at: 2026-03-10T12:46:26.820768409+00:00
-- elapsed: 217ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mssql_to_snowflake.silver_equipment_list_mining_prod
-- query_id: 01c2ef3e-0209-71d5-0002-f322009aaba2
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "NEEMBA"."MINES" LIMIT 10000;
-- created_at: 2026-03-10T12:46:26.823351223+00:00
-- finished_at: 2026-03-10T12:46:27.226767950+00:00
-- elapsed: 403ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mssql_to_snowflake.silver_equipment_list_mining_prod
-- query_id: 01c2ef3e-0209-729d-0002-f322009a9bc2
-- desc: execute adapter call
create or replace   view NEEMBA.mines.equipment_list_mining_prod
  
   as (
    


with v_metaform45 as (
    select * from NEEMBA.mines.b_silver_v_metaform45
),

silver_equipment_list_mining_prod as (
    select  
        display_value2434480 as site,
        c_463                  as equipment,
        c_464                  as equipid,
        display_value465     as model,
        c_2435669              as parentproductgroup,
        c_2433297              as sn,
        c_2435808              as brand,
        c_467                  as status

        -- Audit and lineage fields
        ,current_timestamp()          as dbt_processed_at
        -- Metadata
        ,'silver_equipment_list_mining_prod'            as dbt_model_name
        ,'b_silver_' as layer_prefix
        ,'equipment_list_mining_prod'                   as business_domain
    from v_metaform45 
    where c_2433297 is not null
      and c_467 = -1
)
select * from silver_equipment_list_mining_prod
  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.mssql_to_snowflake.silver_equipment_list_mining_prod", "profile_name": "mssql_to_snowflake", "target_name": "dev"} */;
