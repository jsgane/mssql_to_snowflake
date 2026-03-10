-- created_at: 2026-03-10T16:49:01.982710624+00:00
-- finished_at: 2026-03-10T16:49:02.145917145+00:00
-- elapsed: 163ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f031-0209-6de9-0002-f322009ada96
-- desc: execute adapter call
show terse schemas in database NEEMBA
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "mdp_mssql_mine", "target_name": "dev"} */;
-- created_at: 2026-03-10T16:49:03.568096434+00:00
-- finished_at: 2026-03-10T16:49:03.766461856+00:00
-- elapsed: 198ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mdp_mssql_mine.silver_operating_time_mining_prod
-- query_id: 01c2f031-0209-6de9-0002-f322009ada9a
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "NEEMBA"."MINES" LIMIT 10000;
-- created_at: 2026-03-10T16:49:03.774829128+00:00
-- finished_at: 2026-03-10T16:49:04.434261590+00:00
-- elapsed: 659ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mdp_mssql_mine.silver_operating_time_mining_prod
-- query_id: 01c2f031-0209-71d5-0002-f322009b0602
-- desc: execute adapter call
create or replace   view NEEMBA.mines.silver_operating_time_mining_prod
  
   as (
    


with v_metaform2405991 as (
    select * from NEEMBA.mines.b_silver_v_metaform2405991
),


silver_operating_time_mining_prod as (
    select  
        c_2434477                            as monthyear,
        display_value2434475                 as site,
        display_value2434473                 as equipment,
        c_2434474                            as model,
        c_2434476                            as hours,
        c_2436246                            as sn

        -- Audit and lineage fields
        ,current_timestamp()                 as dbt_processed_at
        -- Metadata
        ,'silver_operating_time_mining_prod' as dbt_model_name
        ,'b_silver_'        as layer_prefix
        ,'operating_time_mining_prod'        as business_domain
    from v_metaform2405991
)
select * from silver_operating_time_mining_prod


  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.mdp_mssql_mine.silver_operating_time_mining_prod", "profile_name": "mdp_mssql_mine", "target_name": "dev"} */;
