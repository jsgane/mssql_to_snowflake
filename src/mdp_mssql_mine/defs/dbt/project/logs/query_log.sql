-- created_at: 2026-03-11T15:41:55.116041493+00:00
-- finished_at: 2026-03-11T15:41:55.241300297+00:00
-- elapsed: 125ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f58d-0209-71d5-0002-f322009caa02
-- desc: Get table schema
describe table "NEEMBA"."MINES"."MTS_VW_LAST_DATASET_AND_SMU";
-- created_at: 2026-03-11T15:41:57.047458596+00:00
-- finished_at: 2026-03-11T15:41:57.201514457+00:00
-- elapsed: 154ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f58d-0209-71d5-0002-f322009caa06
-- desc: execute adapter call
show terse schemas in database NEEMBA
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "mdp_mssql_mine", "target_name": "dev"} */;
-- created_at: 2026-03-11T15:41:58.541119188+00:00
-- finished_at: 2026-03-11T15:41:58.787045319+00:00
-- elapsed: 245ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mdp_mssql_mine.fact_smu
-- query_id: 01c2f58d-0209-71d5-0002-f322009caa0a
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "NEEMBA"."MINES" LIMIT 10000;
-- created_at: 2026-03-11T15:41:58.797166660+00:00
-- finished_at: 2026-03-11T15:41:59.288312544+00:00
-- elapsed: 491ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mdp_mssql_mine.fact_smu
-- query_id: 01c2f58d-0209-6de9-0002-f322009cb222
-- desc: execute adapter call
create or replace   view NEEMBA.mines.fact_smu
  
   as (
    


with mts_vw_last_dataset_and_smu as (
    select * from NEEMBA.mines.mts_vw_last_dataset_and_smu
),


rankeddata as (
    select
	    neembeequipmentid,
        serialnumber,
        lastdatasettime,
        smu,
        source,
        row_number() over (partition by serialnumber
                           order by lastdatasettime desc
                        ) as rn
    from mts_vw_last_dataset_and_smu smu
),

fact_smu as (
    select
        NeembeEquipmentID,
        SerialNumber,
        LastDatasettime,
        SMU,
        Source

        -- Audit and lineage fields
        ,current_timestamp()        as dbt_processed_at
        -- Metadata                             
        ,'gold_fact_smu'            as dbt_model_name
        ,'c_gold_' as layer_prefix
        ,'fact_smu'                 as business_domain
    from rankeddata
    where rn = 1
)


select * from fact_smu

  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.mdp_mssql_mine.fact_smu", "profile_name": "mdp_mssql_mine", "target_name": "dev"} */;
