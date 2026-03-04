-- created_at: 2026-03-04T15:55:15.185015214+00:00
-- finished_at: 2026-03-04T15:55:15.461246076+00:00
-- elapsed: 276ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2ce3b-0209-5c73-0002-f322009539ca
-- desc: execute adapter call
show terse schemas in database NEEMBA
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "mdp_mssql_mine", "target_name": "dev"} */;
-- created_at: 2026-03-04T15:55:16.841504172+00:00
-- finished_at: 2026-03-04T15:55:17.100435450+00:00
-- elapsed: 258ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mdp_mssql_mine.fact_smu
-- query_id: 01c2ce3b-0209-623e-0002-f32200954452
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "NEEMBA"."MINES" LIMIT 10000;
-- created_at: 2026-03-04T15:55:17.103319580+00:00
-- finished_at: 2026-03-04T15:55:17.515991520+00:00
-- elapsed: 412ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mdp_mssql_mine.fact_smu
-- query_id: 01c2ce3b-0209-5c73-0002-f322009539d2
-- desc: execute adapter call
create or replace   view NEEMBA.mines.fact_smu
  
   as (
    


with mts_vw_last_dataset_and_smu as (
    select * from NEEMBA.mines.b_silver_mts_vw_last_dataset_and_smu
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
