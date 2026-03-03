-- created_at: 2026-03-03T14:15:23.996395836+00:00
-- finished_at: 2026-03-03T14:15:24.165112969+00:00
-- elapsed: 168ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2c837-0209-5b7e-0002-f32200936f1a
-- desc: execute adapter call
show terse schemas in database NEEMBA
    limit 10000
/* {"app": "dbt", "connection_name": "", "dbt_version": "2.0.0", "profile_name": "mdp_mssql_mine", "target_name": "dev"} */;
-- created_at: 2026-03-03T14:15:25.686492829+00:00
-- finished_at: 2026-03-03T14:15:25.889558032+00:00
-- elapsed: 203ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mdp_mssql_mine.fact_machine
-- query_id: 01c2c837-0209-5b7e-0002-f32200936f1e
-- desc: get_relation > list_relations call
SHOW OBJECTS IN SCHEMA "NEEMBA"."MINES" LIMIT 10000;
-- created_at: 2026-03-03T14:15:25.892944373+00:00
-- finished_at: 2026-03-03T14:15:26.379124150+00:00
-- elapsed: 486ms
-- outcome: success
-- dialect: snowflake
-- node_id: model.mdp_mssql_mine.fact_machine
-- query_id: 01c2c837-0209-5dc8-0002-f3220093985a
-- desc: execute adapter call
create or replace   view NEEMBA.mines.fact_machine
  
   as (
    


with mts_vw_last_dataset_and_smu as (
    select * from NEEMBA.mines.b_silver_mts_vw_last_dataset_and_smu
),

ref_vw_nba_machines as (
    select * from NEEMBA.mines.a_bronze_ref_vw_nba_machines
),

rankeddata as (
    select
	    m.sitedesc,
		m.siteid,
		m.sitecode,
        s.neembeequipmentid,
        s.serialnumber,
        s.lastdatasettime,
        case
            when datediff('day', s.lastdatasettime, current_date()) < 5 then 'Reporting'
            else 'Not Reporting'
        end as reportingstatus, 
        s.smu,
        s.source,
        row_number() over (partition by s.neembeequipmentid order by s.serialnumber) as rownum
    from mts_vw_last_dataset_and_smu s
	     left join ref_vw_nba_machines m
	            on s.serialnumber = m.serialnumber
    where neembeequipmentid != -2
),

fact_machine as (
    select
        sitedesc,
	    siteid,
	    sitecode,
        neembeequipmentid,
        serialnumber,
        lastdatasettime,
        reportingstatus, 
        smu,
        source

        -- Audit and lineage fields
        ,current_timestamp()        as dbt_processed_at
        -- Metadata                             
        ,'gold_fact_machine'        as dbt_model_name
        ,'c_gold_' as layer_prefix
        ,'fact_machine'             as business_domain
    from rankeddata
    where rownum = 1
      and sitecode in (
                'SIG',
                'FEK',
                'KRSC',
                'SEG',
                'AGB',
                'SIMM',
                'FDE',
                'T014',
                'TIND',
                'BKR',
                'KIAK',
                'BOG',
                'RSSA',
                'MHDT',
                'PG11',
                'ESK',
                'CBG',
                'SAB',
                'TON',
                'SAD'
            )
)


select * from fact_machine

  )
/* {"app": "dbt", "dbt_version": "2.0.0", "node_id": "model.mdp_mssql_mine.fact_machine", "profile_name": "mdp_mssql_mine", "target_name": "dev"} */;
