{{ 
    config(
        materialized= "view",
        tags= ["silver", "fact_machine"]
    )
}}


with mts_vw_last_dataset_and_smu as (
    select * from {{ source('silver', 'b_silver_mts_vw_last_dataset_and_smu') }}
),

ref_vw_nba_machines as (
    select * from {{ source('silver', 'a_bronze_ref_vw_nba_machines') }}
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
        ,'{{ var("gold_prefix") }}' as layer_prefix
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

