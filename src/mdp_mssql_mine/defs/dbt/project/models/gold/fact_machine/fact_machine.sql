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
    from mine_sites.dbo.mts_vw_last_dataset_and_smu s
	left join nmbmts.dbo.ref_vw_nba_machines m
	on s.serialnumber = m.serialnumber
    where neembeequipmentid != -2
),

fact_tum_kpis as (
    select
        tum.siteid,
        tum.site,
        tum.neembaequipmentid,
        tum.month,
        tum.hassmuformonth,
        max(tum.daysinmonth)                                        as daysinmonth,
        max(tum.ct)                                                 as ct,
        max(tum.ot)                                                 as ot,
        max(tum.updt)                                               as updt,
        max(tum.pdt)                                                as pdt,
        max(tum.upc)                                                as upc,
        max(tum.pc)                                                 as pc,
        sum(mtbsafterpm)/ sum(case when mtbsafterpm is null then 0 
                                   else 1 
                              end)                                  as mtbsafterpm


        -- Audit and lineage fields
        ,current_timestamp()                                        as dbt_processed_at
        -- Metadata                             
        ,'gold_fact_machine'                                 as dbt_model_name
        ,'{{ var("gold_prefix") }}'                                 as layer_prefix
        ,'fact_machine'                                      as business_domain
    from cte
        left join fact_machine tum
               on cte.siteid = tum.siteid
              and cte.yearmonth = tum.month
              and cte.neembaequipmentid = tum.neembaequipmentid
        where cte.neembaequipmentid <> -2
          and tum.month >= to_varchar(dateadd('month', -12, current_date()), 'yyyy-mm')
          and tum.site in (
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
        group by
            tum.siteid,
            tum.site,
            tum.neembaequipmentid,
            tum.month,
            tum.hassmuformonth

)


select * from fact_tum_kpis

