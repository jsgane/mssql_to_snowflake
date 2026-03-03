{{ 
    config(
        materialized= "view",
        tags= ["silver", "mts_vw_tum_kpis"]
    )
}}


with mts_vw_down_event_history as (
    select * from {{ source('silver', 'b_silver_mts_vw_down_event_history') }}
),

mts_vw_tum_kpis as (
    select * from {{ source('silver', 'c_gold_mts_vw_tum_kpis') }}
),

cte as (
    select distinct
        siteid,
        neembaequipmentid,
        starttime,
        endtime,
        downtype,
        to_varchar(endtime, 'yyyy-mm') as yearmonth,
        lead(downtype) over (
                            partition by siteid, neembaequipmentid 
                            order by endtime
                        ) as nextdowntype,
        lead(starttime) over (
                            partition by siteid, neembaequipmentid 
                            order by endtime
                        ) as nextdownstart,
        case 
            when downtype = 'PM' 
                 and lead(downtype) over (
                                    partition by siteid, neembaequipmentid 
                                    order by endtime
                                ) <> 'PM'
            then datediff('minute', endtime, 
                        lead(starttime) over (
                                        partition by siteid, neembaequipmentid 
                                        order by endtime
                                    )
                    ) / 60.0
        end as mtbsafterpm
    from mts_vw_down_event_history
    where endtime >= dateadd('month', -12, current_date())
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
        ,'gold_fact_tum_kpis'                                 as dbt_model_name
        ,'{{ var("gold_prefix") }}'                                 as layer_prefix
        ,'fact_tum_kpis'                                      as business_domain
    from cte
        left join mts_vw_tum_kpis tum
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

