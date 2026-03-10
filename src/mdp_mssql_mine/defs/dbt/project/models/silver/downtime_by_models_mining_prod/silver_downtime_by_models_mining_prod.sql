{{ 
    config(
        materialized= "view",
        tags= ["silver", "downtime_by_models_mining_prod"]
    )
}}


with v_metaform84 as (
    select * from {{ source('bronze', 'b_silver_v_metaform84') }}
),


silver_downtime_by_models_mining_prod as (
    select  
        display_value2434438                                        as  site,
        c_2433284                                                   as  monthyear,
        c_2433285                                                   as  model,
        sum(cast(c_2433283 as float))                               as totalhours,
        count(distinct display_value1180)                           as numberofequip,
        count(display_value1180)                                    as totalstoppage,
        count(case when c_2433282 = 'Unplanned' then 1
              end)                                                  as totalstoppageunplanned,
        count(case when c_2433282 = 'Planned' then 1
              end)                                                  as totalstoppageplanned,
        sum(case when c_2433282 = 'Unplanned' then c_2433283
            end)                                                    as unplannedhours,
        sum(case when c_2433282 = 'Planned' then c_2433283
            end)                                                    as plannedhours,
        datediff(day, c_2433284, dateadd(month, 1, c_2433284)) * 24 as monthlycalendarhour,
        datediff(day, c_2433284, dateadd(month, 1, c_2433284)) * 24 
            * count(distinct display_value1180)                     as totalcalendarhours,
    
        ((datediff(day, c_2433284, dateadd(month, 1, c_2433284)) 
          * 24 * count(distinct display_value1180)
         ) - sum(c_2433283)
         )
        /
        (datediff(day, c_2433284, dateadd(month, 1, c_2433284)) 
         * 24 * count(distinct display_value1180)
        )                                                           as availability,
        sum(c_2433283) / count(display_value1180)                   as mttr

        -- Audit and lineage fields
        ,current_timestamp()                                        as dbt_processed_at
        -- Metadata
        ,'silver_downtime_by_models_mining_prod'                    as dbt_model_name
        ,'{{ var("silver_prefix") }}'                               as layer_prefix
        ,'downtime_by_models_mining_prod'                           as business_domain
    from v_metaform84
    group by display_value2434438,
            c_2433284,
            c_2433285
    order by c_2433284 desc
)
select * from silver_downtime_by_models_mining_prod


