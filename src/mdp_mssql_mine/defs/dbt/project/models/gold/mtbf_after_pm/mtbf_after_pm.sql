{{ 
    config(
        materialized= "view",
        tags= ["silver", "mtbf_after_pm"]
    )
}}


with vw_mtbf_after_pm as (
    select * from {{ source('silver', 'vw_mtbf_after_pm') }}
),

mtbf_after_pm as (
    select
        mapm."equip no."                    as equip_no,
        mapm.minesite,
        mapm.pm                             as description_cat,
        mapm.pm_endtime,
        mapm.first_unplanned_failure,
        mapm."labour type"                  as labour_type,
        mapm.hours_after_pm,
        to_char(mapm.pm_endtime, 'yyyy-mm') as yearmonth

        -- Audit and lineage fields
        ,current_timestamp()                as dbt_processed_at
        -- Metadata                                
        ,'gold_mtbf_after_pm'               as dbt_model_name
        ,'{{ var("gold_prefix") }}'         as layer_prefix
        ,'mtbf_after_pm'                    as business_domain
    from vw_mtbf_after_pm mapm
)


select * from mtbf_after_pm

