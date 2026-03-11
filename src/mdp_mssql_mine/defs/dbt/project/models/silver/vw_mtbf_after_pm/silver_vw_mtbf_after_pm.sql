{{ 
    config(
        materialized= "view",
        tags= ["silver", "vw_mtbf_after_pm"]
    )
}}


with eventchain as (
    select * from {{ source('bronze', 'a_bronze_eventchain') }}
),

eventchaincmtval as (
    select * from {{ source('bronze', 'a_bronze_eventchaincmtval') }}
),

eventchaintype as (
    select * from {{ source('bronze', 'a_bronze_eventchaintype') }}
),

get_eventchain_id as (
    select ec.eventchainid
    from eventchaintype ect
        join eventchain ec 
          on ec.eventchaintypeid = ect.eventchaintypeid
    where ect.enabled <> 0
      and ect.eventchaintype = 'EQUIP_NEEMBA'
      and ec.created_date >= '2025-01-01'
),

traversal518 as (
    select 
        aggtable.eventchainid,
        max(case when aggtable.eventchaincmtid = 6572 then aggtable.eventchaincmtval end) as col3547,
        max(case when aggtable.eventchaincmtid = 6573 then aggtable.eventchaincmtval end) as col3548,
        max(case when aggtable.eventchaincmtid = 3618 then aggtable.eventchaincmtval end) as col2433282,
        max(case when aggtable.eventchaincmtid = 3622 then aggtable.eventchaincmtval end) as col2433286,
        max(case when aggtable.eventchaincmtid = 3640 then aggtable.eventchaincmtval end) as col2433463,
        max(case when aggtable.eventchaincmtid = 4641 then aggtable.eventchaincmtval end) as col2434438
    from eventchaincmtval aggtable
    where aggtable.eventchaincmtid in (6572, 6573, 3618, 3622, 3640, 4641)
    group by aggtable.eventchainid
),

downtimedata as (
    select 
        traversal316.equipid as "equip no.",
        traversal518.col2433282 as worktype,
        TO_TIMESTAMP(
            REGEXP_REPLACE(traversal518.col3547, 
                          '(\\w+) (\\d) (\\d{4}) (\\d):', 
                          '\\1 0\\2 \\3 0\\4:'),
            'MON DD YYYY HH12:MIAM')                        as starttime,
        TO_TIMESTAMP(
            REGEXP_REPLACE(traversal518.col3548, 
                          '(\\w+) (\\d) (\\d{4}) (\\d):', 
                          '\\1 0\\2 \\3 0\\4:'),
            'MON DD YYYY HH12:MIAM') as endtime,
        traversal518.col2433286 as "labour type",
        traversal518.col2434438 as minesite,
        traversal518.col2433463 as "description cat",
        traversal316.record_date as "record date"
    from eventchain traversal316
         left join traversal518
                on traversal316.eventchainid = traversal518.eventchainid
    where traversal316.eventchainid in (select eventchainid from get_eventchain_id)
      and traversal518.col3547 is not null
      and traversal518.col3548 is not null
),

planned as (
    select 
        "equip no.",
        endtime as pm_endtime,
        minesite,
        "description cat"
    from downtimedata
    where worktype = 'Planned'
      and "description cat" = 'PM'
),


silver_vw_mtbf_after_pm as (
    select  
        pm."equip no.",
        pm.minesite,
        pm."description cat" as pm,
        pm.pm_endtime,
        up.starttime as first_unplanned_failure,
        up."labour type",
        up."description cat",
        datediff(minute, pm.pm_endtime, up.starttime) / 60.0 as hours_after_pm

        -- Audit and lineage fields
        ,current_timestamp()            as dbt_processed_at
        -- Metadata
        ,'silver_vw_mtbf_after_pm'  as dbt_model_name
        ,'{{ var("silver_prefix") }}'   as layer_prefix
        ,'vw_mtbf_after_pm'         as business_domain
    from planned as pm
    left join (
        select 
            f.*,
            row_number() over (
                partition by f."equip no." 
                order by f.starttime
            ) as rn
        from downtimedata f
        where f.worktype = 'Unplanned'
          and f."labour type" is not null
    ) up
      on up."equip no." = pm."equip no."
     and up.starttime > pm.pm_endtime
     and up.rn = 1
)
select * from silver_vw_mtbf_after_pm


