{{ 
    config(
        materialized= "view",
        tags= ["silver", "vw_nombre_arrets_reels"]
    )
}}


with v_metaform45 as (
    select * from {{ source('bronze', 'b_silver_v_metaform45') }}
),

v_metaform84 as (
    select * from {{ source('bronze', 'b_silver_v_metaform84') }}
),

ordered_data as (
    select
        dw.display_value2434438 as site,
        dw.display_value1180 as equip,
        dw.c_2433285 as model,
        dw.c_2433282 as worktype,
        dw.c_2433286 as labourtype,
        dw.display_value2433463 as descriptioncat,
        cast(dw.c_3547 as timestamp_ntz) as starthours,
        cast(dw.c_3548 as timestamp_ntz) as endhours,
        dw.c_2433283 as downtimehours,
        dw.c_1588 as comments,
        dw.c_2433284 as monthyear,
        dw.c_1180 as equipment_id,
        lag(cast(dw.c_3548 as timestamp_ntz)) over (partition by dw.c_1180 order by cast(dw.c_3547 as timestamp_ntz)) as prevendhours,
        lag(dw.c_2433286) over (partition by dw.c_1180 order by cast(dw.c_3547 as timestamp_ntz)) as prevlabourtype
    from v_metaform84 as dw
    join v_metaform45 as eq
        on dw.c_1180 = eq.c_464
),
flagged as (
    select *,
        case
            when labourtype = prevlabourtype
                 and datediff(minute, prevendhours, starthours) <= 5
            then 0 else 1
        end as isnewgroup
    from ordered_data
),

grouped as (
    select *,
        sum(isnewgroup) over (partition by equipment_id order by starthours rows unbounded preceding) as stop_id
    from flagged
),

group_labels as (
    select
        equipment_id,
        stop_id,
        min(starthours) as groupstart,
        max(worktype) as worktype
    from grouped
    group by equipment_id, stop_id
),

final as (
    select
        g.equipment_id,
        g.equip,
        g.site,
        g.monthyear,
        g.stop_id,
        gl.worktype
    from grouped g
    join group_labels gl
        on g.equipment_id = gl.equipment_id
       and g.stop_id = gl.stop_id
    group by g.equipment_id, g.equip, g.site, g.monthyear, g.stop_id, gl.worktype
),


silver_vw_nombre_arrets_reels as (
    select  
        equipment_id,
        equip,
        site,
        monthyear,
        count(*)                               as nombre_arrets_reels_total,
        count(case when worktype = 'Unplanned' 
                   then 1 
              end)                             as nombre_arrets_unplanned

        -- Audit and lineage fields
        ,current_timestamp()                   as dbt_processed_at
        -- Metadata
        ,'silver_vw_nombre_arrets_reels'       as dbt_model_name
        ,'{{ var("silver_prefix") }}'          as layer_prefix
        ,'vw_nombre_arrets_reels'              as business_domain
    from final
    group by equipment_id, equip, site, monthyear
)
select * from silver_vw_nombre_arrets_reels


