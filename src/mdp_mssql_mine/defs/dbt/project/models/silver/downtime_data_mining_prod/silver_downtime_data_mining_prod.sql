{{ 
    config(
        materialized= "view",
        tags= ["silver", "downtime_data_mining_prod"]
    )
}}


with v_metaform84 as (
    select * from {{ source('bronze', 'b_silver_v_metaform84') }}
),

v_metaform45 as (
    select * from {{ source('bronze', 'b_silver_v_metaform45') }}
),


silver_downtime_data_mining_prod as (
    select  
         dw.display_value2434438 as site,
         dw.display_value1180 as equip,
         dw.c_2433285 as model,
         dw.c_2433282 as worktype,
         dw.c_2433286 as labourtype,
         dw.display_value2433463 as descriptioncat,
         dw.c_3547 as starthours,
         dw.c_3548 as endhours,
         dw.c_2433283 as downtimehours,
         dw.c_1588 as comments,
         dw.c_2433284 as monthyear,
         dw.c_1180 as equipment_id,
         eq.c_2433297 as sn

        -- Audit and lineage fields
        ,current_timestamp()             as dbt_processed_at
        -- Metadata
        ,'silver_downtime_data_mining_prod' as dbt_model_name
        ,'{{ var("silver_prefix") }}'    as layer_prefix
        ,'downtime_data_mining_prod'        as business_domain
    from v_metaform84 as dw
        join v_metaform45 as eq
          on dw.c_1180 = eq.c_464
)
select * from silver_downtime_data_mining_prod


