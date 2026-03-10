{{ 
    config(
        materialized= "view",
        tags= ["silver", "operating_time_mining_prod"]
    )
}}


with v_metaform2405991 as (
    select * from {{ source('bronze', 'b_silver_v_metaform2405991') }}
),


silver_operating_time_mining_prod as (
    select  
        c_2434477                            as monthyear,
        display_value2434475                 as site,
        display_value2434473                 as equipment,
        c_2434474                            as model,
        c_2434476                            as hours,
        c_2436246                            as sn

        -- Audit and lineage fields
        ,current_timestamp()                 as dbt_processed_at
        -- Metadata
        ,'silver_operating_time_mining_prod' as dbt_model_name
        ,'{{ var("silver_prefix") }}'        as layer_prefix
        ,'operating_time_mining_prod'        as business_domain
    from v_metaform2405991
)
select * from silver_operating_time_mining_prod


