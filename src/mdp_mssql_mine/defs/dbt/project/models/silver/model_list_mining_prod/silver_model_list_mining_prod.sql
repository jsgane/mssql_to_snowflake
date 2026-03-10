{{ 
    config(
        materialized= "view",
        tags= ["silver", "model_list_mining_prod"]
    )
}}


with v_metaform36 as (
    select * from {{ source('bronze', 'b_silver_v_metaform36') }}
),

silver_model_list_mining_prod as (
    select  
        c_356                            as model,
        c_2433296                        as type,
        c_2433294                        as family,
        c_2435811                        as primemovers

        -- Audit and lineage fields
        ,current_timestamp()             as dbt_processed_at
        -- Metadata
        ,'silver_model_list_mining_prod' as dbt_model_name
        ,'{{ var("silver_prefix") }}'    as layer_prefix
        ,'model_list_mining_prod'        as business_domain
    from v_metaform36 
)
select * from silver_model_list_mining_prod
