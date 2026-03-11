{{ 
    config(
        materialized= "view",
        tags= ["silver", "product_group"]
    )
}}


with v_metaform2406017 as (
    select * from {{ source('bronze', 'b_silver_v_metaform2406017') }}
),


silver_product_group as (
    select  
        c_2435788                     as Product_Group_Code
       ,c_2435789                     as Product_Group_Description
       ,c_2435809                     as Priority


        -- Audit and lineage fields
        ,current_timestamp()          as dbt_processed_at
        -- Metadata
        ,'silver_product_group'       as dbt_model_name
        ,'{{ var("silver_prefix") }}' as layer_prefix
        ,'product_group'              as business_domain
    from v_metaform2406017
)
select * from silver_product_group


