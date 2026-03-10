{{ 
    config(
        materialized= "view",
        tags= ["silver", "description_cat_list"]
    )
}}


with v_metaform2404989 as (
    select * from {{ source('bronze', 'b_silver_v_metaform2404989') }}
),



silver_description_cat_list as (
    select  
        c_2433461                      as DescriptionCat

        -- Audit and lineage fields
        ,current_timestamp()            as dbt_processed_at
        -- Metadata
        ,'silver_description_cat_list'  as dbt_model_name
        ,'{{ var("silver_prefix") }}'   as layer_prefix
        ,'description_cat_list'         as business_domain
    from v_metaform2404989
)
select * from silver_description_cat_list


