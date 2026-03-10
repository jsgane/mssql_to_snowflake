{{ 
    config(
        materialized= "view",
        tags= ["silver", "mine_site_list_mining_prod"]
    )
}}



with v_metaform2405988 as (
    select * from {{ source('bronze', 'b_silver_v_metaform2405988') }}
),


silver_mine_site_list_mining_prod as (
    select  
        c_2434436                             as country,
        c_2435807                             as underfpr,
        c_2434437                             as minesite,
        display_value2434455                  as miningcontractor,
        display_value2434456                  as projectowner,
        c_2435891                             as focus,
        cast(c_2436078 as integer)            as customercode,
        display_value2436227                  as customertype

        -- Audit and lineage fields
        ,current_timestamp()                  as dbt_processed_at
        -- Metadata
        ,'silver_mine_site_list_mining_prod'  as dbt_model_name
        ,'{{ var("silver_prefix") }}'         as layer_prefix
        ,'mine_site_list_mining_prod'         as business_domain
    from v_metaform2405988
    where c_2435891 = 'Yes'
)
select * from silver_mine_site_list_mining_prod


