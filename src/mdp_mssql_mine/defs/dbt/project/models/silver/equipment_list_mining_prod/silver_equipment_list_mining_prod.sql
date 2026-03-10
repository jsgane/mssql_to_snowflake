{{ 
    config(
        materialized= "view",
        tags= ["silver", "equipment_list_mining_prod"]
    )
}}


with v_metaform45 as (
    select * from {{ source('bronze', 'b_silver_v_metaform45') }}
),

silver_equipment_list_mining_prod as (
    select  
        display_value2434480 as site,
        c_463                  as equipment,
        c_464                  as equipid,
        display_value465     as model,
        c_2435669              as parentproductgroup,
        c_2433297              as sn,
        c_2435808              as brand,
        c_467                  as status

        -- Audit and lineage fields
        ,current_timestamp()          as dbt_processed_at
        -- Metadata
        ,'silver_equipment_list_mining_prod'            as dbt_model_name
        ,'{{ var("silver_prefix") }}' as layer_prefix
        ,'equipment_list_mining_prod'                   as business_domain
    from v_metaform45 
    where c_2433297 is not null
      and c_467 = -1
)
select * from silver_equipment_list_mining_prod
