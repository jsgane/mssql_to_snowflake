{{ 
    config(
        materialized= "view",
        tags= ["silver", "nb_arrets_reels"]
    )
}}


with vw_nombre_arrets_reels as (
    select * from {{ source('silver', 'vw_nombre_arrets_reels') }}
),

nb_arrets_reels as (
    select
        equipment_id,
        equip,
        site,
        monthyear,
        nombre_arrets_reels_total,
        nombre_arrets_unplanned

        -- Audit and lineage fields
        ,current_timestamp()                as dbt_processed_at
        -- Metadata                                
        ,'gold_nb_arrets_reels'             as dbt_model_name
        ,'{{ var("gold_prefix") }}'         as layer_prefix
        ,'nb_arrets_reels'                  as business_domain
    from vw_nombre_arrets_reels
)


select * from nb_arrets_reels


