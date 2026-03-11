-- created_at: 2026-03-11T14:43:23.288231151+00:00
-- finished_at: 2026-03-11T14:43:23.807186604+00:00
-- elapsed: 518ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2f553-0209-6de9-0002-f322009c7436
-- desc: Get table schema
describe table "NEEMBA"."MINES"."VW_NOMBRE_ARRETS_REELS";
-- created_at: 2026-03-11T14:43:26.673203579+00:00
-- finished_at: 2026-03-11T14:43:28.627385986+00:00
-- elapsed: 2.0s
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: not available
-- desc: dbt run query
select * from (select * from (
with vw_nombre_arrets_reels as (
    select * from NEEMBA.mines.vw_nombre_arrets_reels
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
        ,'c_gold_'         as layer_prefix
        ,'nb_arrets_reels'                  as business_domain
    from vw_nombre_arrets_reels
)
SELECT * FROM NB_ARRETS_REELS
) as __preview_sbq__ limit 1000) limit 10;
