-- created_at: 2026-03-04T14:30:55.301496790+00:00
-- finished_at: 2026-03-04T14:30:56.234527391+00:00
-- elapsed: 933ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: not available
-- desc: dbt run query
select * from (select * from (
with diag_f_alerte as (
    select * from NEEMBA.mines.b_silver_diag_f_alerte
),

diag_d_diagnostique_cid as (
    select * from NEEMBA.mines.b_silver_diag_d_diagnostique_cid
)
SELECT * FROM DIAG_D_DIAGNOSTIQUE_CID
) as __preview_sbq__ limit 1000) limit 10;
