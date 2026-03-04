-- created_at: 2026-03-04T14:03:25.948151958+00:00
-- finished_at: 2026-03-04T14:03:26.125851604+00:00
-- elapsed: 177ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2cdcb-0209-5c73-0002-f3220094894e
-- desc: Get table schema
describe table "NEEMBA"."MINES"."B_SILVER_DIAG_D_DIAGNOSTIQUE_CID";
-- created_at: 2026-03-04T14:03:26.189236098+00:00
-- finished_at: 2026-03-04T14:03:26.445423870+00:00
-- elapsed: 256ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2cdcb-0209-62ec-0002-f32200945afa
-- desc: Get table schema
describe table "NEEMBA"."MINES"."B_SILVER_DIAG_D_DIAGNOSTIQUE_EID";
-- created_at: 2026-03-04T14:03:26.446596427+00:00
-- finished_at: 2026-03-04T14:03:26.574574957+00:00
-- elapsed: 127ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2cdcb-0209-5c73-0002-f32200948952
-- desc: Get table schema
describe table "NEEMBA"."MINES"."B_SILVER_DIAG_D_DIAGNOSTIQUE_FMI";
-- created_at: 2026-03-04T14:03:26.575392873+00:00
-- finished_at: 2026-03-04T14:03:26.720328135+00:00
-- elapsed: 144ms
-- outcome: success
-- dialect: snowflake
-- node_id: not available
-- query_id: 01c2cdcb-0209-62ec-0002-f32200945afe
-- desc: Get table schema
describe table "NEEMBA"."MINES"."B_SILVER_DIAG_D_DIAGNOSTIQUE_MID";
-- created_at: 2026-03-04T14:03:26.674865598+00:00
-- finished_at: 2026-03-04T14:03:26.893147321+00:00
-- elapsed: 218ms
-- outcome: error
-- error vendor code: 2003
-- error message: NotFound: [Snowflake] 002003 (42S02): SQL compilation error:
Table 'NEEMBA.MINES.B_SILVER_DIAG_F_ALERTE' does not exist or not authorized.
-- dialect: snowflake
-- node_id: not available
-- query_id: not available
-- desc: Get table schema
describe table "NEEMBA"."MINES"."B_SILVER_DIAG_F_ALERTE";
-- created_at: 2026-03-04T14:03:30.129338663+00:00
-- finished_at: 2026-03-04T14:03:30.288150940+00:00
-- elapsed: 158ms
-- outcome: error
-- error message: Error { message: "[Snowflake] 002003 (42S02): SQL compilation error:\nObject 'NEEMBA.MINES.B_SILVER_DIAG_F_ALERTE' does not exist or not authorized.", status: NotFound, vendor_code: 2003, sqlstate: [52, 50, 83, 48, 50], details: None }
-- dialect: snowflake
-- node_id: not available
-- query_id: not available
-- desc: dbt run query
select * from (


with diag_f_alerte as (
    select * from NEEMBA.mines.b_silver_diag_f_alerte
),

diag_f_alerte as (
    select * from NEEMBA.mines.b_silver_diag_d_diagnostique_cid
),

diag_f_alerte as (
    select * from NEEMBA.mines.b_silver_diag_d_diagnostique_eid
),

diag_f_alerte as (
    select * from NEEMBA.mines.b_silver_diag_d_diagnostique_fmi
),

diag_f_alerte as (
    select * from NEEMBA.mines.b_silver_diag_d_diagnostique_mid
),

silver_mts_vw_visionlink_alerts as (
    select distinct
         vale_code
        ,vale_cid_sk
	    ,e.vale_numero_serie               as serialnumber
        ,vale_date_creation_heure          as eventtimestamp
        ,vale_date_integration_heure     
        ,vale_equ_sk                       as neembaequipmentid
        ,vale_niveau                       as eventlevel
        ,vale_occurences                   as eventcount
        ,vale_origine_donnees              as sourcetype
	    ,eid.eid_code     
	    ,eid_lib_diagnostique_eid          as eid_description
	    ,cid.cid_code     
	    ,cid_lib_diagnostique_cid          as cid_description
	    ,fmi.fmi_code     
	    ,fmi.fmi_lib_diagnostique_fmi      as fmi_description
	    ,mid.mid_code                      as mid_code
	    ,mid.mid_lib_diagnostique_mid      as mid_description

        -- Audit and lineage fields
        ,current_timestamp()               as dbt_processed_at
        -- Metadata
        ,'silver_mts_vw_visionlink_alerts' as dbt_model_name
        ,'b_silver_'      as layer_prefix
        ,'mts_vw_visionlink_alerts'        as business_domain
    from diag_f_alerte e
         left join diag_d_diagnostique_cid cid on cid.cid_sk=e.vale_cid_sk
         left join diag_d_diagnostique_eid eid on eid.eid_sk=e.vale_eid_sk
         left join diag_d_diagnostique_fmi fmi on fmi.fmi_sk=e.vale_fmi_sk
         left join diag_d_diagnostique_mid mid on mid.mid_sk=e.vale_mid_sk
)
select * from silver_mts_vw_visionlink_alerts) limit 1000;
