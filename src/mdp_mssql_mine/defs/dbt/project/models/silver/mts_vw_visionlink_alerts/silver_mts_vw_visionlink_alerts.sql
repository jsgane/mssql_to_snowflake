{{ 
    config(
        materialized= "view",
        tags= ["silver", "mts_vw_visionlink_alerts"]
    )
}}


with diag_f_alerte as (
    select * from {{ source('bronze', 'b_silver_diag_f_alerte') }}
),

diag_f_alerte as (
    select * from {{ source('bronze', 'b_silver_diag_d_diagnostique_cid') }}
),

diag_f_alerte as (
    select * from {{ source('bronze', 'b_silver_diag_d_diagnostique_eid') }}
),

diag_f_alerte as (
    select * from {{ source('bronze', 'b_silver_diag_d_diagnostique_fmi') }}
),

diag_f_alerte as (
    select * from {{ source('bronze', 'b_silver_diag_d_diagnostique_mid') }}
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
        ,'{{ var("silver_prefix") }}'      as layer_prefix
        ,'mts_vw_visionlink_alerts'        as business_domain
    from diag_f_alerte e
         left join diag_d_diagnostique_cid cid on cid.cid_sk=e.vale_cid_sk
         left join diag_d_diagnostique_eid eid on eid.eid_sk=e.vale_eid_sk
         left join diag_d_diagnostique_fmi fmi on fmi.fmi_sk=e.vale_fmi_sk
         left join diag_d_diagnostique_mid mid on mid.mid_sk=e.vale_mid_sk
)
select * from silver_mts_vw_visionlink_alerts
