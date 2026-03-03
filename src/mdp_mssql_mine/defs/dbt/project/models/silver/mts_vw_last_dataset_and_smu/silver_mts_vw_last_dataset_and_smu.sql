{{ 
    config(
        materialized= "view",
        tags= ["silver", "mts_vw_down_event_history"]
    )
}}


with equ_f_smu_localisation as (
    select * from {{ source('bronze', 'b_silver_equ_f_smu_localisation') }}
),

equ_d_equipement as (
    select * from {{ source('bronze', 'b_silver_equ_d_equipement') }}
),

cte_smu_localisation as (
	select 
		 eq.equ_sk
		,loc.vsml_numero_serie
		,loc.vsml_moduletime
		,loc.vsml_compteur_valeur
		,row_number() over (
							partition by loc.vsml_equ_sk
									    ,loc.vsml_numero_serie
									    ,loc.vsml_moduletime
									    ,loc.vsml_compteur_valeur 
							order by vsml_sk desc
							) as rn
	from equ_f_smu_localisation as loc
		 inner join equ_d_equipement as eq 
	 	         on loc.vsml_numero_serie = eq.equ_numero_serie
),
equ_last_seen as (
	select 
		vsml_numero_serie, 
		max(vsml_moduletime) as maxutc
	from equ_f_smu_localisation
	group by vsml_numero_serie
),

silver_mts_vw_last_dataset_and_smu as (
    select
		smu_loc.equ_sk                as neembeequipmentid
	   ,smu_loc.vsml_numero_serie     as serialnumber
	   ,smu_loc.vsml_moduletime       as lastdatasettime
	   ,smu_loc.vsml_compteur_valeur  as smu
	   ,'VisionLink'                  as source
        -- Audit and lineage fields
        ,current_timestamp()          as dbt_processed_at
        -- Metadata                                  
        ,'silver_mts_vw_last_dataset_and_smu' as dbt_model_name
        ,'{{ var("silver_prefix") }}' as layer_prefix
        ,'mts_vw_last_dataset_and_smu'        as business_domain
    
	from cte_smu_localisation as smu_loc
		 inner join equ_last_seen equ_ls 
		on smu_loc.vsml_numero_serie = equ_ls.vsml_numero_serie
		  and smu_loc.vsml_moduletime = equ_ls.maxutc
	where smu_loc.rn = 1

)
select * from silver_mts_vw_last_dataset_and_smu
