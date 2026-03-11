{{ 
    config(
        materialized= "view",
        tags= ["silver", "fact_smu"]
    )
}}


with mts_vw_last_dataset_and_smu as (
    select * from {{ source('silver', 'mts_vw_last_dataset_and_smu') }}
),


rankeddata as (
    select
	    neembeequipmentid,
        serialnumber,
        lastdatasettime,
        smu,
        source,
        row_number() over (partition by serialnumber
                           order by lastdatasettime desc
                        ) as rn
    from mts_vw_last_dataset_and_smu smu
),

fact_smu as (
    select
        NeembeEquipmentID,
        SerialNumber,
        LastDatasettime,
        SMU,
        Source

        -- Audit and lineage fields
        ,current_timestamp()        as dbt_processed_at
        -- Metadata                             
        ,'gold_fact_smu'            as dbt_model_name
        ,'{{ var("gold_prefix") }}' as layer_prefix
        ,'fact_smu'                 as business_domain
    from rankeddata
    where rn = 1
)


select * from fact_smu

