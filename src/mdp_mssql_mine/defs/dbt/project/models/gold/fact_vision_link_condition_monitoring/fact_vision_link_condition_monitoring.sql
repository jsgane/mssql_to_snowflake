{{ 
    config(
        materialized= "view",
        tags= ["silver", "fact_vision_link_condition_monitoring"]
    )
}}


with mts_vw_visionlink_alerts as (
    select * from {{ source('silver', 'b_silver_mts_vw_visionlink_alerts') }}
),

fact_vision_link_condition_monitoring as (
   select
        vale_code
       ,vale_cid_sk
       ,serialnumber
       ,eventtimestamp
       ,vale_date_integration_heure
       ,neembaequipmentid
       ,eventlevel
       ,eventcount
       ,sourcetype
       ,eid_code
       ,eid_description
       ,cid_code
       ,cid_description
       ,fmi_code
       ,fmi_description
       ,mid_code
       ,mid_description
       ,to_varchar(eventtimestamp, 'yyyy-mm') as yearmonth

        -- Audit and lineage fields
        ,current_timestamp()        as dbt_processed_at
        -- Metadata                             
        ,'gold_fact_vision_link_condition_monitoring'        as dbt_model_name
        ,'{{ var("gold_prefix") }}' as layer_prefix
        ,'fact_vision_link_condition_monitoring'             as business_domain
    
    from mts_vw_visionlink_alerts mvslnk
    --where eventtimestamp >= dateadd('day', -4, current_date())
)


select * from fact_vision_link_condition_monitoring

