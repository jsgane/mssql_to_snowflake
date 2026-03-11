{{ 
    config(
        materialized= "view",
        tags= ["silver", "customer_type_performance_target"]
    )
}}


with eventchain as (
    select * from {{ source('bronze', 'a_bronze_eventchain') }}
),

eventchaincmtval as (
    select * from {{ source('bronze', 'a_bronze_eventchaincmtval') }}
),

eventchaintype as (
    select * from {{ source('bronze', 'a_bronze_eventchaintype') }}
),

event as (
    select * from {{ source('bronze', 'a_bronze_event') }}
),

eventtype as (
    select * from {{ source('bronze', 'a_bronze_eventtype') }}
),

business_unit as (
    select * from {{ source('bronze', 'a_bronze_business_unit') }}
),

business_unit_type as (
    select * from {{ source('bronze', 'a_bronze_business_unit_type') }}
),

traversal2418695 as (
        select 
            aggtable.eventchainid as eventchainid,
            max(case when (aggtable.eventtypeid = 2 and aggtable.enabled <> 0) 
                     then aggtable.business_unit_id end
            )                                                                as col2436213
        from event aggtable
        where aggtable.eventtypeid = 2 and aggtable.enabled <> 0
        group by aggtable.eventchainid
),

traversal2418701 as (
        select 
            aggtable.eventchainid                                                             as eventchainid,
            max(case when aggtable.eventchaincmtid = 6719 then aggtable.eventchaincmtval end) as col2436224,
            max(case when aggtable.eventchaincmtid = 6720 then aggtable.eventchaincmtval end) as col2436225,
            max(case when aggtable.eventchaincmtid = 6718 then aggtable.eventchaincmtval end) as col2436226
        from eventchaincmtval aggtable
        where aggtable.eventchaincmtid in (6719, 6720, 6718)
        group by aggtable.eventchainid
),

get_eventchainid as (
        select ec.eventchainid
        from eventchaintype ect
        join eventchain ec on ec.eventchaintypeid = ect.eventchaintypeid
        join event e on e.eventchainid = ec.eventchainid 
                       and e.eventtypeid = (select eventtypeid 
                                            from eventtype 
                                            where eventtype = 'Start Load')
        join business_unit bu on bu.business_unit_id = e.business_unit_id 
                              and business_unit_type_id = (select business_unit_type_id 
                                                           from business_unit_type 
                                                           where business_unit_type = 'Performance_Target')
        where ect.enabled <> 0 
          and ect.eventchaintype = 'TRUCKING'
),
details as (
    select 
        traversal2418694.eventchainid                      as eventchainid,
        traversal2418694.created_by                        as created_by,
        traversal2418694.created_date                      as created_date,
        traversal2418694.eventchaintypeid                  as eventchaintypeid,
        traversal2418695.col2436213                        as type,
        cast(traversal2418701.col2436224 as decimal(24,6)) as availability,
        cast(traversal2418701.col2436225 as decimal(24,6)) as mtbf,
        traversal2418701.col2436226                        as type_customer
    from eventchain traversal2418694
    left join traversal2418695 on traversal2418694.eventchainid = traversal2418695.eventchainid
    left join traversal2418701 on traversal2418694.eventchainid = traversal2418701.eventchainid
    where traversal2418694.eventchainid in (
        select ec.eventchainid
        from get_eventchainid ec
    )
),

silver_customer_type_performance_target as (
    select  
        details.eventchainid                       as eventchainid,
        details.created_by                         as created_by,
        details.created_date                       as created_date,
        details.eventchaintypeid                   as eventchaintypeid,
        details.type                               as type,
        details.availability                       as availability,
        details.mtbf                               as mtbf,
        details.type_customer                      as type_customer

        -- Audit and lineage fields
        ,current_timestamp()                       as dbt_processed_at
        -- Metadata
        ,'silver_customer_type_performance_target' as dbt_model_name
        ,'{{ var("silver_prefix") }}'              as layer_prefix
        ,'customer_type_performance_target'        as business_domain
    from details
    order by created_date desc
)
select * from silver_customer_type_performance_target


