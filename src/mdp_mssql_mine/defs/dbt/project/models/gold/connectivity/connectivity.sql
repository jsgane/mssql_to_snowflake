{{ 
    config(
        materialized= "view",
        tags= ["silver", "connectivity"]
    )
}}


with vlinkdevice as (
    select * from {{ source('bronze', 'a_bronze_vlinkdevice') }}
),


vlinkentete as (
    select * from {{ source('bronze', 'a_bronze_vlinkentete') }}
),


device_status as (
        select
            ve.numeroseriesource as serialnumber,
            left(ve.numeroseriesource, 3) as prefix,
            vd.devicestatus as devicestatus,
            ve.dateheurecreation as eventtime
        from vlinkdevice vd
        join vlinkentete ve
          on vd.idvlinkentete = ve.idvlinkentete
        where ve.dateheurecreation >= dateadd(day, -30, current_timestamp())
),

conn_kpi_agg as (
    select
        h.serialnumber,
        h.prefix,
        max(h.eventtime)                                                                         as lasteventtime30d,
        max(case when h.devicestatus in ('Reporting','Not Expected to Report') 
                 then 1 else 0 end)                                                              as hasreporting,
        max(case when h.devicestatus in ('Not Reporting','Alternate Power','Awaiting to Report') 
                 then 1 else 0 end)                                                              as hasconnected,
        max(case when h.devicestatus in ('Reporting','Not Expected to Report') 
                 then h.eventtime end)                                                           as lastreportingfamilytime,
        max(case when h.devicestatus in ('Not Reporting','Alternate Power','Awaiting to Report') 
                 then h.eventtime end)                                                           as lastconnectedfamilytime,
        max(case when h.devicestatus not in (
                    'Reporting','Not Expected to Report',
                    'Not Reporting','Alternate Power','Awaiting to Report'
                 ) then h.eventtime end)                                                         as lastnotconnectedfamilytime
    from device_status h
    group by h.serialnumber, h.prefix
),


connectivity as (
    select
        agg.serialnumber,
        agg.prefix,
        agg.lasteventtime30d,
        case
            when agg.hasreporting = 1 then 'Reporting'
            when agg.hasconnected = 1 then 'Connected'
            else 'Not Connected'
        end                                                                as statusmachine,
        case
            when agg.hasreporting = 1 then agg.lastreportingfamilytime
            when agg.hasconnected = 1 then agg.lastconnectedfamilytime
            else agg.lastnotconnectedfamilytime
        end                                                                as lastreportingtime,
        datediff(
            hour,
            case
                when agg.hasreporting = 1 then agg.lastreportingfamilytime
                when agg.hasconnected = 1 then agg.lastconnectedfamilytime
                else agg.lastnotconnectedfamilytime
            end,
            current_timestamp()
        )                                                                  as hourssincelastreport

        -- Audit and lineage fields
        ,current_timestamp()                                               as dbt_processed_at
        -- Metadata                                                               
        ,'gold_connectivity'                                               as dbt_model_name
        ,'{{ var("gold_prefix") }}'                                        as layer_prefix
        ,'connectivity'                                                    as business_domain
    from conn_kpi_agg agg
)


select * from connectivity

