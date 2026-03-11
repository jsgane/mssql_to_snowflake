import dagster as dg
from dagster import AssetExecutionContext, RetryPolicy
from dagster_dlt import DagsterDltResource, dlt_assets
from pathlib import Path
import dlt
import logging
from mdp_mssql_mine.defs.load.load_bcp_copy_into import Config, extract_mssql_data
# Pipeline DLT
pipeline = dlt.pipeline(
    pipeline_name="mssql_to_snowflake_pipeline",
    destination="snowflake",
    dataset_name="equipement",
    progress="log",
)

# Retry policy global
retry_policy = RetryPolicy(
    max_retries=3,
    delay=10,  # 10 secondes entre chaque retry
)


##### ASSETS USING BCP + COPY INTO

@dg.asset(
    name="mns_d_site",
    group_name="data_for_mine_silver",
    description="Mns_d_site from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def mns_d_site_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """Equipment from MSSQL"""
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "MINES", 
        mssql_table_name = "mns_d_site",
        snowflake_table_name = "a_bronze_mns_d_site",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )  

@dg.asset(
    name="mns_f_utilisation_equipement",
    group_name="data_for_mine_silver",
    description="Mns_f_utilisation_equipement from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def mns_f_utilisation_equipement_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """mns_f_utilisation_equipement from MSSQL"""
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "MINES", 
        mssql_table_name = "mns_f_utilisation_equipement",
        snowflake_table_name = "a_bronze_mns_f_utilisation_equipement",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )  

@dg.asset(
    name="ref_vw_nba_machines",
    group_name="data_for_mine_silver",
    description="Ref_vw_nba_machines from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def ref_vw_nba_machines_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """ref_vw_nba_machines from MSSQL"""
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "MINES", 
        mssql_table_name = "ref_vw_nba_machines",
        snowflake_table_name = "a_bronze_ref_vw_nba_machines",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 
 
@dg.asset(
    name="mns_f_equipement_arret",
    group_name="data_for_mine_silver",
    description="Mns_f_equipement_arret from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def mns_f_equipement_arret_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """mns_f_equipement_arret_v_0_1 from MSSQL"""
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "MINES", 
        mssql_table_name = "mns_f_equipement_arret_v_0_1",
        snowflake_table_name = "b_silver_mns_f_equipement_arret",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )  
 
@dg.asset(
    name="mns_d_type_evenement_arret",
    group_name="data_for_mine_silver",
    description="Mns_d_type_evenement_arret from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def mns_d_type_evenement_arret_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """mns_d_type_evenement_arret from MSSQL"""
    result = extract_mssql_data(
        snowflake_database = "NEEMBA",
        snowflake_schema = "MINES", 
        mssql_table_name = "mns_d_type_evenement_arret",
        snowflake_table_name = "b_silver_mns_d_type_evenement_arret",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    )  

@dg.asset(
    name="diag_f_alerte",
    group_name="data_for_mine_silver",
    description="Diag_f_alerte from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def diag_f_alerte_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """diag_f_alerte from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "diag_f_alerte",
        snowflake_table_name = "b_silver_diag_f_alerte",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 
@dg.asset(
    name="diag_d_diagnostique_cid",
    group_name="data_for_mine_silver",
    description="diag_d_diagnostique_cid from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def diag_d_diagnostique_cid_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """diag_d_diagnostique_cid from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "diag_d_diagnostique_cid",
        snowflake_table_name = "b_silver_diag_d_diagnostique_cid",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 
@dg.asset(
    name="diag_d_diagnostique_eid",
    group_name="data_for_mine_silver",
    description="diag_d_diagnostique_eid from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def diag_d_diagnostique_eid_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """diag_d_diagnostique_eid from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "diag_d_diagnostique_eid",
        snowflake_table_name = "b_silver_diag_d_diagnostique_eid",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 
@dg.asset(
    name="diag_d_diagnostique_fmi",
    group_name="data_for_mine_silver",
    description="diag_d_diagnostique_fmi from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def diag_d_diagnostique_fmi_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """diag_d_diagnostique_fmi from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "diag_d_diagnostique_fmi",
        snowflake_table_name = "b_silver_diag_d_diagnostique_fmi",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 


@dg.asset(
    name="diag_d_diagnostique_mid",
    group_name="data_for_mine_silver",
    description="diag_d_diagnostique_mid from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def diag_d_diagnostique_mid_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """diag_d_diagnostique_mid from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "diag_d_diagnostique_mid",
        snowflake_table_name = "b_silver_diag_d_diagnostique_mid",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 



############################################""""""""""""""""""   
@dg.asset(
    name="v_metaform45",
    group_name="data_for_mine_silver",
    description="v_metaform45 from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def v_metaform45_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """v_metaform45 from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "META_FORM_VIEW_SCHEMA.v_metaform45",
        snowflake_table_name = "b_silver_v_metaform45",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 

@dg.asset(
    name="v_metaform36",
    group_name="data_for_mine_silver",
    description="v_metaform36 from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def v_metaform36_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """v_metaform36 from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "META_FORM_VIEW_SCHEMA.v_metaform36",
        snowflake_table_name = "b_silver_v_metaform36",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 

@dg.asset(
    name="v_metaform84",
    group_name="data_for_mine_silver",
    description="v_metaform84 from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def v_metaform84_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """v_metaform84 from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "META_FORM_VIEW_SCHEMA.v_metaform84",
        snowflake_table_name = "b_silver_v_metaform84",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 

@dg.asset(
    name="v_metaform2405988",
    group_name="data_for_mine_silver",
    description="v_metaform2405988 from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def v_metaform2405988_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """v_metaform2405988 from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "META_FORM_VIEW_SCHEMA.v_metaform2405988",
        snowflake_table_name = "b_silver_v_metaform2405988",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 


@dg.asset(
    name="v_metaform2404989",
    group_name="data_for_mine_silver",
    description="v_metaform2404989 from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def v_metaform2404989_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """v_metaform2404989 from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "META_FORM_VIEW_SCHEMA.v_metaform2404989",
        snowflake_table_name = "b_silver_v_metaform2404989",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 


@dg.asset(
    name="v_metaform2405991",
    group_name="data_for_mine_silver",
    description="v_metaform2405991 from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def v_metaform2405991_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """v_metaform2405991 from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "META_FORM_VIEW_SCHEMA.v_metaform2405991",
        snowflake_table_name = "b_silver_v_metaform2405991",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 



@dg.asset(
    name="v_metaform2406017",
    group_name="data_for_mine_silver",
    description="v_metaform2406017 from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "silver"}
)
def v_metaform2406017_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """v_metaform2406017 from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "META_FORM_VIEW_SCHEMA.v_metaform2406017",
        snowflake_table_name = "b_silver_v_metaform2406017",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 


##### BRONZE ASSETS USING BCP + COPY INTO


@dg.asset(
    name="EVENTCHAIN",
    group_name="data_for_mine_bronze",
    description="EVENTCHAIN from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def eventchain_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """EVENTCHAIN from MSSQL"""
    result = extract_mssql_data(
        #snowflake_database = "NEEMBA",
        #snowflake_schema = "MINES", 
        mssql_table_name = "dbo.EVENTCHAIN",
        snowflake_table_name = "a_bronze_eventchain",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 


@dg.asset(
    name="EVENTCHAIN",
    group_name="data_for_mine_bronze",
    description="EVENTCHAIN from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def eventchain_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    """EVENTCHAIN from MSSQL"""
    result = extract_mssql_data(
        mssql_table_name = "dbo.EVENTCHAIN",
        snowflake_table_name = "a_bronze_eventchain",
        logger = context.log,
    )

    return dg.MaterializeResult(
        metadata={
            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
        }
    ) 


@dg.asset(
    name="EVENT",
    group_name="data_for_mine_bronze",
    description="EVENT from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def event_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.EVENT",
        snowflake_table_name="a_bronze_event",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )


@dg.asset(
    name="EVENTTYPE",
    group_name="data_for_mine_bronze",
    description="EVENTTYPE from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def eventtype_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.EVENTTYPE",
        snowflake_table_name="a_bronze_eventtype",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )


@dg.asset(
    name="EVENTCHAINCMTVAL",
    group_name="data_for_mine_bronze",
    description="EVENTCHAINCMTVAL from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def eventchaincmtval_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.EVENTCHAINCMTVAL",
        snowflake_table_name="a_bronze_eventchaincmtval",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )


@dg.asset(
    name="EVENTCHAINTYPE",
    group_name="data_for_mine_bronze",
    description="EVENTCHAINTYPE from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def eventchaintype_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.EVENTCHAINTYPE",
        snowflake_table_name="a_bronze_eventchaintype",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )


@dg.asset(
    name="BUSINESS_UNIT",
    group_name="data_for_mine_bronze",
    description="BUSINESS_UNIT from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def business_unit_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.BUSINESS_UNIT",
        snowflake_table_name="a_bronze_business_unit",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )


@dg.asset(
    name="BUSINESS_UNIT_TYPE",
    group_name="data_for_mine_bronze",
    description="BUSINESS_UNIT_TYPE from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def business_unit_type_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.BUSINESS_UNIT_TYPE",
        snowflake_table_name="a_bronze_business_unit_type",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )


@dg.asset(
    name="EQUIP",
    group_name="data_for_mine_bronze",
    description="EQUIP from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def equip_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.EQUIP",
        snowflake_table_name="a_bronze_equip",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )


@dg.asset(
    name="EQUIPCMTVAL",
    group_name="data_for_mine_bronze",
    description="EQUIPCMTVAL from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def equipcmtval_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.EQUIPCMTVAL",
        snowflake_table_name="a_bronze_equipcmtval",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )


@dg.asset(
    name="EQUIPTYPE",
    group_name="data_for_mine_bronze",
    description="EQUIPTYPE from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def equiptype_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.EQUIPTYPE",
        snowflake_table_name="a_bronze_equiptype",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )
################### Comon_Prod ###############
@dg.asset(
    name="VlinkDevice",
    group_name="data_for_mine_bronze",
    description="VlinkDevice from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def vlinkdevice_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.VlinkDevice",
        snowflake_table_name="a_bronze_vlinkdevice",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )

@dg.asset(
    name="VLinkEntete",
    group_name="data_for_mine_bronze",
    description="VLinkEntete from MSSQL → Snowflake via BCP + COPY INTO",
    kinds={"snowflake", "python", "sql", "bronze"}
)
def vlinkentete_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    result = extract_mssql_data(
        mssql_table_name="dbo.VLinkEntete",
        snowflake_table_name="a_bronze_vlinkentete",
        logger=context.log,
    )
    return dg.MaterializeResult(
        metadata={"rows_loaded": dg.MetadataValue.int(result["rows_loaded"])}
    )



##@dg.asset(
##    name="mts_vw_down_event_history",
##    group_name="data_for_mine_silver",
##    description="Mts_vw_down_event_history from MSSQL → Snowflake via BCP + COPY INTO",
##)
##def mts_vw_down_event_history_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
##    """mts_vw_down_event_history from MSSQL"""
##    result = extract_mssql_data(
##        snowflake_database = "NEEMBA",
##        snowflake_schema = "MINES", 
##        mssql_table_name = "mts_vw_down_event_history",
##        snowflake_table_name = "a_bronze_mts_vw_down_event_history",
##        logger = context.log,
##    )
##
##    return dg.MaterializeResult(
##        metadata={
##            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
##        }
##    )  

###@dg.asset(
###    name="V_facture_dashboard_am",
###    group_name="data_for_nmbai",
###    description="Facture_dashboard_am from MSSQL → Snowflake via BCP + COPY INTO",
###)
###def facture_dashboard_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
###    """Facture_dashboard_am from MSSQL"""
###    
###    result = extract_mssql_data(
###        snowflake_database = "NEEMBA",
###        snowflake_schema = "EQUIPEMENT", 
###        mssql_table_name="V_facture_dashboard_am",
###        snowflake_table_name="AI_V_facture_dashboard_am",
###        logger=context.log,
###    )
###
###    return dg.MaterializeResult(
###        metadata={
###            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
###        }
###    )  
###
###@dg.asset(
###    name="V_tiers_dashboard_am",
###    group_name="data_for_nmbai",
###    description="Tiers_dashboard_am from MSSQL → Snowflake via BCP + COPY INTO",
###)
###def tiers_dashboard_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
###    """Tiers_dashboard_am from MSSQL"""
###    
###    result = extract_mssql_data(
###        snowflake_database = "NEEMBA",
###        snowflake_schema = "EQUIPEMENT", 
###        mssql_table_name="V_tiers_dashboard_am",
###        snowflake_table_name="AI_V_tiers_dashboard_am",
###        logger=context.log,
###    )
###
###    return dg.MaterializeResult(
###        metadata={
###            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
###        }
###    )  
###
###
###@dg.asset(
###    name="GCM_Retour_Donnees_OLGA",
###    group_name="data_for_nmbai",
###    description="GCM_Retour_Donnees_OLGA from MSSQL → Snowflake via BCP + COPY INTO",
###)
###def gcm_retour_donnees_olga_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
###    """GCM_Retour_Donnees_OLGA from MSSQL"""
###    
###    result = extract_mssql_data(
###        snowflake_database = "NEEMBA",
###        snowflake_schema = "EQUIPEMENT", 
###        mssql_table_name="GCM_Retour_Donnees_OLGA",
###        snowflake_table_name="AI_GCM_Retour_Donnees_OLGA",
###        logger=context.log,
###    )
###
###    return dg.MaterializeResult(
###        metadata={
###            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
###        }
###    )  
###
###
###@dg.asset(
###    name="V_LEAD_PSE_Facture_Comm_Devis",
###    group_name="data_for_nmbai",
###    description="V_LEAD_PSE_Facture_Comm_Devis from MSSQL → Snowflake via BCP + COPY INTO",
###)
###def v_lean_pse_facture_comm_devis_assets(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
###    """V_LEAD_PSE_Facture_Comm_Devis from MSSQL"""
###    
###    result = extract_mssql_data(
###        snowflake_database = "NEEMBA",
###        snowflake_schema = "EQUIPEMENT", 
###        mssql_table_name="V_LEAD_PSE_Facture_Comm_Devis",
###        snowflake_table_name="V_LEAD_PSE_Facture_Comm_Devis",
###        logger=context.log,
###    )
###
###    return dg.MaterializeResult(
###        metadata={
###            "rows_loaded": dg.MetadataValue.int(result["rows_loaded"]),
###        }
###    )  
###
###### ASSET USING DLT
##@dlt_assets(
##    dlt_source=inventory_parts_ops_source(),
##    dlt_pipeline=pipeline,
##    name="v_Inventory_Parts_Ops",
##    group_name="data_for_nmbai",
##    #retry_policy=retry_policy,
##)
##def inventory_parts_ops_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
##    """Inventory Parts Ops from MSSQL"""
##    try:
##        yield from dlt.run(context=context)
##    except Exception as e:
##        context.log.error(f"❌ Inventory asset failed: {e}")
##        raise

###@dlt_assets(
###    dlt_source=equipment_source(),
###    dlt_pipeline=pipeline,
###    name="V_Equipment",
###    group_name="data_for_nmbai",
###    op_tags={"priority": "high"},
###    #retry_policy=retry_policy,
###)
###def equipment_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
###    """Equipment data from MSSQL"""
###    try:
###        yield from dlt.run(context=context)
###    except Exception as e:
###        context.log.error(f"❌ Equipment asset failed: {e}")
###        raise
###
###
###@dlt_assets(
###    dlt_source=facture_source(),
###    dlt_pipeline=pipeline,
###    name="V_facture_dashboard_am",
###    group_name="data_for_nmbai",
###    #retry_policy=retry_policy,
###)
###def facture_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
###    """Facture data from MSSQL"""
###    try:
###        yield from dlt.run(context=context)
###    except Exception as e:
###        context.log.error(f"❌ Facture asset failed: {e}")
###        raise
###
###
###@dlt_assets(
###    dlt_source=tiers_source(),
###    dlt_pipeline=pipeline,
###    name="V_tiers_dashboard_am",
###    group_name="data_for_nmbai",
###    #retry_policy=retry_policy,
###)
###def tiers_dashboard_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
###    """Tiers data from MSSQL"""
###    try:
###        yield from dlt.run(context=context)
###    except Exception as e:
###        context.log.error(f"❌ Tiers asset failed: {e}")
###        raise
###
###
###@dlt_assets(
###    dlt_source=gcm_retour_donnees_olga_source(),
###    dlt_pipeline=pipeline,
###    name="GCM_Retour_Donnees_OLGA",
###    group_name="data_for_nmbai",
###    #retry_policy=retry_policy,
###)
###def gcm_retour_donnees_olga_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
###    """GCM Retour Données OLGA from MSSQL"""
###    try:
###        yield from dlt.run(context=context)
###    except Exception as e:
###        context.log.error(f"❌ GCM asset failed: {e}")
###        raise


##@dlt_assets(
##    dlt_source=make_inventory_parts_ops_source(logging.getLogger(__name__)),
##    dlt_pipeline=pipeline,
##    name="v_Inventory_Parts_Ops",
##    group_name="data_for_nmbai",
##    #retry_policy=retry_policy,
##)
##def inventory_parts_ops_assets(context: dg.AssetExecutionContext, dlt: DagsterDltResource):
##    """Inventory Parts Ops from MSSQL"""
##    try:
##        dlt_source = make_inventory_parts_ops_source(context.log)
##        yield from dlt.run(context=context, dlt_source=dlt_source)
##    except Exception as e:
##        context.log.error(f"❌ Inventory asset failed: {e}")
##        raise



###  Cet asset utilise bcp + copy into
#@asset(
#    name="vlinklocalisation_bcp_fast",
#    group_name="DATA_FOR_NMB_AI",
#    description=f"BCP direct: {Config.MAX_ROWS:,} rows via BCP + COPY INTO",
#)
#def vlinklocalisation_bcp_asset(context: AssetExecutionContext) -> Output:
#    """
#    Asset Dagster inpired by the PowerShell script
#    """
#    
#    context.log.info("🚀 Démarrage pipeline BCP")
#    
#    result = run_pipeline()
#    
#    context.log.info(f"✅ Pipeline terminé: {result['rows_loaded']:,} lignes")
#    
#    return Output(
#        value=result,
#        metadata={
#            "rows_loaded": MetadataValue.int(result['rows_loaded']),
#            "errors": MetadataValue.int(result['errors']),
#            "duration_seconds": MetadataValue.float(result['duration']),
#            "method": MetadataValue.text("BCP + COPY INTO"),
#            "speed_rows_per_sec": MetadataValue.float(
#                result['rows_loaded'] / result['duration']
#            ),
#        }
#    )
