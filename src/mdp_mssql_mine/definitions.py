from pathlib import Path

from dagster import Definitions, load_from_defs_folder, ScheduleDefinition, define_asset_job
from dagster_dlt import DagsterDltResource
from mdp_mssql_mine.defs.load.assets import(
    mns_d_site_assets,
    mns_f_utilisation_equipement_assets,
    ref_vw_nba_machines_assets,
    #mts_vw_down_event_history_assets,
    mns_d_type_evenement_arret_assets,
    mns_f_equipement_arret_assets,
    diag_f_alerte_assets,
    diag_d_diagnostique_cid_assets,
    diag_d_diagnostique_eid_assets,
    diag_d_diagnostique_fmi_assets,
    diag_d_diagnostique_mid_assets,
    v_metaform45_assets,
    v_metaform36_assets,
    v_metaform84_assets,
    # bronze assets
    eventchain_assets,
    event_assets,
    eventtype_assets,
    eventchaincmtval_assets,
    eventchaintype_assets,
    business_unit_assets,
    business_unit_type_assets,
    equip_assets,
    equipcmtval_assets,
    equiptype_assets,
)

########  jobs
mns_d_site_job = define_asset_job(
    name="mns_d_site_assets_job",
    selection=[mns_d_site_assets],
)

mns_f_utilisation_equipement_job = define_asset_job(
    name="mns_f_utilisation_equipement_job",
    selection=[mns_f_utilisation_equipement_assets],
)

ref_vw_nba_machines_job = define_asset_job(
    name="ref_vw_nba_machines_job",
    selection=[ref_vw_nba_machines_assets],
)
#mts_vw_down_event_history_job = define_asset_job(
#    name="mts_vw_down_event_history_job",
#    selection=[mts_vw_down_event_history_assets],
#)
#
mns_d_type_evenement_arret_job = define_asset_job(
    name="mns_d_type_evenement_arret_job",
    selection=[mns_d_type_evenement_arret_assets],
)

mns_f_equipement_arret_job = define_asset_job(
    name="mns_f_equipement_arret_job",
    selection=[mns_f_equipement_arret_assets],
)

diag_f_alerte_job = define_asset_job(
    name="diag_f_alerte_job",
    selection=[diag_f_alerte_assets],
)

diag_d_diagnostique_cid_job = define_asset_job(
    name="diag_d_diagnostique_cid_job",
    selection=[diag_d_diagnostique_cid_assets],
)

diag_d_diagnostique_eid_job = define_asset_job(
    name="diag_d_diagnostique_eid_job",
    selection=[diag_d_diagnostique_eid_assets],
)

diag_d_diagnostique_fmi_job = define_asset_job(
    name="diag_d_diagnostique_fmi_job",
    selection=[diag_d_diagnostique_eid_assets],
)

diag_d_diagnostique_mid_job = define_asset_job(
    name="diag_d_diagnostique_mid_job",
    selection=[diag_d_diagnostique_fmi_assets],
)

v_metaform45_job = define_asset_job(
    name="v_metaform45_job",
    selection=[v_metaform45_assets],
)

v_metaform36_job = define_asset_job(
    name="v_metaform36_job",
    selection=[v_metaform36_assets],
)
v_metaform84_job = define_asset_job(
    name="v_metaform84_job",
    selection=[v_metaform84_assets],
)


eventchain_job = define_asset_job(
    name="eventchain_assets_job",
    selection=[eventchain_assets],
)

event_job = define_asset_job(
    name="event_assets_job",
    selection=[event_assets],
)

eventtype_job = define_asset_job(
    name="eventtype_assets_job",
    selection=[eventtype_assets],
)

eventchaincmtval_job = define_asset_job(
    name="eventchaincmtval_assets_job",
    selection=[eventchaincmtval_assets],
)

eventchaintype_job = define_asset_job(
    name="eventchaintype_assets_job",
    selection=[eventchaintype_assets],
)

business_unit_job = define_asset_job(
    name="business_unit_assets_job",
    selection=[business_unit_assets],
)

business_unit_type_job = define_asset_job(
    name="business_unit_type_assets_job",
    selection=[business_unit_type_assets],
)

equip_job = define_asset_job(
    name="equip_assets_job",
    selection=[equip_assets],
)

equipcmtval_job = define_asset_job(
    name="equipcmtval_assets_job",
    selection=[equipcmtval_assets],
)

equiptype_job = define_asset_job(
    name="equiptype_assets_job",
    selection=[equiptype_assets],
)


####### schedule : every day
mns_d_site_schedule = ScheduleDefinition(
    job=mns_d_site_job,
    cron_schedule="0 0 * * *", ## every day
)

mns_f_utilisation_equipement_schedule = ScheduleDefinition(
    job=mns_f_utilisation_equipement_job,
    cron_schedule="0 0 * * *", ## every day
)

ref_vw_nba_machines_schedule = ScheduleDefinition(
    job=ref_vw_nba_machines_job,
    cron_schedule="0 0 * * *", ## every day
)

#mts_vw_down_event_history_schedule = ScheduleDefinition(
#    job=mts_vw_down_event_history_job,
#    cron_schedule="0 0 * * *", ## every day
#)


mns_d_type_evenement_arret_schedule = ScheduleDefinition(
    job=mns_d_type_evenement_arret_job,
    cron_schedule="0 0 * * *", ## every day
)

mns_f_equipement_arret_schedule = ScheduleDefinition(
    job=mns_f_equipement_arret_job,
    cron_schedule="0 0 * * *", ## every day
)

diag_f_alerte_schedule = ScheduleDefinition(
    job=diag_f_alerte_job,
    cron_schedule="0 0 * * *", ## every day
)

diag_d_diagnostique_eid_schedule = ScheduleDefinition(
    job=diag_d_diagnostique_eid_job,
    cron_schedule="0 0 * * *", ## every day
)

diag_d_diagnostique_cid_schedule = ScheduleDefinition(
    job=diag_d_diagnostique_cid_job,
    cron_schedule="0 0 * * *", ## every day
)

diag_d_diagnostique_fmi_schedule = ScheduleDefinition(
    job=diag_d_diagnostique_fmi_job,
    cron_schedule="0 0 * * *", ## every day
)

diag_d_diagnostique_mid_schedule = ScheduleDefinition(
    job=diag_d_diagnostique_mid_job,
    cron_schedule="0 0 * * *", ## every day
)

v_metaform45_schedule = ScheduleDefinition(
    job=v_metaform45_job,
    cron_schedule="0 0 * * *", ## every day
)

v_metaform36_schedule = ScheduleDefinition(
    job=v_metaform36_job,
    cron_schedule="0 0 * * *", ## every day
)

v_metaform84_schedule = ScheduleDefinition(
    job=v_metaform84_job,
    cron_schedule="0 0 * * *", ## every day
)


eventchain_schedule = ScheduleDefinition(
    job=eventchain_job,
    cron_schedule="0 0 * * *",  # every day
)

event_schedule = ScheduleDefinition(
    job=event_job,
    cron_schedule="0 0 * * *",  # every day
)

eventtype_schedule = ScheduleDefinition(
    job=eventtype_job,
    cron_schedule="0 0 * * *",  # every day
)

eventchaincmtval_schedule = ScheduleDefinition(
    job=eventchaincmtval_job,
    cron_schedule="0 0 * * *",  # every day
)

eventchaintype_schedule = ScheduleDefinition(
    job=eventchaintype_job,
    cron_schedule="0 0 * * *",  # every day
)

business_unit_schedule = ScheduleDefinition(
    job=business_unit_job,
    cron_schedule="0 0 * * *",  # every day
)

business_unit_type_schedule = ScheduleDefinition(
    job=business_unit_type_job,
    cron_schedule="0 0 * * *",  # every day
)

equip_schedule = ScheduleDefinition(
    job=equip_job,
    cron_schedule="0 0 * * *",  # every day
)

equipcmtval_schedule = ScheduleDefinition(
    job=equipcmtval_job,
    cron_schedule="0 0 * * *",  # every day
)

equiptype_schedule = ScheduleDefinition(
    job=equiptype_job,
    cron_schedule="0 0 * * *",  # every day
)
###### Defs
defs = Definitions(
    jobs= [
        mns_d_site_job,
        mns_f_utilisation_equipement_job,
        ref_vw_nba_machines_job,
        #mts_vw_down_event_history_job,
        mns_d_type_evenement_arret_job,
        mns_f_equipement_arret_job,
        diag_f_alerte_job,
        diag_d_diagnostique_eid_job,
        diag_d_diagnostique_cid_job,
        diag_d_diagnostique_fmi_job,
        diag_d_diagnostique_mid_job,
        v_metaform45_job,
        v_metaform36_job,
        v_metaform84_job,
        eventchain_job,
        event_job,
        eventtype_job,
        eventchaincmtval_job,
        eventchaintype_job,
        business_unit_job,
        business_unit_type_job,
        equip_job,
        equipcmtval_job,
        equiptype_job,
    ],
    assets=[
        mns_d_site_assets,
        mns_f_utilisation_equipement_assets,
        ref_vw_nba_machines_assets,
        #mts_vw_down_event_history_assets,
        mns_d_type_evenement_arret_assets,
        mns_f_equipement_arret_assets,
        diag_f_alerte_assets,
        diag_d_diagnostique_cid_assets,
        diag_d_diagnostique_eid_assets,
        diag_d_diagnostique_fmi_assets,
        diag_d_diagnostique_mid_assets,
        v_metaform45_assets,
        v_metaform36_assets,
        v_metaform84_assets,
         # bronze assets
        eventchain_assets,
        event_assets,
        eventtype_assets,
        eventchaincmtval_assets,
        eventchaintype_assets,
        business_unit_assets,
        business_unit_type_assets,
        equip_assets,
        equipcmtval_assets,
        equiptype_assets,
    ],
    resources={
        "dlt":DagsterDltResource(),
    },
    schedules = [
        mns_d_site_schedule,
        mns_f_utilisation_equipement_schedule,
        ref_vw_nba_machines_schedule,
        #mts_vw_down_event_history_schedule,
        mns_d_type_evenement_arret_schedule,
        mns_f_equipement_arret_schedule,
        diag_f_alerte_schedule,
        diag_d_diagnostique_eid_schedule,
        diag_d_diagnostique_cid_schedule,
        diag_d_diagnostique_fmi_schedule,
        diag_d_diagnostique_mid_schedule,
        v_metaform45_schedule,
        v_metaform36_schedule,
        v_metaform84_schedule,
         # bronze schedules
        eventchain_schedule,
        event_schedule,
        eventtype_schedule,
        eventchaincmtval_schedule,
        eventchaintype_schedule,
        business_unit_schedule,
        business_unit_type_schedule,
        equip_schedule,
        equipcmtval_schedule,
        equiptype_schedule,
    ]
)


