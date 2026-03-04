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
    diag_d_diagnostique_mid_assets
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
        diag_d_diagnostique_mid_assets
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
    ]
)


