from pathlib import Path

from dagster import Definitions, load_from_defs_folder, ScheduleDefinition, define_asset_job
from dagster_dlt import DagsterDltResource
from mdp_mssql_mine.defs.assets import(
    mns_d_site_assets,
    mns_f_utilisation_equipement_assets,
    ref_vw_nba_machines_assets,

)

#jobs
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


#schedule : every day
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

defs = Definitions(
    jobs= [mns_d_site_job,mns_f_utilisation_equipement_job,ref_vw_nba_machines_job],
    assets=[mns_d_site_assets,mns_f_utilisation_equipement_assets,ref_vw_nba_machines_assets],
    resources={
        "dlt":DagsterDltResource(),
    },
    schedules = [mns_d_site_schedule,mns_f_utilisation_equipement_schedule,ref_vw_nba_machines_schedule]
)


