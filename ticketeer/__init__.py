from dagster import AssetSelection, Definitions, EnvVar, ScheduleDefinition, define_asset_job, load_assets_from_modules

from . import assets, resources

all_assets = load_assets_from_modules([assets])

request_access_job = define_asset_job("approval_job", selection=AssetSelection.all())
request_access_schedule = ScheduleDefinition(
    job=request_access_job,
    cron_schedule="0 * * * *",  # every hour
)

defs = Definitions(
    assets=all_assets,
    schedules=[request_access_schedule],
    resources={
        "gsheet": resources.GoogleSheetResource(
            sheet_id=EnvVar("GOOGLE_SHEET_ID"), 
            google_application_credentials=EnvVar("GOOGLE_APPLICATION_CREDENTIALS")),
    }
)
