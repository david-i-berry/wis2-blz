from dagster import (
    AssetSelection,
    Definitions,
    define_asset_job,
    load_assets_from_modules,
    ScheduleDefinition
)

from . import assets

all_assets = load_assets_from_modules([assets])

# Addition: define a job that will materialize the assets
synop_job = define_asset_job("publish_synop", selection=AssetSelection.all())

# Addition: a ScheduleDefinition the job it should run and a cron schedule of how frequently to run it
synop_schedule = ScheduleDefinition(
    job= synop_job,
    cron_schedule="*/15 * * * *",
)

defs = Definitions(
    assets=all_assets,
    schedules=[synop_schedule],
)

