from dagster import Definitions, load_assets_from_modules

from . import assets
from .jobs import daily_update_job, adhoc_request_job
from .schedules import daily_schedule

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[daily_update_job, adhoc_request_job],
    schedules=[daily_schedule],
)
