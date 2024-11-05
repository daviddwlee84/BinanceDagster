from dagster import Definitions, load_assets_from_modules

from . import assets
from .jobs import klines_update_job

all_assets = load_assets_from_modules([assets])

defs = Definitions(
    assets=all_assets,
    jobs=[klines_update_job],
)
