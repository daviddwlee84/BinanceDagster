from dagster import AssetSelection, define_asset_job
from .partitions import daily_partition

btc_klines_1m_daily = AssetSelection.assets("btc_klines_1m_daily")

daily_update_job = define_asset_job(
    name="daily_update_job",
    partitions_def=daily_partition,
    selection=btc_klines_1m_daily,
)
