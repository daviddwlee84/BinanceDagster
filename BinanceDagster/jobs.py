from dagster import AssetSelection, define_asset_job
from .partitions import daily_partition

# Regular Jobs

btc_klines_1m_daily = AssetSelection.assets("btc_klines_1m_daily")

daily_update_job = define_asset_job(
    name="daily_update_job",
    partitions_def=daily_partition,
    selection=btc_klines_1m_daily,
)

# Adhoc Jobs

adhoc_btc_klines_1m = AssetSelection.assets("adhoc_btc_klines_1m")

adhoc_request_job = define_asset_job(
    name="adhoc_request_job",
    selection=adhoc_btc_klines_1m,
    config={
        "ops": {
            "adhoc_btc_klines_1m": {
                "config": {
                    "start_date": "2024-11-01",
                    "end_date": "2024-11-02",
                }
            }
        }
    },
)
