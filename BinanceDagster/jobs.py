from dagster import AssetSelection, define_asset_job

btc_klines_1m_daily = AssetSelection.assets("btc_klines_1m_daily")

daily_update_job = define_asset_job(
    name="daily_update_job",
    selection=btc_klines_1m_daily,
)
