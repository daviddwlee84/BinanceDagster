from dagster import AssetSelection, define_asset_job

btc_klines_1m = AssetSelection.assets("btc_klines_1m")

klines_update_job = define_asset_job(
    name="klines_update_job",
    selection=btc_klines_1m,
)
