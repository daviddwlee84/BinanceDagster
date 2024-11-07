from dagster import (
    SensorResult,
    sensor,
    SensorEvaluationContext,
    AssetMaterialization,
)
import datetime
from .utilities import download_checksum_file


# https://docs.dagster.io/_apidocs/assets#dagster.AutomationConditionSensorDefinition
# https://docs.dagster.io/concepts/automation/declarative-automation/customizing-automation-conditions#arbitary-python-automationconditions
@sensor(minimum_interval_seconds=86400)
def binance_klines_1m_daily_sensor(context: SensorEvaluationContext) -> SensorResult:
    # Materialization happened in external system, but is recorded here
    # https://www.30secondsofcode.org/python/s/days-ago-days-from-now-add-subtract-dates/
    utc_time = datetime.datetime.now(datetime.timezone.utc)
    date_str_two_days_ago = (utc_time - datetime.timedelta(days=2)).strftime("%Y-%m-%d")

    path = f"data/spot/daily/klines/BTCUSDT/1m/BTCUSDT-1m-{date_str_two_days_ago}"
    # URLs to download
    file_url = f"https://data.binance.vision/{path}.zip"
    checksum_url = file_url + ".CHECKSUM"

    try:
        # Try to get checksum
        download_checksum_file(checksum_url)
        return SensorResult(
            asset_events=[
                AssetMaterialization(
                    asset_key="raw_btc_klines_1m_daily",
                    partition=date_str_two_days_ago,
                    metadata={
                        "source": f'Materialize raw_btc_klines_1m_daily {date_str_two_days_ago} From sensor "{context.sensor_name}" at UTC time "{utc_time.strftime("%Y-%m-%d, %H:%M:%S")}"'
                    },
                ),
                # AssetMaterialization(
                #     asset_key="btc_klines_1m_daily",
                #     partition=date_str_two_days_ago,
                #     metadata={
                #         "source": f'Materialize btc_klines_1m_daily {date_str_two_days_ago} From sensor "{context.sensor_name}" at UTC time "{utc_time.strftime("%Y-%m-%d, %H:%M:%S")}"'
                #     },
                # ),
            ]
        )
    except:
        context.log.info(f"File haven't produced at {file_url} yet. Will check again.")
