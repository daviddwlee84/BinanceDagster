from dagster import (
    StaticPartitionsDefinition,
    DailyPartitionsDefinition,
    MultiPartitionsDefinition,
)

START_DATE = "2023-11-01"

daily_partition = DailyPartitionsDefinition(start_date=START_DATE)

# https://docs.dagster.io/concepts/partitions-schedules-sensors/partitioning-assets#multi-dimensionally-partitioned-assets
# Define symbols and dates
# TODO: Replace with dynamic fetching
# TODO: Figure out dynamic length of symbols on different dates
# https://github.com/binance/binance-public-data/blob/master/python/utility.py
# @lru_cache(3)
# def get_all_symbols(type):
#     if type == 'um':
#         response = urllib.request.urlopen("https://fapi.binance.com/fapi/v1/exchangeInfo").read()
#     elif type == 'cm':
#         response = urllib.request.urlopen("https://dapi.binance.com/dapi/v1/exchangeInfo").read()
#     else:
#         response = urllib.request.urlopen("https://api.binance.com/api/v3/exchangeInfo").read()
#     return list(map(lambda symbol: symbol['symbol'], json.loads(response)['symbols']))
symbols = ["BTCUSDT", "ETHUSDT", "BNBUSDT"]

symbol_partitions = StaticPartitionsDefinition(symbols)

# Combine partitions
date_symbol_partition = MultiPartitionsDefinition(
    {
        "symbol": symbol_partitions,
        "date": daily_partition,
    }
)
