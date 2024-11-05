from dagster import DailyPartitionsDefinition

START_DATE = "2023-11-01"

daily_partition = DailyPartitionsDefinition(start_date=START_DATE)
