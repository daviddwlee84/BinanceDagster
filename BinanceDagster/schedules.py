from dagster import ScheduleDefinition
from .jobs import daily_update_job

# https://docs.dagster.io/concepts/automation/schedules
daily_schedule = ScheduleDefinition(
    job=daily_update_job,
    # https://crontab.guru/
    cron_schedule="0 0 * * *",  # Every day at midnight
    # https://docs.dagster.io/concepts/automation/schedules/customizing-executing-timezones
    # https://www.iana.org/time-zones
    execution_timezone="Asia/Taipei",
)
