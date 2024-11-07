from dagster import Config


class AdhocRequestConfig(Config):
    start_date: str
    end_date: str
