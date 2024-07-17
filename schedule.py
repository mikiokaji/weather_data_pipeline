from dagster import schedule
from weather_pipeline import weather_data_pipeline

@schedule(cron_schedule="*/5 * * * *", job=weather_data_pipeline, execution_timezone="America/Chicago")
def every_five_minutes_weather_data_schedule(_context):
    return {}

@schedule(cron_schedule="0 * * * *", job=weather_data_pipeline, execution_timezone="America/Chicago")
def every_hour_weather_data_schedule(_context):
    return {}

@schedule(cron_schedule="* * * * *", job=weather_data_pipeline, execution_timezone="America/Chicago")
def every_minute_weather_data_schedule(_context):
    return {}