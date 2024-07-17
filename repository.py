from dagster import repository
from weather_pipeline import weather_data_pipeline
from schedule import (
    every_five_minutes_weather_data_schedule,
    every_hour_weather_data_schedule,
    every_minute_weather_data_schedule
)

@repository
def weather_data_repository():
    return [weather_data_pipeline,
            every_five_minutes_weather_data_schedule,
            every_hour_weather_data_schedule,
            every_minute_weather_data_schedule]



