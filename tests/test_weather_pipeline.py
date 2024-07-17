API_KEY = '54fc74fbbb09f2576d8de10e5b606388'

import pandas as pd
from dagster import execute_job, build_op_context, DagsterInstance, reconstructable
import os
import sys

# Ensure the module is discoverable in the subprocess
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from weather_pipeline import (
    extract_data_for_locations,
    get_weather_data,
    extract_transform_weather_data,
    load_weather_data_to_db,
    define_weather_data_pipeline,
    coordinates,
    extract_current_weather_data,
    transform_weather_data
)

# Mock data for testing
mock_weather_data = {
    "lat": 40.7128,
    "lon": -74.0060,
    "timezone": "America/New_York",
    "current": {
        "dt": 1625812800,
        "temp": 298.77,
        "feels_like": 298.77,
        "pressure": 1016,
        "humidity": 53,
        "dew_point": 288.71,
        "uvi": 0.89,
        "clouds": 40,
        "visibility": 10000,
        "wind_speed": 4.12,
        "wind_deg": 150,
        "weather": [{"main": "Clouds", "description": "few clouds"}]
    }
}

# Test function for extracting data for locations
def test_extract_data_for_locations():
    context = build_op_context()
    result = extract_data_for_locations(context)
    locations = list(result)
    assert len(locations) == len(coordinates)

# Test function for fetching weather data
def test_get_weather_data():
    context = build_op_context(resources={"weather_api_key": API_KEY})
    result = get_weather_data(context, location=(40.7128, -74.0060))
    assert result is not None
    assert "current" in result

# Test function for extracting and transforming weather data
def test_extract_transform_weather_data():
    context = build_op_context()
    result = extract_transform_weather_data(context, weather_data=mock_weather_data)
    assert isinstance(result, pd.DataFrame)
    assert not result.empty

# Mock function to insert weather data into the database
def mock_insert_weather_data(session, data):
    assert isinstance(data, list)
    assert len(data) > 0
    assert "lat" in data[0]

# Test function for loading weather data to the database
def test_load_weather_data_to_db(monkeypatch):
    # Use monkeypatch to replace the original insert_weather_data function with the mock version
    monkeypatch.setattr('weather_data_pipeline.weather_pipeline.insert_weather_data', mock_insert_weather_data)

    # Transform the mock data
    transformed_data = transform_weather_data([extract_current_weather_data(mock_weather_data)])

    # Execute the solid with the transformed data
    context = build_op_context()
    load_weather_data_to_db(context, transformed_data=transformed_data)

# Test function for executing the entire weather data pipeline
def test_weather_data_pipeline():
    instance = DagsterInstance.local_temp()
    result = execute_job(
        reconstructable(define_weather_data_pipeline),
        instance=instance
    )
    assert result.success