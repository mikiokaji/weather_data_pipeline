# Weather Data Pipeline

This project is a data pipeline that extracts weather data from the OpenWeatherMap API, transforms it, and loads it into a PostgreSQL database.

## Prerequisites
- Python 3.6+
- PostgreSQL
- OpenWeatherMap API key

## Installation
1. Clone the repo:
    ```sh
    git clone https://github.com/mikiokaji/weather_data_pipeline.git
    cd weather_data_pipeline
    ```
2. Create and activate a virtual environment:
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows, use `venv\Scripts\activate`
    ```
3. Install dependencies:
    ```sh
    pip install -r requirements.txt
    ```
4. Set up PostgreSQL and create a database named `weather_data`.

## Configuration
1. Set your OpenWeatherMap API key:
    ```sh
    export OPENWEATHER_API_KEY='your_api_key'  # On Windows, use `set` instead of `export`
    ```
2. Update `DATABASE_URL` in `weather_pipeline.py` to match your PostgreSQL setup:
    ```python
    DATABASE_URL = 'postgresql://<username>:<password>@localhost/weather_data'
    ```

## Running the Project
```sh
python weather_pipeline.py
```

## Database Schema

```sql
CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    lat FLOAT NOT NULL,
    lon FLOAT NOT NULL,
    timezone VARCHAR NOT NULL,
    local_time TIMESTAMPTZ NOT NULL,
    temp FLOAT NOT NULL,
    feels_like FLOAT NOT NULL,
    pressure INT NOT NULL,
    humidity INT NOT NULL,
    dew_point FLOAT NOT NULL,
    uvi FLOAT NOT NULL,
    clouds INT NOT NULL,
    visibility INT NOT NULL,
    wind_speed FLOAT NOT NULL,
    wind_deg INT NOT NULL,
    weather_main VARCHAR NOT NULL,
    weather_description VARCHAR NOT NULL
);
```

## ETL Process
- **Extract**: Fetch weather data from OpenWeatherMap API.
- **Transform**: Clean and format data.
- **Load**: Insert data into PostgreSQL database.

## Scheduling with Dagster

### To list schedules
```sh
dagster schedule list -w workspace.yaml --location repository.py
```

### To start a schedule
```sh
dagster schedule start -w workspace.yaml --location repository.py every_five_minutes_weather_data_schedule
dagster schedule start -w workspace.yaml --location repository.py every_hour_weather_data_schedule
dagster schedule start -w workspace.yaml --location repository.py every_minute_weather_data_schedule
```

### To stop a schedule
```sh
dagster schedule stop -w workspace.yaml --location repository.py every_five_minutes_weather_data_schedule
dagster schedule stop -w workspace.yaml --location repository.py every_hour_weather_data_schedule
dagster schedule stop -w workspace.yaml --location repository.py every_minute_weather_data_schedule
```

### To start the Dagster UI and manage schedules

1. Open the UI using the command in a terminal:
    ```sh
    dagster dev -w workspace.yaml
    ```

2. In another terminal, list the schedules:
    ```sh
    dagster schedule list -w workspace.yaml --location repository.py
    ```

3. In the same terminal, start the desired schedule:
    ```sh
    dagster schedule start -w workspace.yaml --location repository.py <schedule_name>
    ```

4. Confirm the schedule is running in the Dagster UI.

5. To stop the schedule, run the stop command in the same terminal:
    ```sh
    dagster schedule stop -w workspace.yaml --location repository.py <schedule_name>
    ```

## Testing

Run unit tests:
```sh
export PYTHONPATH=$PYTHONPATH:/path/to/weather_data_pipeline
pytest tests/test_weather_pipeline.py
```