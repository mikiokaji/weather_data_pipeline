from dagster import job, op, DynamicOut, DynamicOutput, In, resource
from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import requests
import pandas as pd
import logging
import os

# Setup logging configuration
logging.basicConfig(level=logging.INFO)

# Constants
BASE_URL = 'https://api.openweathermap.org/data/3.0/onecall'
API_KEY = os.getenv("OPENWEATHER_API_KEY")
DATABASE_URL = 'postgresql://mikiokaji:password@localhost/weather_data'  # Database connection URL

# Define the coordinates for various locations
coordinates = [
    (33.0198, -96.6989),   # Plano, TX
    (40.7128, -74.0060),   # New York City, NY
    (34.0522, -118.2437),  # Los Angeles, CA
    (39.7392, -104.9903),  # Denver, CO
    (41.8781, -87.6298),   # Chicago, IL
    (35.6762, 139.6503),   # Tokyo, Japan
    (51.5074, -0.1278)     # London, UK
]

# Define the SQLAlchemy base class
Base = declarative_base()

# Define the WeatherData table schema
class WeatherData(Base):
    __tablename__ = 'weather_data'
    __table_args__ = {'extend_existing': True}
    id = Column(Integer, primary_key=True, autoincrement=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    timezone = Column(String, nullable=False)
    local_time = Column(DateTime, nullable=False)
    temp = Column(Float, nullable=False)
    feels_like = Column(Float, nullable=False)
    pressure = Column(Integer, nullable=False)
    humidity = Column(Integer, nullable=False)
    dew_point = Column(Float, nullable=False)
    uvi = Column(Float, nullable=False)
    clouds = Column(Integer, nullable=False)
    visibility = Column(Integer, nullable=False)
    wind_speed = Column(Float, nullable=False)
    wind_deg = Column(Integer, nullable=False)
    weather_main = Column(String, nullable=False)
    weather_description = Column(String, nullable=False)

# Database connection setup
engine = create_engine(DATABASE_URL)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)
session = Session()

@resource
def weather_api_key_resource(_):
    # Return the API key for accessing the weather data
    return API_KEY

@op(out=DynamicOut())
def extract_data_for_locations(context):
    # Extract coordinates for the specified locations
    for lat, lon in coordinates:
        # Create a unique mapping key for each location
        mapping_key = f"lat_{lat:.4f}_lon_{lon:.4f}".replace('.', '_').replace('-', '_')
        # Log the processing of the current coordinates
        context.log.info(f"Processing coordinates: ({lat}, {lon}) with mapping_key: {mapping_key}")
        # Yield the coordinates with the generated mapping key
        yield DynamicOutput((lat, lon), mapping_key=mapping_key)

@op(ins={"location": In()}, required_resource_keys={"weather_api_key"})
def get_weather_data(context, location, exclude=None, units='metric', lang='en'):
    # Fetch weather data from the API for the given location
    lat, lon = location
    api_key = context.resources.weather_api_key
    params = {
        'lat': lat,
        'lon': lon,
        'appid': api_key,
        'units': units,
        'lang': lang,
        'exclude': exclude
    }
    try:
        response = requests.get(BASE_URL, params=params)
        response.raise_for_status()
        weather_data = response.json()
        context.log.info(f"Data fetched for coordinates: ({lat}, {lon})")
        context.log.debug(f"Weather data: {weather_data}")
        return weather_data
    except requests.exceptions.HTTPError as err:
        context.log.error(f"HTTP error occurred: {err}")
        return None
    except Exception as err:
        context.log.error(f"An error occurred: {err}")
        return None


def extract_current_weather_data(weather_data):
    # Extract the current weather data from the API response
    lat = weather_data.get('lat')
    lon = weather_data.get('lon')
    timezone = weather_data.get('timezone')
    current = weather_data.get('current', {})
    current_data = {
        "lat": lat,
        "lon": lon,
        "timezone": timezone,
        "dt": current.get('dt'),
        "temp": current.get('temp'),
        "feels_like": current.get('feels_like'),
        "pressure": current.get('pressure'),
        "humidity": current.get('humidity'),
        "dew_point": current.get('dew_point'),
        "uvi": current.get('uvi'),
        "clouds": current.get('clouds'),
        "visibility": current.get('visibility'),
        "wind_speed": current.get('wind_speed'),
        "wind_deg": current.get('wind_deg'),
        "weather_main": current.get('weather', [{}])[0].get('main'),
        "weather_description": current.get('weather', [{}])[0].get('description')
    }
    return current_data

def transform_weather_data(data):
    # Transform the extracted weather data into a DataFrame and clean it
    df = pd.DataFrame(data)
    df['dt'] = pd.to_datetime(df['dt'], unit='s')
    df['local_time'] = df.apply(lambda row: row['dt'].tz_localize('UTC').tz_convert(row['timezone']), axis=1)
    df['temp'] = df['temp'].astype(float)
    df['feels_like'] = df['feels_like'].astype(float)
    df['pressure'] = df['pressure'].astype(int)
    df['humidity'] = df['humidity'].astype(int)
    df['dew_point'] = df['dew_point'].astype(float)
    df['uvi'] = df['uvi'].astype(float)
    df['clouds'] = df['clouds'].astype(int)
    df['visibility'] = df['visibility'].astype(int)
    df['wind_speed'] = df['wind_speed'].astype(float)
    df['wind_deg'] = df['wind_deg'].astype(int)
    df['weather_main'] = df['weather_main'].astype(str).str.lower()
    df['weather_description'] = df['weather_description'].astype(str).str.lower()
    df.fillna({
        'temp': 0.0,
        'feels_like': 0.0,
        'pressure': 0,
        'humidity': 0,
        'dew_point': 0.0,
        'uvi': 0.0,
        'clouds': 0,
        'visibility': 0,
        'wind_speed': 0.0,
        'wind_deg': 0,
        'weather_main': 'unknown',
        'weather_description': 'unknown'
    }, inplace=True)
    cols = list(df.columns)
    cols.insert(cols.index('lon') + 1, cols.pop(cols.index('local_time')))
    df = df[cols]
    return df

def insert_weather_data(session, data):
    # Insert the transformed weather data into the database
    for record in data:
        weather_record = WeatherData(
            lat=record['lat'],
            lon=record['lon'],
            timezone=record['timezone'],
            local_time=record['local_time'],
            temp=record['temp'],
            feels_like=record['feels_like'],
            pressure=record['pressure'],
            humidity=record['humidity'],
            dew_point=record['dew_point'],
            uvi=record['uvi'],
            clouds=record['clouds'],
            visibility=record['visibility'],
            wind_speed=record['wind_speed'],
            wind_deg=record['wind_deg'],
            weather_main=record['weather_main'],
            weather_description=record['weather_description']
        )
        session.add(weather_record)
    session.commit()

@op(ins={"weather_data": In()})
def extract_transform_weather_data(weather_data):
    # Extract and transform the weather data
    extracted_data = extract_current_weather_data(weather_data)
    transformed_data = transform_weather_data([extracted_data])
    return transformed_data

@op(ins={"transformed_data": In()})
def load_weather_data_to_db(context, transformed_data):
    # Load the transformed weather data into the database
    data_as_dict = transformed_data.to_dict(orient='records')
    context.log.info(f'insert_weather_data {data_as_dict}')
    insert_weather_data(session, data_as_dict)

@job(resource_defs={"weather_api_key": weather_api_key_resource})
def weather_data_pipeline():
    # Define the weather data pipeline
    locations = extract_data_for_locations()
    weather_data = locations.map(get_weather_data)
    transformed_data = weather_data.map(extract_transform_weather_data)
    transformed_data.map(load_weather_data_to_db)

def define_weather_data_pipeline():
    # Define the weather data pipeline function
    return weather_data_pipeline

# Main function to execute the pipeline
if __name__ == "__main__":
    weather_data_pipeline.execute_in_process()