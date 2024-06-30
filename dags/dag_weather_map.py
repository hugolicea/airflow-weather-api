import json
import pandas as pd
import pytz
import os

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from datetime import datetime, timedelta, timezone

# Set the base path for the file
base_path = os.getcwd()


def get_key(key):
    return Variable.get(key)

def kelvin_to_fahrenheit(kelvin):
    return round((kelvin - 273.15) * 9/5 + 32, 2)

def transform_weather_data(**kwargs):
    # Extract the data from the XCom
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_weather_data')
    city = data["name"]
    weather_description = data["weather"][0]['description']
    weather_main = data["weather"][0]["main"]
    temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp"])
    feels_like_farenheit= kelvin_to_fahrenheit(data["main"]["feels_like"])
    min_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_min"])
    max_temp_farenheit = kelvin_to_fahrenheit(data["main"]["temp_max"])
    pressure = data["main"]["pressure"]
    humidity = data["main"]["humidity"]
    wind_speed = data["wind"]["speed"]
    clouds_all = data["clouds"]["all"]
    time_of_record = datetime.fromtimestamp(data['dt'] + data['timezone'], tz=timezone.utc)
    sunrise_time = datetime.fromtimestamp(data['sys']['sunrise'] + data['timezone'], tz=timezone.utc)
    sunset_time = datetime.fromtimestamp(data['sys']['sunset'] + data['timezone'], tz=timezone.utc)
            
    transformed_data = {
        "city": city,
        "weather_description": weather_description,
        "weather_main": weather_main,
        "temp_farenheit": temp_farenheit,
        "feels_like_farenheit": feels_like_farenheit,
        "min_temp_farenheit": min_temp_farenheit,
        "max_temp_farenheit": max_temp_farenheit,
        "pressure": pressure,
        "humidity": humidity,
        "wind_speed": wind_speed,
        "clouds_all": clouds_all,
        "time_of_record": time_of_record,
        "sunrise_time": sunrise_time,
        "sunset_time": sunset_time
    }
    
    transformed_data_list = [transformed_data]
    df = pd.DataFrame(transformed_data_list)
    
    dt_string = time_of_record.strftime("%Y%m%d")
    file_name = f"/tmp/weather_data_{dt_string}.csv"
    print(f"Saving data to {file_name}")
    df.to_csv(file_name, index=False)
    
        

# Define default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 6, 29, tzinfo=pytz.timezone('US/Central')),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define your DAG
with DAG(
    'dag_weather_map',
    default_args=default_args,
    description='A DAG for weather mapping',
    schedule_interval='@daily',  # Runs daily at midnight
    start_date=datetime(2022, 1, 1),
    catchup=False,
    tags=['weather', 'api'],
) as dag:

    is_weather_api_available = HttpSensor(
        task_id='is_weather_api_available',
        http_conn_id='weather_api',
        endpoint=f'/data/2.5/weather?q=Dallas&appid={get_key("weather_api_key")}',
    )
    
    extract_weather_data = SimpleHttpOperator(
        task_id='extract_weather_data',
        http_conn_id='weather_api',
        endpoint=f'/data/2.5/weather?q=Dallas&appid={get_key("weather_api_key")}',
        method='GET',
        response_filter=lambda response: json.loads(response.text),
        log_response=True,
    )
    
    
    transform_weather_data = PythonOperator(
        task_id='transform_weather_data',
        python_callable=transform_weather_data,
    )

    # Set task dependencies
    is_weather_api_available >> extract_weather_data >> transform_weather_data
