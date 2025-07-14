from datetime import datetime
import logging
from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import uuid
default_args = {
    'owner' : 'airscholar',
    'start_date' : datetime(2025, 6, 4, 18, 00)
}


def get_data():
    import json
    import requests

    API_KEY = '49707cf136074394945133259250206'  # replace with your actual API key
    CITY = 'Toulouse'
    URL = f'http://api.weatherapi.com/v1/current.json?key={API_KEY}&q={CITY}'

    response = requests.get(URL)

    if response.status_code == 200:
        weather_data = response.json()
        return weather_data

    else:
        raise Exception(f"Failed to fetch data: {response.status_code} - {response.text}")

def format_data(res):
    data = {}
    data['id'] = str(uuid.uuid4())
    data['location_city'] = res['location']['name']
    data['location_region'] = res['location']['region']
    data['location_country'] = res['location']['country']
    data['localtime'] = res['location']['localtime']
    data['current_temp_c'] = res['current']['temp_c']
    data['last_updated'] = res['current']['last_updated']
    data['description'] = res['current']['condition']['text']
    data['icon'] = res['current']['condition']['icon']
    data['feelslike_c'] = res['current']['feelslike_c']
    data["wind_kph"] = res['current']["wind_kph"]
    data["wind_degree"] = res['current']["wind_degree"]
    data["pressure_mb"] = res['current']["pressure_mb"]
    data["humidity"] = res['current']["humidity"]
    data["heatindex_c"] = res['current']["heatindex_c"]
    data["dewpoint_c"] = res['current']["dewpoint_c"]
    data["vis_km"] = res['current']["vis_km"]
    data["uv"] = res['current']["uv"]
    data["gust_kph"] = res['current']["gust_kph"]
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=15000)
    current_time = time.time()
    try:
        while True:
            if time.time() > current_time + 60:
                break
            res = get_data()
            res = format_data(res)
            logging.info("Sending data to Kafka...")
            producer.send('weather_data', json.dumps(res).encode('utf-8'))
            time.sleep(5)  # slight pause to avoid API rate limit
    except Exception as e: 
        logging.error(f'An error occurred: {e}')
        raise
    finally:
        if producer:
            logging.info("Flushing Kafka producer...")
            producer.flush()
            logging.info("Closing Kafka producer...")
            producer.close()


    

with DAG('weather_data_automation',
         default_args=default_args,
         schedule='@hourly',
         catchup=False) as dag : 
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )
