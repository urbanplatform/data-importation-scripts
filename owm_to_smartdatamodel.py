
import datetime
import json
import logging as log
import copy
import requests

import pandas as pd
from nextcloud import init_client, upload_to_cloud, download_from_cloud
from utils import post_request, get_request
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates

from kombu import Connection
from kombu import Exchange
from kombu import Producer

try:
    from constants import OPEN_WEATHER_API_KEY, RAW_OPEN_WEATHER_URL
    from constants import RABBIT_URL_NEANIAS
    from utils import connect_to_rabbit_no_exchange
except ImportError:
    from dags.constants import OPEN_WEATHER_API_KEY, RAW_OPEN_WEATHER_URL
    from dags.constants import RABBIT_URL_NEANIAS
    from dags.utils import connect_to_rabbit_no_exchange

# DAG Variables
DAG_NAME = 'OWM_to_SmartDataModel'
SDL_SDM_WEATHER = '0 */1 * * *'  # At minute 0 past every hour


HELSINKI_LATITUDE = 60.1699 
HELSINKI_LONGITUDE = 24.9384
OPEN_WEATHER_URL = '{}/weather?lat={}&lon={}&appid={}'.format(
    RAW_OPEN_WEATHER_URL, HELSINKI_LATITUDE, HELSINKI_LONGITUDE, OPEN_WEATHER_API_KEY
)

#Rabbit Variables
RABBIT_QUEUE = "SmatDataModel"  
RABBIT_USER = 'guest'
RABBIT_PASSWORD = 'guest'
RABBIT_URL='neanias-management.urbanplatform.city'#'64.225.98.211'
RABBIT_PORT = '5672'



class WeatherSmartDataModel:
    def __init__(self, id, type, address, dateObserved, areaServed, location, source, typeOfLocation, precipitation, relativeHumidity, temperature, windDirection, windSpeed, airQualityLevel, airQualityIndex, reliability, co, no, co2, nox, so2, coLevel,refPointOfInterest):
        self.id = id
        self.type = type
        self.address = address
        self.dateObserved = dateObserved
        self.areaServed = areaServed
        self.location = location
        self.source = source
        self.typeOfLocation = typeOfLocation
        self.precipitation = precipitation
        self.relativeHumidity = relativeHumidity
        self.temperature = temperature
        self.windDirection = windDirection
        self.windSpeed = windSpeed
        self.airQualityLevel = airQualityLevel
        self.airQualityIndex = airQualityIndex
        self.reliability = reliability
        self.co = co
        self.no = no  
        self.co2 = co2
        self.nox = nox
        self.so2 = so2
        self.coLevel = coLevel
        self.refPointOfInterest = refPointOfInterest

    @staticmethod
    def from_json(json_dct):
        model = WeatherSmartDataModel(
            json_dct['id'],
            json_dct['type'], 
            json_dct['address'],
            json_dct['dateObserved'], 
            json_dct['areaServed'],
            json_dct['location'],
            json_dct['source'],
            json_dct['typeOfLocation'],
            json_dct['precipitation'],
            json_dct['relativeHumidity'],
            json_dct['temperature'],
            json_dct['windDirection'],
            json_dct['windSpeed'],
            json_dct['airQualityLevel'],
            json_dct['airQualityIndex'],
            json_dct['reliability'],
            json_dct['co'],
            json_dct['no'],
            json_dct['co2'],
            json_dct['nox'],
            json_dct['so2'],
            json_dct['coLevel'],
            json_dct['sourrefPointOfInterestce'],
            )
        return model




def convert_open_weather(**kwargs):
    headers = {'Content-Type': 'application/json',}
    response = requests.get(OPEN_WEATHER_URL, headers)
    results=json.loads(response.content)
    id = results["sys"]["country"]+"-"+results["name"]+"-AmbientObserverd"+str(datetime.datetime.now())
    r_street = requests.get('https://nominatim.ubiwhere.com/reverse.php', params={'format':'json', 'lat': HELSINKI_LATITUDE,'lon':HELSINKI_LONGITUDE})
    if r_street.status_code ==200:
        try:
            street_data = r_street.json()
            
            address = {  
                "addressCountry": results["sys"]["country"],  
                "addressLocality": results["name"],  
                "streetAddress":  street_data["address"]["cycleway"]
            }
        except:
            address = "problems with nominatim"
    else:
        address = "problems with nominatim"

    location = {  
        "type": "Point",  
        "coordinates": [HELSINKI_LATITUDE, HELSINKI_LONGITUDE]  
    }
    try:
        rain = results["rain"]["1h"]
    except:
        rain = 0

    outputModel=WeatherSmartDataModel(
        id,
        "AirQualityObserved",
        address,
        str(datetime.datetime.now()),
        None,
        location,
        RAW_OPEN_WEATHER_URL,
        "outdoor",
        rain,
        results["main"]["humidity"],
        results["main"]["temp"],
        results["wind"]["deg"],
        results["wind"]["speed"],
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None
    )

    conn = Connection('amqp://{}:{}@{}:{}//'.format(RABBIT_USER, RABBIT_PASSWORD, RABBIT_URL, RABBIT_PORT))
    exchange = Exchange('neanias', 'fanout', durable=True)
    channel = conn.channel()
    producer = Producer(channel, exchange, serializer='json')

    msg = json.dumps(outputModel.__dict__)

    producer.publish(msg, routing_key=RABBIT_QUEUE)

    log.info("Message sent to consumer {}".format(RABBIT_URL_NEANIAS))




args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(2),
    'email': [
        'jmgarcia@ubiwhere.com',
        'rvitorino@ubiwhere.com',
        'fgsantos@ubiwhere.com',
        'jmarques@ubiwhere.com'
    ],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(DAG_NAME, description='Convert OWM to SmartDataModel',
          schedule_interval=SDL_SDM_WEATHER,
          start_date=datetime.datetime(2022, 6, 24),
          catchup=False) 


convert_open_weather = PythonOperator(
    task_id='import_data_open_weather',
    python_callable=convert_open_weather,
    provide_context=True, dag=dag
)

convert_open_weather