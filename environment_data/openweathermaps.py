
import datetime
import json
import logging as log
import copy
import requests

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from constants import EMAILS

try:
    from ..constants import EMAILS, OPEN_WEATHER_API_KEY, RAW_OPEN_WEATHER_URL
    from ..constants import FANOUT, RABBIT_URL, RABBIT_USER, RABBIT_PASSWORD, RABBIT_PORT
    from ..utils import connect_to_rabbit, get_street_name
except ImportError:
    from dags.constants import OPEN_WEATHER_API_KEY, RAW_OPEN_WEATHER_URL
    from dags.constants import RABBIT_URL
    from dags.utils import connect_to_rabbit, get_street_name

# DAG Variables
DAG_NAME = 'OpenWeatherMap2SmartDataModel'
SDL_SDM_WEATHER = '0 */1 * * *'  # At minute 0 past every hour

HELSINKI_LATITUDE = 60.1699 
HELSINKI_LONGITUDE = 24.9384
OPEN_WEATHER_URL = '{}/weather?lat={}&lon={}&appid={}'.format(
    RAW_OPEN_WEATHER_URL, HELSINKI_LATITUDE, HELSINKI_LONGITUDE, OPEN_WEATHER_API_KEY
)

# Other RabbitMQ Variables imported from constants
RABBIT_QUEUE = "openweather_queue"
RABBIT_EXCHANGE = "openweather_exchange"


class WeatherSmartDataModel:
    """ An observation of weather conditions at a certain place and time. This data model has been developed in cooperation with mobile operators and the GSMA. """
    def __init__(self, id, type, address, dateObserved, areaServed, location, source, precipitation, relativeHumidity, temperature, windDirection, windSpeed, airQualityLevel, airQualityIndex, reliability, co, no, co2, nox, so2, coLevel,refPointOfInterest):
        self.id = id
        self.type = type
        self.address = address
        self.dateObserved = dateObserved
        self.areaServed = areaServed
        self.location = location
        self.source = source
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
            json_dct['refPointOfInterest'],
        )
        return model


def convert_open_weather(**kwargs):
    headers = {'Content-Type': 'application/json',}
    response = requests.get(OPEN_WEATHER_URL, headers)
    results = json.loads(response.content)
    id = results["sys"]["country"]+"-"+results["name"]+"-AmbientObserved"+str(datetime.datetime.now())
    # TODO use Nominatim (or similar to translate coordinates to address) using get_street_name()
    address = "Mannerheimintie"

    location = {  
        "type": "Point",  
        "coordinates": [HELSINKI_LATITUDE, HELSINKI_LONGITUDE]  
    }
    try:
        rain = results["rain"]["1h"]
    except:
        rain = 0

    outputModel=WeatherSmartDataModel(
        id=id,
        type="AirQualityObserved",
        address=address,
        dateObserved=str(datetime.datetime.now()),
        areaServed=None,
        location=location,
        source=RAW_OPEN_WEATHER_URL,
        precipitation=rain,
        relativeHumidity=results["main"]["humidity"],
        temperature=results["main"]["temp"],
        windDirection=results["wind"]["deg"],
        windSpeed=results["wind"]["speed"],
        airQualityLevel=None,
        airQualityIndex=None,
        reliability=None,
        co=None,
        no=None,
        co2=None,
        nox=None,
        so2=None,
        coLevel=None,
        refPointOfInterest=None
    )

    producer = connect_to_rabbit(RABBIT_USER, RABBIT_PASSWORD, RABBIT_URL, RABBIT_PORT, FANOUT, RABBIT_EXCHANGE)
    msg = json.dumps(outputModel.__dict__)
    producer.publish(msg, routing_key=RABBIT_QUEUE)
    log.info("Message sent to consumer {}".format(RABBIT_URL))


args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(2),
    'email': [
        EMAILS
    ],
    'email_on_failure': True,
    'email_on_retry': False,
}

dag = DAG(DAG_NAME, description='Convert OpenWeatherMap to SmartDataModel WeatherObserved',
          schedule_interval=SDL_SDM_WEATHER,
          start_date=datetime.datetime(2022, 6, 24),
          catchup=False) 


convert_open_weather = PythonOperator(
    task_id='import_data_open_weather',
    python_callable=convert_open_weather,
    provide_context=True, dag=dag
)

convert_open_weather