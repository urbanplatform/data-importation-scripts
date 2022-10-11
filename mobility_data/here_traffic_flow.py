#!/usr/bin/env python
# coding: utf-8

import json
from datetime import datetime

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from kombu import Connection
from kombu import Exchange
from kombu import Producer

from ..constants import EMAILS, RABBIT_PASSWORD, FANOUT
from ..constants import RABBIT_PORT
from ..constants import RABBIT_URL
from ..constants import RABBIT_USER
from ..constants import TRAFFIC_FLOW_BASE_URL, TRAFFIC_FLOW_APP_ID, TRAFFIC_FLOW_APP_CODE, \
    TRAFFIC_FLOW_BBOX, TRAFFIC_FLOW_RESP_ATTRS, TRAFFIC_FLOW_UNITS, TRAFFIC_FLOW_TS
from ..schedules import SDL_HERE_TRAFFIC_FLOW

TRAFFIC_FLOW_BACKEND_TOKEN = None

HEADERS = {
    'Content-Type': 'application/json'
}


def connect_to_rabbit():
    conn = Connection('amqp://{}:{}@{}:{}//'.format(str(RABBIT_USER), str(RABBIT_PASSWORD), RABBIT_URL, RABBIT_PORT))
    exchange_name = 'traffic_exchange'
    exchange = Exchange(exchange_name, FANOUT, durable=True)
    channel = conn.channel()
    producer = Producer(channel, exchange, serializer='json')

    return producer


def send_to_rabbit(event, producer):
    event['schema_name'] = 'public'
    body = {
        'payload': event
    }
    producer.publish(body, expiration=60 * 5)
    print("Message sent to rabbitmq!")


def parse_data(producer):
    url = '{}?app_id={}&app_code={}&bbox={}&responseattributes={}&units={}&ts={}'.format(
        TRAFFIC_FLOW_BASE_URL, TRAFFIC_FLOW_APP_ID, TRAFFIC_FLOW_APP_CODE, TRAFFIC_FLOW_BBOX,
        TRAFFIC_FLOW_RESP_ATTRS, TRAFFIC_FLOW_UNITS, TRAFFIC_FLOW_TS
    )

    response = requests.get(url)

    if response.status_code != 200:
        print("Failed to get traffic flow data for {} with status code {}\nMesssage: {}".format(
            TRAFFIC_FLOW_BBOX, response.status_code, response.text)
        )

    else:
        response = json.loads(response.text)

        send_to_rabbit(response, producer)


def get_data(**kwargs):
    producer = connect_to_rabbit()
    parse_data(producer)


args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(2),
    'email': EMAILS,
    'email_on_failure': True,
    'email_on_retry': False,
}
DAG_NAME = 'here_tech_traffic_importer'
dag = DAG(DAG_NAME, description='Here Technologies Traffic Flow Importer',
          schedule_interval=SDL_HERE_TRAFFIC_FLOW,
          start_date=datetime(2021, 7, 1),
          catchup=False)

here_tech_traffic = PythonOperator(
    task_id='import_full',
    python_callable=get_data,
    provide_context=True, dag=dag
)
