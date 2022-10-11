#!/usr/bin/env python
# coding: utf-8

import datetime
import logging as log

import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils import dates
from kombu import Connection
from kombu import Exchange
from kombu import Producer

from ..constants import EMAILS
from ..constants import FANOUT
from ..constants import RABBIT_PASSWORD
from ..constants import RABBIT_PORT
from ..constants import RABBIT_URL
from ..constants import RABBIT_USER
from ..constants import TRIMET_ALERTS_URL
from ..schedules import SDL_ALERTS_IMPORTER

HEADERS = {
    'Content-Type': 'application/json',
}

all_events = []


def connect_to_rabbit():
    conn = Connection('amqp://{}:{}@{}:{}//'.format(RABBIT_USER, RABBIT_PASSWORD, RABBIT_URL, RABBIT_PORT))
    exchange_name = 'open511_exchange'
    exchange = Exchange(exchange_name, FANOUT, durable=True)
    channel = conn.channel()
    producer = Producer(channel, exchange, serializer='json')

    return producer


def send_to_rabbit(payload, producer):
    producer.publish(payload)
    print("Message sent to rabbitmq!")


def get_events(producer):
    messages = []
    response = requests.get(TRIMET_ALERTS_URL)

    if response.status_code is 200:
        response_json = response.json()['resultSet']['alert']
        log.info('Processing {} incident reports'.format(len(response_json)))
        for item in response_json:
            
            open_date = item.get('begin')
            if open_date:
                open_date = datetime.datetime.fromtimestamp(open_date)
            answered_at = item.get('end')
            if answered_at:
                answered_at = datetime.datetime.fromtimestamp(answered_at)
            categories = ['Example']

            ev_type = 'Alert'

            ev_subtype = 'Route Status'

            image = item.get('image')
            current_status = item.get('state')

            current_status = 'Received' if current_status == 'Inserido' else 'In Resolution' \
                if current_status == 'Em Tratamento' else 'Archived'

            location = item.get('location', None)
            lat = long = None
            if location:
                lat = location.get('lat')
                long = item.get('lng')

            description = item.get('desc')

            message = {
                "vague_id": item.get('id'),  # This id will NOT serve as a unique identifier
                "event_id": item.get('id'),
                "location": {
                    "full_location": item.get('address'),
                    "latitude": lat,
                    "longitude": long,
                },
                "open_date": open_date,
                "close_date": answered_at,
                "description": description,
                "image_url": image,
                "status": current_status,
                "ev_type": ev_type,
                "ev_subtype": ev_subtype,
                "answered_by": item.get('answered_by', None),
                "citizen_submitted": True,
                "additional_data": {
                    "answered": item.get('answered', None),
                }
            }
            messages.append(message)

            if len(messages) == 10:
                d = {
                    "payload": messages
                }
                send_to_rabbit(d, producer)
                messages = []


def get_data(**kwargs):
    producer = connect_to_rabbit()
    log.info('Getting incident events')
    get_events(producer)


args = {
    'owner': 'airflow',
    'start_date': dates.days_ago(2),
    'email': [
        EMAILS,
    ],
    'email_on_failure': True,
    'email_on_retry': False,
}

DAG_NAME = 'trimet_alerts_importer'
dag = DAG(DAG_NAME, description='Guimaraes Incidents Importer',
          schedule_interval=SDL_ALERTS_IMPORTER,
          start_date=datetime.datetime(2019, 8, 28),
          catchup=False)

trimet_alerts_importer = PythonOperator(
    task_id='import_full',
    python_callable=get_data,
    provide_context=True, dag=dag
)
