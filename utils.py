import json
import logging as log
import sys

from kombu import Connection
from kombu import Exchange
from kombu import Producer
import psycopg2
import requests
import xmltodict


# Get Street name using Nominatim
def get_street_name(latitude, longitude, url):
    response = requests.get(
        '{}/reverse.php?lat={}&lon={}&zoom=100'.format(url, latitude, longitude),
        verify=False
    )

    if response.status_code is 200:
        response = json.loads(json.dumps(xmltodict.parse(response.text)))

        street_name = response.get('reversegeocode').get('result').get('#text')
        values = response.get('reversegeocode').get('addressparts')

        if values.get('suburb'):
            street_name = values.get('suburb')
        if values.get('neighbourhood'):
            street_name = values.get('neighbourhood')
        if values.get('road'):
            street_name = values.get('road')

        return street_name

    else:
        return "Street name not available"


def connect_to_rabbit(user, password, url, port, fanout, exchange):
    """ Returns a Celery Worker to write to a given RabbitMQ exchange """
    conn = Connection('amqp://{}:{}@{}:{}//'.format(user, password, url, port))
    exchange = Exchange(exchange, fanout, durable=True)
    channel = conn.channel()
    producer = Producer(channel, exchange, serializer='json')
    return producer


def connect_to_db(user, password, host, port, database):
    """ Returns a PostgreSQL connection """
    try:
        connection = psycopg2.connect(user=user, password=password, host=host, port=port, database=database)
        cursor = connection.cursor()
        print(connection.get_dsn_parameters(), "\n")
        cursor.execute("SELECT version();")
        record = cursor.fetchone()
        print("You are connected to - ", record, "\n")
        return connection, cursor
    except (Exception, psycopg2.Error) as error:
        print("Error while connecting to PostgreSQL", error)


def disconnect_from_db(connection, cursor):
    cursor.close()
    connection.close()
    print("PostgreSQL connection is closed")
