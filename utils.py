import json
import logging as log
import sys

import psycopg2
import requests
import xmltodict
from kombu import Connection
from kombu import Exchange
from kombu import Producer


def make_request_xml(url, city):
    info = 'Get {} traffic devices'.format(city)
    try:
        response = requests.get(url)
    except requests.exceptions.HTTPError as error:
        log.info("[ERROR] Request -> {}: {}".format(info, error))
        sys.exit(1)

    if response.status_code is 200:
        r = response.text
        xml_to_dict = xmltodict.parse(r)
        return json.loads(json.dumps(xml_to_dict))
    else:
        log.info("[ERROR] Request -> {}. Status Code: {}".format(info, response.status_code))
        sys.exit(1)


def make_request_txt(url, city):
    info = 'Get {} traffic devices'.format(city)

    try:
        response = requests.get(url)
    except requests.exceptions.HTTPError as error:
        log.info("[ERROR] Request -> {}: {}".format(info, error))
        sys.exit(1)

    if response.status_code is 200:
        return json.loads(response.text)
    else:
        log.info("[ERROR] Request -> {}. Status Code: {}".format(info, response.status_code))
        sys.exit(1)


def get_source(identifier):
    response = requests.get(
        'https://data.embers.city/private-sources/?identifier={}'.format(identifier)
    )
    if response.status_code is not 200:
        log.info("It was not possible to get the data URL - Status Code {}\n".format(response.status_code))
        return None

    response = json.loads(response.text)

    if response.get('count') is 1:
        if len(response.get('results')[0].get('data_source')) is 1:
            return response.get('results')[0].get('data_source')[0].get('data_url'), \
                   int(response.get('results')[0].get('id'))
        else:
            return response.get('results')[0].get('data_source'), int(response.get('results')[0].get('id'))
    else:
        log.info("Data URL with identifier {} not found\n".format(identifier))
        return None, None


# Get Street name using nominatim
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
    conn = Connection('amqp://{}:{}@{}:{}//'.format(user, password, url, port))
    exchange = Exchange(exchange, fanout, durable=True)
    channel = conn.channel()
    producer = Producer(channel, exchange, serializer='json')

    return producer


def connect_to_db(user, password, host, port, database):
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
