import os

FANOUT = 'fanout'
RABBIT_URL = 'rabbitmq'
RABBIT_USER = 'guest'
RABBIT_PASSWORD = 'guest'
RABBIT_PORT = 5672

TRAFFIC_FLOW_BASE_URL = 'https://traffic.api.here.com/traffic/6.2/flow.json/'
TRAFFIC_FLOW_APP_ID = os.environ.get('TRAFFIC_FLOW_APP_ID')
TRAFFIC_FLOW_APP_CODE = os.environ.get('TRAFFIC_FLOW_APP_CODE')
TRAFFIC_FLOW_BBOX = '45.523378,-122.698528;45.484875,-122.623128'

TRAFFIC_FLOW_RESP_ATTRS = 'sh,fc'
TRAFFIC_FLOW_UNITS = 'metric'
TRAFFIC_FLOW_TS = 'true'

TRIMET_ALERTS_API_KEY = '5EC1222618116DAB188A7C370'
TRIMET_ALERTS_URL = 'https://developer.trimet.org/ws/v2/alerts/?appID={}'.format(TRIMET_ALERTS_API_KEY)

EMAILS = [os.environ.get('EMAILS').split(',')]