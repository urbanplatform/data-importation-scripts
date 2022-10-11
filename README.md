# Data importation scripts for the Urban Platform (Airflow DAGs)

Data can be integrated into the Urban Platform using Python code, orchestrated using [Apache Airflow](https://airflow.apache.org/), an open-source platform to programmatically author, schedule and monitor workflows.

Airflow's workflows are known as **DAGs** (i.e. [Directed Acyclic Graphs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html)), which are sets of tasks organized with dependencies and relationships to say how they should run.

This repository provides some real-life examples of tasks and DAGs for the integration of different datasets into the Urban Platform.

## How tasks work

Generally, tasks should perform three jobs:
1. Gather data from a remote location (API, Database, Files, etc.);
2. Transform such data to comply with a given (preferably [standard/normalised](https://smartdatamodels.org/)) data model;
3. Make the (harmonised) data available to the Urban Platform via the [AMQP Protocol](https://www.amqp.org/) to ensure horizontal scalability in the data ingestion pipelines.

### Gathering data

Data can be found in different types of platforms using many different technologies. If it is available via an API, the Airflow DAG can use the `requests` Python library to perform the API call and retrieve the data. 

### Transforming data

The data gathered should be transformed to conform to data models that can be consumed/stored by the Platform. The Urban Platform complies with data models specified by the [Smart Data Models initiative](https://smartdatamodels.org/) wherever possible. This project provides DAGs that automatically transform external data into these harmonised data models.

### Making data available

The Data processed in the previous steps can be made available using the integrated AMQP connector, which leverages [RabbitMQ](https://www.rabbitmq.com/). To do this, you will need to update the `contants.py` file with the RabbitMQ variables (host, port, credentials and name of the queue):

```python
FANOUT = # Queue name
RABBIT_URL = # RabbitMQ host URL
RABBIT_PORT = # RabbitMQ host port (default 5672)
RABBIT_USER = # RabbitMQ User
RABBIT_PASSWORD = # ...
```

Each datapoint generates a message into the RabbitMQ Exchange, making it available for consumption by asynchronous workers subscribed to the RabbitMQ message queue(s).

### Maintenance DAGS

Two Airflow Maintenance DAGs are currently available to perform cleanups to Airflow's logs and database. These DAGs have been adapted from [an open-source repository](https://github.com/teamclairvoyant/airflow-maintenance-dags/) to work in the Airflow instances used in the Urban Platform's deployments.

(Major kudos to [team Clairvoyant](https://github.com/teamclairvoyant) :clap:)

In order to integrate them, simply import them into the `dags` folder and enable them, they will clean up the tool's logs and database in a daily fashion.

However, to actually reclaim storage from the database, a `vacuum full` must be performed. Please be aware that if the space is not being properly reclaimed, that means the vacuum cannot be performed on your database and the DAG is only creating zombie files.

## How to contribute

Contributions are welcome! Here are some ways you can contribute:
* Add new data sources (at the moment, there is HERE, OpenWeatherMaps and TriMet)
* Add new southbound connectors (besides REST API calls)
* Suggest new data models to include and develop data converters for them
* Develop new northbound connectors (besides AMQP/RabbitMQ)

[Join us on Slack](https://urbanplatform.slack.com/) if you wish to discuss developments or need any help to get started.

We would love your feedback on how to improve the contribution experience!


## Frequently Asked Questions

TBD
