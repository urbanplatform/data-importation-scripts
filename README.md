# Airflow DAGs for Urban Platform integration

Data integration in the Urban Platform consists on Python tasks, orchestrated using [Apache Airflow](https://airflow.apache.org/).

The core concept of Airflow consists on the [DAGs or Directed Acyclic Graphs](https://airflow.apache.org/docs/apache-airflow/stable/concepts/dags.html), which are sets of tasks organized with dependencies and relationships to say how they should run.

In this repository are examples of tasks and DAG definitions for the integration of different datasets in the Urban Platform.

## How tasks work

Generally, tasks should perform 3 jobs:
* Gather data from a remote location (API, Database, etc.);
* Transform that data to conform with a given (preferably [standard](https://smartdatamodels.org/)) data model;
* Make that data available to the Urban Platform using the [AMQP Protocol](https://www.amqp.org/).

### Gathering data

Data can be found in different types of platforms using many different technologies. 

### Transforming data

Gathered data can be transformed in order to conform to a data model that can be consumed by the Platform. The Platform uses the data models specified by the [Smart Data Models initiative](https://smartdatamodels.org/index.php/ddbb-of-properties-descriptions/) wherever possible. This project will provide tools to automatically transform incoming data to these data models.

### Making data available

Data made available through the previous steps can be made available using the integrated AMQP connector, which uses [RabbitMQ](https://www.rabbitmq.com/). To do this, the RabbitMQ variables must be edited in the `contants.py` file:

```
FANOUT = # Queue name
RABBIT_URL = # RabbitMQ host URL
RABBIT_PORT = # RabbitMQ host port (default 5672)
RABBIT_USER = # RabbitMQ User
RABBIT_PASSWORD = 
```

For each datapoint, a message will be generated in the RabbitMQ Exchange, making it available for consumption

### Maintenance DAGS

In order to use the maintenance DAGS simply add them to the dags and leave them enabled, they will perform daily cleanups to the logs and database.
To actually reclaim storage from the database a vacuum full must be performed, if space is not being reclaimed it may be beacuase the vacuum can't be properly performed on your database and the DB cleanup DAG is now only creating zombie files.

## How to contribute

Contributions are welcome! Here are some ways you can contribute:
* Add new data sources to the project
* Add new southbound connectors
* Suggest new data models to include and develop data converters for them
* Develop new northbound connectors

Join us on Slack if you wish to discuss development or need help to get started.

We would love your feedback on how to improve the contribution experience!


## Frequently Asked Questions

TBD
