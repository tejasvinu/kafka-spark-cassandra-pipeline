from __future__ import annotations

import pendulum
import datetime

from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
# We'll need libraries to interact with Prometheus and Kafka
# For Prometheus, 'requests' is common.
# For Kafka, 'kafka-python' is a popular choice.

# Define the Prometheus exporters and their targets
PROMETHEUS_EXPORTERS = {
    'ipmi_exporter': 'http://10.180.8.24:9290/metrics',
    'node_exporter': 'http://10.180.8.24:9100/metrics',
    'dcgm_exporter': 'http://10.180.8.24:9400/metrics',
    'slurm_exporter': 'http://10.180.8.24:8080/metrics', # Assuming /metrics endpoint
}

KAFKA_BOOTSTRAP_SERVERS = 'broker:29092'
KAFKA_TOPIC_PREFIX = 'prometheus_metrics'

# Define the path to spark_stream.py relative to the DAGs folder
# Assuming spark_stream.py is in the parent directory of dags/
SPARK_SCRIPT_PATH = "/opt/airflow/dags/../spark_stream.py"

DEFAULT_ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(seconds=15),
}

def fetch_prometheus_metrics(exporter_name: str, endpoint_url: str):
    """
    Fetches metrics from a Prometheus exporter.
    """
    import requests
    try:
        response = requests.get(endpoint_url, timeout=10)
        response.raise_for_status()  # Raise an exception for HTTP errors
        print(f"Successfully fetched data from {exporter_name} at {endpoint_url}")
        # In a real scenario, you'd parse and process the metrics.
        # For now, we'll just return the raw text.
        return response.text
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from {exporter_name} at {endpoint_url}: {e}")
        # Optionally, re-raise or handle as per your error strategy
        raise

def stream_to_kafka(metrics_data: str, kafka_topic: str):
    """
    Streams the fetched metrics data to a Kafka topic.
    """
    from kafka import KafkaProducer
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: str(v).encode('utf-8') # Send data as UTF-8 encoded strings
        )
        print(f"Sending data to Kafka topic {kafka_topic}...")
        producer.send(kafka_topic, value=metrics_data)
        producer.flush() # Ensure all messages are sent
        producer.close()
        print(f"Successfully sent data to Kafka topic {kafka_topic}")
    except Exception as e:
        print(f"Error sending data to Kafka topic {kafka_topic}: {e}")
        # Optionally, re-raise or handle
        raise

with DAG(
    dag_id="prometheus_to_kafka_and_cassandra",
    start_date=pendulum.datetime(2023, 10, 26, tz="UTC"),
    catchup=False,
    schedule=datetime.timedelta(seconds=5),
    tags=["prometheus", "kafka", "spark", "cassandra"],
    default_args=DEFAULT_ARGS,
    max_active_runs=1,
) as dag:
    
    all_kafka_stream_tasks = []

    for exporter_name, endpoint_url in PROMETHEUS_EXPORTERS.items():
        # Define a unique Kafka topic for each exporter
        kafka_topic_name = f"{KAFKA_TOPIC_PREFIX}_{exporter_name}"

        fetch_task = PythonOperator(
            task_id=f"fetch_{exporter_name}_metrics",
            python_callable=fetch_prometheus_metrics,
            op_kwargs={'exporter_name': exporter_name, 'endpoint_url': endpoint_url},
            execution_timeout=datetime.timedelta(seconds=60), # Timeout after 60 seconds
        )

        stream_task = PythonOperator(
            task_id=f"stream_{exporter_name}_to_kafka",
            python_callable=stream_to_kafka,
            op_kwargs={'metrics_data': fetch_task.output, 'kafka_topic': kafka_topic_name},
            execution_timeout=datetime.timedelta(seconds=60), # Timeout after 60 seconds
        )

        fetch_task >> stream_task
        all_kafka_stream_tasks.append(stream_task)

# Note: The 'requests' and 'kafka-python' libraries need to be installed 
# in your Airflow environment. You should add them to your requirements.txt
# and rebuild your Airflow image if necessary.
# Add:
# requests
# kafka-python 