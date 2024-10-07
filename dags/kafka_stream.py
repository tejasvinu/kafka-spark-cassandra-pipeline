import time
import random
import json
import numpy as np
from datetime import datetime, timedelta
from faker import Faker
from kafka import KafkaProducer
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging


default_args = {
    "owner": "debanjan",
    "start_date": datetime(2024, 10, 4, 10, 00),
    "email": ["debanjansh@gmail.com"],
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(seconds=30),
    "retry_exponential_backoff": True,
    "schedule_interval": "@hourly",
}

fake = Faker()
logging.basicConfig(level=logging.INFO)

# Kafka producer setup
AIRFLOW_ORCHESTRATOR = True  # Determine orchestration provider
records_per_second = 100  # Pass records per second dynamically
run_for_seconds = 600  # Set runtime dynamically (e.g., 1 hour)
KAFKA_HOSTS = [
    "localhost:9092",
    "localhost:9093",
    "localhost:9094",
    "broker:9092",
    "broker:29092",
]


def generate_weather_data():
    temperature = np.random.normal(loc=15, scale=10)
    humidity = np.random.normal(loc=60, scale=20)
    pressure = np.random.normal(loc=1013, scale=10)
    wind_speed = np.random.exponential(scale=5)
    wind_direction = random.choice(["N", "NE", "E", "SE", "S", "SW", "W", "NW"])

    # Clamp values to realistic ranges
    temperature = max(min(temperature, 40), -20)
    humidity = max(min(humidity, 100), 0)
    pressure = max(min(pressure, 1050), 950)
    wind_speed = round(min(wind_speed, 100), 1)

    return {
        "station_id": fake.uuid4(),
        "timestamp": fake.iso8601(),
        "location": {
            "city": fake.city(),
            "country": fake.country(),
            "latitude": float(fake.latitude()),
            "longitude": float(fake.longitude()),
        },
        "temperature": round(temperature, 1),
        "humidity": int(humidity),
        "pressure": round(pressure, 1),
        "wind_speed": wind_speed,
        "wind_direction": wind_direction,
        "precipitation": round(random.uniform(0, 50), 1),
        "weather_condition": random.choice(
            ["Sunny", "Cloudy", "Rainy", "Snowy", "Stormy"]
        ),
    }


def create_kafka_producer():
    return KafkaProducer(
        bootstrap_servers=KAFKA_HOSTS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        max_block_ms=5000,
        linger_ms=100,
        retries=5,
        acks="all",
    )
    # return KafkaProducer(
    #     bootstrap_servers=KAFKA_HOSTS,
    #     value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    #     max_block_ms=5000,
    #     batch_size=32768,
    #     linger_ms=100,
    #     compression_type="snappy",
    #     buffer_memory=67108864,  # 64MB
    #     max_in_flight_requests_per_connection=5,
    #     retries=5,
    #     acks=1,
    # )


def stream_weather_data(records_per_second, run_for_seconds):
    """Stream nested weather data to Kafka."""
    producer = None
    try:
        producer = create_kafka_producer()
        logging.info(f"Started streaming weather data to Kafka topic 'weather_data'")

        start_time = time.time()
        while True:
            if run_for_seconds and time.time() - start_time >= run_for_seconds:
                break

            for _ in range(records_per_second):
                weather_data = generate_weather_data()
                future = producer.send("weather_data", value=weather_data)
                future.get(timeout=10)

            time.sleep(1)

        logging.info(f"Successfully completed data streaming to Kafka.")

    except Exception as e:
        logging.error(f"Error occurred while streaming data to Kafka: {e}")
        raise
    finally:
        if producer:
            producer.flush()  # Flush any remaining messages
            producer.close(timeout=60)
            logging.info("Kafka producer closed.")


def run_spark_stream():
    from spark_stream import start_spark_stream

    start_spark_stream()


if AIRFLOW_ORCHESTRATOR:
    # Airflow DAG definition
    with DAG(
        "weather_data_streaming",
        default_args=default_args,
        schedule_interval=timedelta(minutes=30),
        catchup=False,
        max_active_runs=1,
    ) as dag:

        # Kafka streaming task
        kafka_streaming_task = PythonOperator(
            task_id="stream_weather_data_kafka",
            python_callable=stream_weather_data,
            op_kwargs={
                "records_per_second": records_per_second,
                "run_for_seconds": run_for_seconds,
            },
            execution_timeout=timedelta(minutes=15),
        )

        # Spark streaming task
        spark_streaming_task = PythonOperator(
            task_id="process_data_with_spark",
            python_callable=run_spark_stream,
            execution_timeout=timedelta(minutes=30),
        )

        # Set task dependencies
        kafka_streaming_task >> spark_streaming_task

else:
    stream_weather_data(records_per_second, run_for_seconds)
