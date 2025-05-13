#!/usr/bin/env python3

import time
import subprocess
import requests
import logging
from kafka import KafkaProducer

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
PROMETHEUS_EXPORTERS = {
    'ipmi_exporter': 'http://10.180.8.24:9290/metrics',
    'node_exporter': 'http://10.180.8.24:9100/metrics',
    'dcgm_exporter': 'http://10.180.8.24:9400/metrics',
    'slurm_exporter': 'http://10.180.8.24:8080/metrics', # Assuming /metrics endpoint for slurm
}

KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092' # As confirmed from docker-compose
KAFKA_TOPIC_PREFIX = 'prometheus_metrics'
SPARK_SCRIPT_PATH = "./spark_stream.py" # Assuming it's in the same directory
PIPELINE_INTERVAL_SECONDS = 5

# --- Prometheus and Kafka Functions ---
def fetch_prometheus_metrics(exporter_name: str, endpoint_url: str):
    """Fetches metrics from a Prometheus exporter."""
    try:
        response = requests.get(endpoint_url, timeout=10)
        response.raise_for_status()
        logging.info(f"Successfully fetched data from {exporter_name} at {endpoint_url}")
        return response.text
    except requests.exceptions.RequestException as e:
        logging.error(f"Error fetching data from {exporter_name} at {endpoint_url}: {e}")
        return None

def send_to_kafka(metrics_data: str, kafka_topic: str, producer: KafkaProducer):
    """Sends metrics data to a Kafka topic."""
    if not metrics_data:
        logging.warning(f"No metrics data to send for topic {kafka_topic}")
        return
    try:
        producer.send(kafka_topic, value=metrics_data)
        logging.info(f"Sent data to Kafka topic {kafka_topic}")
    except Exception as e:
        logging.error(f"Error sending data to Kafka topic {kafka_topic}: {e}")

# --- Spark Job Function ---
def run_spark_job():
    """Runs the Spark stream processing job."""
    logging.info(f"Starting Spark job: {SPARK_SCRIPT_PATH}")
    try:
        # Added kafka-clients to the --packages list
        # Ensure there are NO SPACES in the comma-separated package list
        spark_submit_command = [
            'spark-submit',
            '--packages',
            'org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.apache.spark:spark-streaming-kafka-0-10_2.12:3.4.3,org.apache.kafka:kafka-clients:3.4.1,com.datastax.spark:spark-cassandra-connector_2.12:3.4.0',
            SPARK_SCRIPT_PATH
        ]
        logging.info(f"Executing Spark command: {' '.join(spark_submit_command)}")
        process = subprocess.run(
            spark_submit_command,
            capture_output=True,
            text=True,
            check=True  # Raises CalledProcessError for non-zero exit codes
        )
        logging.info("Spark job completed successfully.")
        logging.info(f"Spark job stdout:\n{process.stdout}")
        if process.stderr:
            logging.info(f"Spark job stderr:\n{process.stderr}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Spark job failed with exit code {e.returncode}.")
        logging.error(f"Spark job stdout:\n{e.stdout}")
        logging.error(f"Spark job stderr:\n{e.stderr}")
    except FileNotFoundError:
        logging.error(
            f"Error: spark-submit command not found. "
            f"Ensure Spark is installed and spark-submit is in your PATH."
        )
    except Exception as e:
        logging.error(f"An unexpected error occurred while running Spark job: {e}")

# --- Main Orchestration Loop ---
def main():
    logging.info("Starting pipeline orchestration...")
    
    kafka_producer = None
    try:
        kafka_producer = KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: str(v).encode('utf-8'),
            retries=5, # Retry sending messages
            linger_ms=100 # Batch messages for 100ms
        )
        logging.info(f"KafkaProducer connected to {KAFKA_BOOTSTRAP_SERVERS}")
    except Exception as e:
        logging.error(f"Failed to initialize KafkaProducer: {e}. Exiting.")
        return

    try:
        while True:
            logging.info("--- Starting new pipeline cycle ---")
            
            # 1. Fetch from Prometheus and produce to Kafka
            for exporter_name, endpoint_url in PROMETHEUS_EXPORTERS.items():
                kafka_topic_name = f"{KAFKA_TOPIC_PREFIX}_{exporter_name}"
                metrics = fetch_prometheus_metrics(exporter_name, endpoint_url)
                if metrics:
                    send_to_kafka(metrics, kafka_topic_name, kafka_producer)
            
            # Ensure all messages are sent before running Spark job
            try:
                kafka_producer.flush(timeout=30) # Wait up to 30s for messages to be sent
                logging.info("Kafka messages flushed.")
            except Exception as e:
                logging.error(f"Error flushing Kafka messages: {e}")

            # 2. Run Spark job
            run_spark_job()
            
            logging.info(f"--- Cycle finished. Waiting for {PIPELINE_INTERVAL_SECONDS} seconds ---")
            time.sleep(PIPELINE_INTERVAL_SECONDS)
            
    except KeyboardInterrupt:
        logging.info("Orchestration stopped by user.")
    except Exception as e:
        logging.error(f"Unhandled exception in main loop: {e}", exc_info=True)
    finally:
        if kafka_producer:
            logging.info("Closing Kafka producer.")
            kafka_producer.close()
        logging.info("Pipeline orchestration finished.")

if __name__ == "__main__":
    main()