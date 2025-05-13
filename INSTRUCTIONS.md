# Prometheus to Cassandra Data Pipeline Instructions

This document provides instructions on how to set up and run the data pipeline that fetches metrics from Prometheus exporters, streams them through Kafka, processes them with Spark, and stores them in Cassandra.

## Prerequisites

*   Docker and Docker Compose installed.
*   Access to a terminal or command prompt.
*   The project codebase cloned to your local machine.

## Directory Structure Overview

*   `assets/`: (If any static assets are used)
*   `dags/`: Contains Airflow DAG definitions (e.g., `prometheus_to_kafka_dag.py`).
*   `script/`: Contains helper scripts (e.g., Airflow entrypoint).
*   `spark_stream.py`: The Spark Streaming application.
*   `docker-compose.yml`: Defines all the services (Kafka, Zookeeper, Spark, Cassandra, Airflow, etc.).
*   `prometheus.yml`: Prometheus configuration for scraping targets.
*   `requirements.txt`: Python dependencies for Airflow.

## Data Flow

1.  **Prometheus Exporters**: Continuously expose metrics.
2.  **Airflow DAG (`prometheus_to_kafka_and_cassandra`):**
    *   Runs on a defined schedule (e.g., every 5 seconds).
    *   **Part 1:** Fetches metrics from Prometheus exporters and sends them to Kafka topics.
    *   **Part 2:** Triggers a Spark batch job (`spark_stream.py`).
3.  **Kafka (`broker:29092`):** Acts as a message broker.
4.  **Spark Batch Job (`spark_stream.py`):**
    *   Executed by Airflow via `spark-submit` in each DAG run.
    *   Connects to Kafka and reads available data from `prometheus_metrics_*` topics.
    *   Parses the Prometheus text format.
    *   Transforms the data.
    *   Writes the processed data to Cassandra.
    *   Exits upon completion.
5.  **Cassandra (`cassandra:9042`):** Stores the final metrics data.

## Setup and Running the Pipeline

### 1. Start Docker Services

Navigate to the root directory of the project in your terminal and run:

```bash
docker-compose up -d
```

This command will build the necessary images (if not already built) and start all services defined in `docker-compose.yml` in detached mode. This includes:
*   Zookeeper
*   Kafka (Broker)
*   Schema Registry (though not actively used by this specific pipeline version)
*   Control Center (Confluent Control Center for Kafka monitoring)
*   Airflow Webserver & Scheduler
*   Postgres (Airflow metadata database)
*   Spark Master & Worker(s)
*   Cassandra
*   Prometheus (configured by `prometheus.yml`)

To check the status of your containers:
```bash
docker-compose ps
```

To view logs for a specific service (e.g., Airflow scheduler):
```bash
docker-compose logs -f scheduler
```

### 2. Access Airflow UI

*   Open your web browser and go to: `http://localhost:8080`
*   The default Airflow login is typically `airflow` / `airflow` (this might vary based on specific Airflow image configurations if customized).
*   Locate the DAG named `prometheus_to_kafka_and_cassandra`.
*   Ensure the DAG is **unpaused**. It will then run on its schedule.
*   The DAG will now handle both fetching data to Kafka AND submitting the Spark batch job to process that data and store it in Cassandra.

*(Note on `spark_stream.py` location for Airflow: The `BashOperator` in the DAG assumes it can find `spark_stream.py` at `/opt/airflow/dags/../spark_stream.py`. This path implies `spark_stream.py` is in the project root, and the DAG is in the `dags/` subdirectory. Ensure your Airflow worker\'s environment and volume mounts allow `spark-submit` to access this script. If you encounter issues, you might need to adjust the `SPARK_SCRIPT_PATH` in the DAG or ensure `spark_stream.py` is copied to a location accessible by `spark-submit` within the Airflow worker container, e.g., by adding a `COPY` command to your Airflow Dockerfile or adjusting volume mounts.)*

### 3. Run the Spark Streaming Application (No Longer Manual)

**The Spark job (`spark_stream.py`) is now automatically triggered by the Airflow DAG (`prometheus_to_kafka_and_cassandra`) as a batch process in each DAG run. You do not need to run `spark-submit` manually for regular operation.**

You can monitor the Spark job execution through the Airflow UI (logs of the `process_with_spark_and_store_in_cassandra` task) and the Spark Master UI (usually accessible at `http://localhost:9090`, check your `docker-compose.yml` for the Spark Master UI port mapping).

### 4. Verify Data in Cassandra

1.  You can connect to the Cassandra container to use `cqlsh` (Cassandra Query Language Shell).
    1.  Find your Cassandra container name: `docker-compose ps` (should be `cassandra`).
    2.  Exec into it:
        ```bash
        docker exec -it cassandra bash
        ```
    3.  Once inside, start `cqlsh`:
        ```bash
        cqlsh
        ```
        (If authentication is enabled as per `docker-compose.yml` - CASSANDRA_USERNAME=cassandra, CASSANDRA_PASSWORD=cassandra - you might need `cqlsh -u cassandra -p cassandra`)
    4.  Query your data:
        ```sql
        USE spark_streams;
        SELECT * FROM prometheus_metrics LIMIT 10;
        ```
        You should see data appearing in the table as it's processed.

## Stopping the Pipeline

1.  **Pause the Airflow DAG:** In the Airflow UI, toggle the `prometheus_to_kafka_and_cassandra` DAG to "paused". This will stop new pipeline runs.
2.  To stop all Docker services:
    ```bash
    docker-compose down
    ```
    To stop and remove volumes (useful for a clean restart, **will delete Kafka data, Cassandra data, etc.**):
    ```bash
    docker-compose down -v
    ```

## Troubleshooting Tips

*   **Check Service Logs:** Use `docker-compose logs -f <service_name>` (e.g., `scheduler`, `broker`, `spark-master`, `cassandra`) to see detailed logs.
*   **Network Issues:** Ensure all containers are on the `confluent` network as defined in `docker-compose.yml`. Spark, Kafka, and Cassandra need to be able to reach each other by their service names.
*   **Airflow DAG Errors:** Check the Airflow UI for task failures and logs.
*   **Spark Job Failures:** Examine the Airflow task logs for `process_with_spark_and_store_in_cassandra`. Also check the Spark Master/Worker UIs and logs. Ensure `spark-submit` is available in the Airflow worker environment and the path to `spark_stream.py` (`SPARK_SCRIPT_PATH` in the DAG) is correct. 