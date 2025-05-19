import json
import time
import requests
import os
import sys
from datetime import datetime
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType
from confluent_kafka import Producer, Consumer
from confluent_kafka.admin import AdminClient, NewTopic
from concurrent.futures import ThreadPoolExecutor, as_completed

# --- Configuration ---
PROMETHEUS_URL = "http://localhost:9091"
KAFKA_BROKER = "localhost:9092"
CASSANDRA_HOSTS = ["localhost"]
CASSANDRA_KEYSPACE = "metrics_keyspace"

DATA_SOURCES = [
    {
        "job_name": "ipmi-exporter",
        "kafka_topic": "ipmi_metrics",
        "cassandra_table": "ipmi_data"
    },
    {
        "job_name": "node-exporter",
        "kafka_topic": "node_metrics",
        "cassandra_table": "node_data"
    },
    {
        "job_name": "dcgm-exporter",
        "kafka_topic": "dcgm_metrics",
        "cassandra_table": "dcgm_data"
    },
    {
        "job_name": "slurm-exporter",
        "kafka_topic": "slurm_metrics",
        "cassandra_table": "slurm_data"
    }
]

INSERT_STMTS = {}

# add notify_failure if not already present
def notify_failure(msg):
    print(f"[{datetime.now()}] ALERT: {msg}")

# --- Prometheus Scraping ---
def scrape_prometheus_metrics(job_name):
    """Scrapes all metrics from Prometheus for a given job name, with retries."""
    query = f'{{job="{job_name}"}}'
    for attempt in range(1, 4):
        try:
            resp = requests.get(f"{PROMETHEUS_URL}/api/v1/query", params={"query": query})
            resp.raise_for_status()
            js = resp.json()
            results = js.get('data', {}).get('result', [])
            print(f"Scraped {len(results)} series for job {job_name} (attempt {attempt})")
            return results
        except Exception as e:
            print(f"Attempt {attempt} failed scraping job {job_name}: {e}")
            time.sleep(1)
    notify_failure(f"Failed to scrape Prometheus for job {job_name} after 3 attempts")
    return []

# --- Kafka Production ---
def create_kafka_producer():
    try:
        # ensure topics exist
        admin = AdminClient({'bootstrap.servers': KAFKA_BROKER})
        topics = [src['kafka_topic'] for src in DATA_SOURCES]
        new_topics = [NewTopic(t, num_partitions=1, replication_factor=1) for t in topics]
        fs = admin.create_topics(new_topics, request_timeout=5)
        for t, f in fs.items():
            try:
                f.result()
            except Exception:
                pass  # already exists or failed, we'll retry on produce
        producer_conf = {
            'bootstrap.servers': KAFKA_BROKER,
            'compression.type': 'gzip',
            'linger.ms': 100,
            'batch.num.messages': 10000
        }
        producer = Producer(producer_conf)
        return producer
    except Exception as e:
        print(f"Error creating Kafka producer: {e}")
        return None

def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed for topic {msg.topic()}: {err}")

def produce_to_kafka(producer, topic, data_list):
    if not producer:
        print(f"Kafka producer not available for topic {topic}.")
        return
    for record in data_list:
        try:
            message = json.dumps(record).encode('utf-8')
            producer.produce(topic, value=message, callback=delivery_report)
        except Exception as e:
            print(f"Error producing message to Kafka topic {topic}: {e}")
    producer.flush(timeout=10)

# --- Cassandra Insert ---
def create_cassandra_session():
    """Creates a Cassandra session and ensures keyspace/tables exist."""
    cluster = Cluster(CASSANDRA_HOSTS)
    session = cluster.connect()
    session.execute(f"""
        CREATE KEYSPACE IF NOT EXISTS {CASSANDRA_KEYSPACE}
        WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}};
    """)
    session.set_keyspace(CASSANDRA_KEYSPACE)
    # enforce strong consistency
    session.default_consistency_level = ConsistencyLevel.LOCAL_QUORUM
    for source in DATA_SOURCES:
        table = source['cassandra_table']
        session.execute(f"""
        CREATE TABLE IF NOT EXISTS {table} (
            job_name TEXT,
            metric_name TEXT,
            instance TEXT,
            labels MAP<TEXT, TEXT>,
            timestamp TIMESTAMP,
            value DOUBLE,
            PRIMARY KEY ((job_name, metric_name, instance), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """)
        # prepare statement with quorum consistency
        stmt = session.prepare(f"""
            INSERT INTO {table} (job_name, metric_name, instance, labels, timestamp, value)
            VALUES (?, ?, ?, ?, ?, ?)
        """)
        stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        INSERT_STMTS[table] = stmt
    return session

def insert_metrics_to_cassandra(session, table, metrics_data, job_name):
    stmt = INSERT_STMTS[table]
    # for very large payloads, use unlogged batch in chunks
    if len(metrics_data) > 500:
        for start in range(0, len(metrics_data), 500):
            chunk = metrics_data[start:start+500]
            batch = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_QUORUM,
                                   batch_type=BatchType.UNLOGGED)
            for item in chunk:
                metric = item.get("metric", {})
                value_arr = item.get("value", [])
                metric_name = metric.get("__name__", "unknown")
                instance = metric.get("instance", "unknown")
                labels = {k: v for k, v in metric.items()}
                for k in ("__name__", "instance", "job"):
                    labels.pop(k, None)
                try:
                    ts = datetime.fromtimestamp(float(value_arr[0]))
                    val = float(value_arr[1])
                except:
                    ts, val = None, None
                try:
                    batch.add(stmt, (job_name, metric_name, instance, labels, ts, val))
                except Exception as e:
                    notify_failure(f"Batch insert failed for table {table}: {e}")
            try:
                session.execute(batch)
            except Exception as e:
                notify_failure(f"Batch insert failed for table {table}: {e}")
        return
    # batch in parallel threads
    def write(item):
        metric = item.get("metric", {})
        value_arr = item.get("value", [])
        metric_name = metric.get("__name__", "unknown")
        instance = metric.get("instance", "unknown")
        labels = {k: v for k, v in metric.items()}
        for k in ("__name__", "instance", "job"):
            labels.pop(k, None)
        try:
            ts = datetime.fromtimestamp(float(value_arr[0]))
            val = float(value_arr[1])
        except:
            ts, val = None, None
        try:
            return session.execute_async(stmt, (job_name, metric_name, instance, labels, ts, val))
        except Exception as e:
            notify_failure(f"Async submit failed for {metric_name}: {e}")
            return None

    futures = []
    with ThreadPoolExecutor(max_workers=16) as pool:
        for item in metrics_data:
            futures.append(pool.submit(write, item))

    # wait for results
    for f in as_completed(futures):
        fut = f.result()
        if fut:
            try:
                fut.result()
            except Exception as e:
                notify_failure(f"Cassandra write failed: {e}")

# --- Kafka Consumer and Cassandra Sink ---
def create_kafka_consumer(topics):
    # ensure topics exist before subscribing
    admin = AdminClient({'bootstrap.servers': KAFKA_BROKER})
    fs = admin.create_topics([NewTopic(t, num_partitions=1, replication_factor=1) for t in topics], request_timeout=5)
    for t, f in fs.items():
        try:
            f.result()
        except Exception:
            pass
    consumer_conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'group.id': 'metrics_cassandra_sink',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe(topics)
    return consumer

def consume_kafka_and_insert_to_cassandra():
    print("Starting Kafka consumer to insert metrics into Cassandra...")
    session = create_cassandra_session()
    topic_table_map = {src["kafka_topic"]: (src["cassandra_table"], src["job_name"]) for src in DATA_SOURCES}
    topics = list(topic_table_map.keys())
    consumer = create_kafka_consumer(topics)
    try:
        while True:
            try:
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    print(f"Kafka error: {msg.error()}")
                    continue
                data = json.loads(msg.value().decode('utf-8'))
                topic = msg.topic()
                cassandra_table, job_name = topic_table_map[topic]
                insert_metrics_to_cassandra(session, cassandra_table, [data], job_name)
            except Exception as e:
                print(f"Error in Kafka consumer loop: {e}")
                time.sleep(1)
    except KeyboardInterrupt:
        print("\nKafka consumer stopped by user.")
    finally:
        consumer.close()

# --- Main Orchestration ---
def run_prometheus_kafka_etl():
    """Main function to scrape Prometheus and produce to Kafka."""
    print("Starting Prometheus scraping and Kafka production cycle...")
    kafka_producer = create_kafka_producer()
    if not kafka_producer:
        print("Failed to create Kafka producer. Aborting ETL cycle.")
        return
    # parallel scrape and produce
    def process(src):
        data = scrape_prometheus_metrics(src["job_name"])
        if data:
            produce_to_kafka(kafka_producer, src["kafka_topic"], data)
    with ThreadPoolExecutor(max_workers=len(DATA_SOURCES)) as ex:
        tasks = [ex.submit(process, s) for s in DATA_SOURCES]
        for t in as_completed(tasks):
            try: t.result()
            except Exception as e: print(f"ETL error: {e}")
    print("Prometheus scraping and Kafka production cycle complete.")

if __name__ == "__main__":
    mode = None
    if len(sys.argv) > 1:
        mode = sys.argv[1]

    if mode is None or mode == "all":
        if mode == "all":
            print("Running in 'all' mode: Starting ETL process and Kafka->Cassandra consumer.")
        else:
            print("No mode specified, defaulting to 'all' mode: Starting ETL process and Kafka->Cassandra consumer.")

        # Start Kafka consumer in a subprocess
        from multiprocessing import Process
        consumer_proc = Process(target=consume_kafka_and_insert_to_cassandra)
        consumer_proc.start()
        try:
            while True:
                try:
                    run_prometheus_kafka_etl()
                except Exception as e:
                    print(f"Error during ETL cycle: {e}")
                print("ETL cycle complete. Sleeping for 15 seconds before next cycle...")
                time.sleep(15)
        except KeyboardInterrupt:
            print("\nETL process stopped by user.")
        finally:
            consumer_proc.terminate()
            consumer_proc.join()
            print("Exiting 'all' mode.")
    elif mode == "etl":
        print("Running in ETL mode (Prometheus Scrape -> Kafka Produce) in a loop.")
        print("Press Ctrl+C to stop.")
        try:
            while True:
                try:
                    run_prometheus_kafka_etl()
                except Exception as e:
                    print(f"Error during ETL cycle: {e}")
                print("ETL cycle complete. Sleeping for 15 seconds before next ETL cycle...")
                time.sleep(15)
        except KeyboardInterrupt:
            print("\nETL process stopped by user.")
        finally:
            print("Exiting ETL mode.")
    elif mode == "cassandra":
        print("Running in Cassandra sink mode (Kafka Consume -> Cassandra Insert)")
        consume_kafka_and_insert_to_cassandra()
    else:
        print(f"Unknown mode: {sys.argv[1]}. Use 'etl', 'cassandra', or 'all' (or no arguments for 'all').")
        print("Example for 'all' mode: python script/pipeline_orchestrator.py")
        print("Example for ETL only: python script/pipeline_orchestrator.py etl")
        print("Example for Cassandra sink only: python script/pipeline_orchestrator.py cassandra")