import json
import time
import sys
import asyncio
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
from cassandra.cluster import Cluster
from cassandra import ConsistencyLevel
from cassandra.query import BatchStatement, BatchType
from aiokafka import AIOKafkaProducer, AIOKafkaConsumer

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

############################
# Kafka (aiokafka) Helpers #
############################

async def create_kafka_producer():
    """Create and start an AIOKafkaProducer. Rely on broker auto-create for topics."""
    try:
        producer = AIOKafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            compression_type="gzip",
            linger_ms=100
        )
        await producer.start()
        return producer
    except Exception as e:
        notify_failure(f"Failed to start aiokafka producer: {e}")
        return None

async def produce_to_kafka(producer: AIOKafkaProducer, topic: str, data_list):
    """Produce records concurrently in chunks to reduce latency for large metric sets."""
    if not producer or not data_list:
        return
    CHUNK_SIZE = 500  # tuneable
    errors = 0
    for start in range(0, len(data_list), CHUNK_SIZE):
        chunk = data_list[start:start+CHUNK_SIZE]
        coros = []
        for record in chunk:
            try:
                payload = json.dumps(record).encode('utf-8')
                coros.append(producer.send_and_wait(topic, payload))
            except Exception as e:
                errors += 1
                notify_failure(f"Enqueue produce error {e}")
        if coros:
            results = await asyncio.gather(*coros, return_exceptions=True)
            for r in results:
                if isinstance(r, Exception):
                    errors += 1
    if errors:
        print(f"Topic {topic}: {errors} produce errors out of {len(data_list)} messages")

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
async def consume_kafka_and_insert_to_cassandra(stop_event: asyncio.Event):
    """Consume metrics from Kafka and insert into Cassandra asynchronously (I/O wise)."""
    print("Starting aiokafka consumer to insert metrics into Cassandra...")
    session = create_cassandra_session()
    topic_table_map = {src["kafka_topic"]: (src["cassandra_table"], src["job_name"]) for src in DATA_SOURCES}
    topics = list(topic_table_map.keys())
    consumer = AIOKafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        group_id='metrics_cassandra_sink',
        auto_offset_reset='earliest'
    )
    await consumer.start()

    loop = asyncio.get_running_loop()

    def insert_single(metric_obj, table, job_name):
        # reuse logic from earlier multi-insert path
        stmt = INSERT_STMTS[table]
        metric = metric_obj.get("metric", {})
        value_arr = metric_obj.get("value", [])
        metric_name = metric.get("__name__", "unknown")
        instance = metric.get("instance", "unknown")
        labels = {k: v for k, v in metric.items()}
        for k in ("__name__", "instance", "job"):
            labels.pop(k, None)
        try:
            ts = datetime.fromtimestamp(float(value_arr[0]))
            val = float(value_arr[1])
        except Exception:
            ts, val = None, None
        try:
            future = session.execute_async(stmt, (job_name, metric_name, instance, labels, ts, val))
            future.result()  # wait to surface errors
        except Exception as e:
            notify_failure(f"Cassandra insert failed: {e}")

    try:
        while not stop_event.is_set():
            try:
                # getmany allows some batching. timeout to allow cancellation checks.
                results = await consumer.getmany(timeout_ms=1000, max_records=500)
                if not results:
                    continue
                for tp, messages in results.items():
                    table, job_name = topic_table_map[tp.topic]
                    for msg in messages:
                        try:
                            data = json.loads(msg.value.decode('utf-8'))
                        except Exception as e:
                            notify_failure(f"JSON decode failed: {e}")
                            continue
                        # Run blocking Cassandra write in default thread pool
                        await loop.run_in_executor(None, insert_single, data, table, job_name)
            except Exception as e:
                notify_failure(f"Error in consumer loop: {e}")
                await asyncio.sleep(1)
    except asyncio.CancelledError:
        print("Kafka consumer task cancelled.")
    finally:
        await consumer.stop()
        print("Kafka consumer stopped.")

# --- Main Orchestration ---
async def run_prometheus_kafka_etl(producer: AIOKafkaProducer):
    """Scrape all sources concurrently and push to Kafka with timing logs."""
    cycle_start = time.time()
    print("Starting Prometheus scraping + Kafka production cycle...")

    async def process(src):
        t0 = time.time()
        data = await asyncio.to_thread(scrape_prometheus_metrics, src["job_name"])
        scrape_dur = time.time() - t0
        count = len(data)
        if data:
            p0 = time.time()
            await produce_to_kafka(producer, src["kafka_topic"], data)
            prod_dur = time.time() - p0
        else:
            prod_dur = 0.0
        print(f"Job {src['job_name']}: scraped {count} series in {scrape_dur:.2f}s, produced in {prod_dur:.2f}s")

    await asyncio.gather(*(process(s) for s in DATA_SOURCES))
    total = time.time() - cycle_start
    print(f"Prometheus scraping + Kafka production cycle complete in {total:.2f}s")

async def etl_loop(stop_event: asyncio.Event):
    producer = await create_kafka_producer()
    if not producer:
        notify_failure("Cannot start ETL loop without producer")
        return
    try:
        while not stop_event.is_set():
            try:
                await run_prometheus_kafka_etl(producer)
                print(f"[{datetime.now()}] ETL cycle completed.")
            except Exception as e:
                notify_failure(f"ETL cycle error: {e}")
            if stop_event.is_set():
                break
            print("Sleeping 15s before next ETL cycle...")
            await asyncio.sleep(15)
    except asyncio.CancelledError:
        print("ETL loop task cancelled.")
    finally:
        try:
            await producer.stop()
        except Exception:
            pass
        print("Producer stopped.")

async def cassandra_loop(stop_event: asyncio.Event):
    await consume_kafka_and_insert_to_cassandra(stop_event)

async def run_all_mode():
    stop_event = asyncio.Event()
    consumer_task = asyncio.create_task(cassandra_loop(stop_event))
    etl_task = asyncio.create_task(etl_loop(stop_event))
    try:
        await asyncio.gather(etl_task, consumer_task)
    except asyncio.CancelledError:
        pass
    finally:
        stop_event.set()
        consumer_task.cancel()
        etl_task.cancel()
        await asyncio.gather(consumer_task, etl_task, return_exceptions=True)
        print("All mode tasks stopped.")

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"
    print(f"Selected mode: {mode}")
    try:
        if mode == "all":
            print("Running in 'all' mode (ETL loop + Cassandra sink). Ctrl+C to stop.")
            asyncio.run(run_all_mode())
        elif mode == "etl":
            print("Running ETL loop only. Ctrl+C to stop.")
            asyncio.run(etl_loop(asyncio.Event()))
        elif mode == "cassandra":
            print("Running Cassandra sink only. Ctrl+C to stop.")
            stop_event = asyncio.Event()
            asyncio.run(consume_kafka_and_insert_to_cassandra(stop_event))
        else:
            print(f"Unknown mode: {mode}. Use 'etl', 'cassandra', or 'all'.")
    except KeyboardInterrupt:
        print("Interrupted by user – shutting down.")