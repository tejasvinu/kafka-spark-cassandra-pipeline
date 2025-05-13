#!/usr/bin/env python3

import logging
from cassandra.cluster import Cluster, NoHostAvailable, AuthenticationFailed
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import col, udf, explode, split, regexp_extract, when, lit, current_timestamp
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
    MapType,
    ArrayType,
    DoubleType
)
import re
from cassandra.auth import PlainTextAuthProvider

# Cassandra Configuration
# For direct Python driver connection (schema creation by Spark driver on HOST)
CASSANDRA_HOST_FOR_DRIVER = 'localhost' # Connect via host's localhost due to port mapping
CASSANDRA_PORT = 9042
CASSANDRA_USERNAME = 'cassandra'
CASSANDRA_PASSWORD = 'cassandra'

# Configure logging for the Spark script (will be visible in Spark driver logs)
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(module)s - %(message)s')


def create_keyspace(session):
    """Creates the keyspace if it doesn't exist."""
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS spark_streams
            WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
        """)
        logging.info("Keyspace 'spark_streams' created successfully or already exists.")
        return True
    except Exception as e:
        logging.error(f"Failed to create keyspace: {str(e)}", exc_info=True)
        return False


def create_table(session):
    """Creates the metrics table if it doesn't exist."""
    try:
        session.execute("""
        CREATE TABLE IF NOT EXISTS spark_streams.prometheus_metrics (
            metric_name TEXT,
            timestamp TIMESTAMP,
            value DOUBLE,
            labels MAP<TEXT, TEXT>,
            job TEXT,
            PRIMARY KEY ((metric_name, job), timestamp)
        ) WITH CLUSTERING ORDER BY (timestamp DESC);
        """)
        logging.info("Table 'prometheus_metrics' created successfully or already exists.")
        return True
    except Exception as e:
        logging.error(f"Failed to create table: {str(e)}", exc_info=True)
        return False


def create_spark_connection():
    s_conn = None
    try:
        s_conn = (
            SparkSession.builder.appName("SparkPrometheusStream")
            .master("spark://localhost:7077") # Spark master is accessible from host via port mapping
            # Packages are provided by spark-submit in orchestrator.py, no need to list them here.
            # If listed, ensure versions match those in spark-submit.
            # .config("spark.jars.packages",
            #         "com.datastax.spark:spark-cassandra-connector_2.12:3.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3,org.apache.kafka:kafka-clients:3.4.1")
            
            # Configuration for Spark Cassandra Connector (used by EXECUTORS in Docker network)
            .config("spark.cassandra.connection.host", "cassandra") # Executors connect to 'cassandra' hostname
            .config("spark.cassandra.connection.port", str(CASSANDRA_PORT))
            .config("spark.cassandra.auth.username", CASSANDRA_USERNAME)
            .config("spark.cassandra.auth.password", CASSANDRA_PASSWORD)
            .getOrCreate()
        )
        # It's good practice to set log level for Spark context if not done elsewhere
        s_conn.sparkContext.setLogLevel("WARN") # Or "ERROR" to be less verbose
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}", exc_info=True)
    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        # Kafka is 'broker:29092' internally in Docker network.
        # If the driver (on host) needs to resolve this for metadata, it might fail.
        # If it fails, change this to 'localhost:9092' (requires Kafka to be accessible to executors via this address,
        # which is complex) or, preferably, run the Spark driver inside the Docker network.
        # For now, let's assume executors handle the connection primarily.
        spark_df = (
            spark_conn.read.format("kafka")
            # Use localhost:9092 for the driver running on the host
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribePattern", "prometheus_metrics_*")
            .option("failOnDataLoss", "false") # Good for dev, consider implications for prod
            # .option("startingOffsets", "earliest") # Useful for batch reprocessing during dev
            .load()
        )
        logging.info("Kafka DataFrame loaded successfully for batch processing")
    except Exception as e:
        logging.error(f"Kafka DataFrame could not be created because: {e}", exc_info=True)
    return spark_df


def create_cassandra_connection():
    """Creates a connection to Cassandra (from Spark DRIVER ON HOST) and returns the session."""
    try:
        cluster = Cluster(
            [CASSANDRA_HOST_FOR_DRIVER], # Use 'localhost' for driver on host
            port=CASSANDRA_PORT,
            auth_provider=PlainTextAuthProvider(
                username=CASSANDRA_USERNAME,
                password=CASSANDRA_PASSWORD
            )
        )
        logging.info(f"Attempting to connect to Cassandra at {CASSANDRA_HOST_FOR_DRIVER}:{CASSANDRA_PORT} (for schema setup by driver)")
        cas_session = cluster.connect() # This will connect to keyspace system by default
        logging.info(f"Successfully connected to Cassandra cluster via Python driver from host ({CASSANDRA_HOST_FOR_DRIVER})")
        return cas_session
    except NoHostAvailable as e:
        logging.error(f"Could not connect to Cassandra host at {CASSANDRA_HOST_FOR_DRIVER}:{CASSANDRA_PORT} using Python driver. Error: {str(e)}", exc_info=True)
        return None
    except AuthenticationFailed as e:
        logging.error(f"Authentication failed when connecting to Cassandra ({CASSANDRA_HOST_FOR_DRIVER}) using Python driver. Error: {str(e)}", exc_info=True)
        return None
    except Exception as e:
        logging.error(f"Unexpected error while connecting to Cassandra ({CASSANDRA_HOST_FOR_DRIVER}) using Python driver: {str(e)}", exc_info=True)
        return None


def parse_labels_udf(labels_str):
    if labels_str is None:
        return {}
    labels = {}
    # Regex to find all label="value" pairs
    # Original: r'([a-zA-Z_][a-zA-Z0-9_]*)\\s*=\\s*\\"([^\\"]*)\\"'
    # Python raw string r"" means backslashes are literal.
    # So \\s* becomes \s* for regex engine. \\" becomes \"
    # This is correct if the input string has literal backslashes before " and s.
    # Assuming standard Prometheus format, it should be: label="value"
    # Let's simplify for clarity if Prometheus format doesn't have escaped quotes in the string itself.
    # If labels_str is like 'key1=\"value1\",key2=\"value2\"' then it's fine.
    # If labels_str is like 'key1="value1",key2="value2"' then use:
    pattern = re.compile(r'([a-zA-Z_][a-zA-Z0-9_]*)\s*=\s*"([^"]*)"')
    for match in pattern.finditer(labels_str):
        labels[match.group(1)] = match.group(2)
    return labels

parse_labels = udf(parse_labels_udf, MapType(StringType(), StringType()))

def create_selection_df_from_kafka(spark_df):
    raw_metrics_df = spark_df.select(
        F.col("topic"),
        F.col("value").cast("string").alias("metrics_text")
    )

    metrics_with_job_df = raw_metrics_df.withColumn(
        "job",
        F.regexp_extract(F.col("topic"), "prometheus_metrics_(.*)", 1)
    )

    lines_df = metrics_with_job_df.select(
        F.col("job"),
        F.explode(F.split(F.col("metrics_text"), "\n")).alias("metric_line") # Use "\n" for newline
    )

    filtered_lines_df = lines_df.filter(
        (~F.col("metric_line").startswith("#")) & (F.trim(F.col("metric_line")) != "")
    )
    
    # Regex for Prometheus metrics: metric_name{labels} value timestamp (timestamp optional here)
    # Adjusted to handle missing labels more gracefully and ensure metric_name is captured.
    # Original: r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(\\{([^\\}]*)\\})?\\s+([\\+\\-]?(?:[0-9]*[.])?[0-9]+(?:[eE][\\+\\-]?[0-9]+)?).*"
    # In Python string, \\ becomes \ for regex. So \\{ is \{. This is correct for literal brace.
    # Using raw string for regex is often clearer:
    regex_pattern = r"^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{(.*?)\})?\s+([\+\-]?(?:[0-9]*\.)?[0-9]+(?:[eE][\+\-]?[0-9]+)?)(?:\s+\d+)?$"
    # Group 1: metric_name
    # Group 2: labels_str (optional, non-capturing group for outer braces)
    # Group 3: value
    # Optional timestamp at the end: (?:\s+\d+)?

    parsed_metrics_df = filtered_lines_df.select(
        F.col("job"),
        F.col("metric_line"), # Keep for debugging
        F.regexp_extract(F.col("metric_line"), regex_pattern, 1).alias("metric_name_raw"),
        F.regexp_extract(F.col("metric_line"), regex_pattern, 2).alias("labels_str"), # Content inside {}
        F.regexp_extract(F.col("metric_line"), regex_pattern, 3).alias("value_str")
    )

    valid_metrics_df = parsed_metrics_df.filter(F.col("metric_name_raw") != "")

    # Debug: Show what's being parsed
    # valid_metrics_df.show(truncate=False)

    final_df = valid_metrics_df.select(
        F.col("metric_name_raw").alias("metric_name"),
        parse_labels(F.col("labels_str")).alias("labels"),
        F.col("value_str").cast(DoubleType()).alias("value"),
        F.col("job"),
        F.current_timestamp().alias("timestamp")
    )
    
    return final_df.select("metric_name", "timestamp", "value", "labels", "job")


def start_spark_stream():
    try:
        spark_conn = create_spark_connection()
        if spark_conn is None:
            logging.error("Failed to create Spark connection. Exiting.")
            return

        # Create Cassandra connection using Python driver for schema setup (run by DRIVER ON HOST)
        # This session is ONLY for DDL (keyspace/table creation)
        cassandra_py_session = create_cassandra_connection()
        if cassandra_py_session is None:
            logging.error("Failed to create Cassandra connection using Python driver (for schema). Exiting.")
            # spark_conn.stop() # Clean up Spark session if further operations depend on Cassandra session
            return

        if not create_keyspace(cassandra_py_session):
            logging.error("Failed to create/ensure keyspace exists. Exiting.")
            cassandra_py_session.shutdown()
            # spark_conn.stop()
            return

        if not create_table(cassandra_py_session):
            logging.error("Failed to create/ensure table exists. Exiting.")
            cassandra_py_session.shutdown()
            # spark_conn.stop()
            return
        
        # Important: Shutdown the Python driver Cassandra session as it's no longer needed.
        # The actual data writing will use Spark's own Cassandra connector.
        cassandra_py_session.shutdown()
        logging.info("Python Cassandra driver session (for schema setup) shut down.")

        # Connect to Kafka and process data
        spark_df_kafka = connect_to_kafka(spark_conn)
        if spark_df_kafka is None or spark_df_kafka.rdd.isEmpty(): # Check if DataFrame is empty
            logging.warning("Kafka DataFrame is None or empty. No data to process.")
            spark_conn.stop()
            return

        selection_df = create_selection_df_from_kafka(spark_df_kafka)
        if selection_df is None or selection_df.rdd.isEmpty():
            logging.warning("Processed DataFrame is None or empty. No data to write to Cassandra.")
            spark_conn.stop()
            return
        
        logging.info("Schema of DataFrame to be written to Cassandra:")
        selection_df.printSchema()
        logging.info("Sample data to be written to Cassandra:")
        selection_df.show(5, truncate=False)

        # Write data to Cassandra using Spark Cassandra Connector
        try:
            selection_df.write \
                .format("org.apache.spark.sql.cassandra") \
                .option("keyspace", "spark_streams") \
                .option("table", "prometheus_metrics") \
                .mode("append") \
                .save()
            logging.info("Successfully wrote data to Cassandra using Spark Cassandra Connector.")
        except Exception as e:
            logging.error(f"Failed to write data to Cassandra using Spark Cassandra Connector: {str(e)}", exc_info=True)
        
        spark_conn.stop() # Stop Spark session after batch processing

    except Exception as e:
        logging.error(f"Spark job failed with an unhandled exception: {str(e)}", exc_info=True)
        # If spark_conn was initialized, try to stop it
        if 'spark_conn' in locals() and spark_conn:
            try:
            try:spark_conn.stop()
                spark_conn.stop()_stop:
            except Exception as e_stop:topping Spark connection during cleanup: {e_stop}", exc_info=True)
                logging.error(f"Error stopping Spark connection during cleanup: {e_stop}", exc_info=True)
        raise # Re-raise the exception so orchestrator knows it failed

if __name__ == "__main__":
if __name__ == "__main__":for the script itself, if run directly or by Spark.
    # Setup basic logging for the script itself, if run directly or by Spark.ubmitted.
    # This configuration might be overridden by Spark's logging system when submitted.onfigured
    if not logging.getLogger().handlers: # Avoid adding multiple handlers if already configured- %(message)s')
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logging.info("Starting Spark batch job (spark_stream.py)...")
    logging.info("Starting Spark batch job (spark_stream.py)...")
    start_spark_stream()batch job (spark_stream.py) finished.")    logging.info("Spark batch job (spark_stream.py) finished.")