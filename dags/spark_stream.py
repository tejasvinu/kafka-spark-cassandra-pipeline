import logging
from cassandra.cluster import Cluster
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    FloatType,
    IntegerType,
)


def create_keyspace(session):
    session.execute(
        """
        CREATE KEYSPACE IF NOT EXISTS spark_streams
        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'};
    """
    )
    print("Keyspace created successfully!")


def create_table(session):
    session.execute(
        """
    CREATE TABLE IF NOT EXISTS spark_streams.weather_data (
        station_id UUID PRIMARY KEY,
        timestamp TEXT,
        city TEXT,
        country TEXT,
        latitude FLOAT,
        longitude FLOAT,
        temperature FLOAT,
        humidity INT,
        pressure FLOAT,
        wind_speed FLOAT,
        wind_direction TEXT,
        precipitation FLOAT,
        weather_condition TEXT);
    """
    )
    print("Weather data table created successfully!")


def create_spark_connection():
    s_conn = None
    try:
        s_conn = (
            SparkSession.builder.appName("SparkDataStreaming")
            .config(
                "spark.jars.packages",
                "com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,"
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1",
            )
            .config("spark.cassandra.connection.host", "localhost")
            .getOrCreate()
        )
        s_conn.sparkContext.setLogLevel("ERROR")
        logging.info("Spark connection created successfully!")
    except Exception as e:
        logging.error(f"Couldn't create the spark session due to exception {e}")

    return s_conn


def connect_to_kafka(spark_conn):
    spark_df = None
    try:
        spark_df = (
            spark_conn.readStream.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9092")
            .option("subscribe", "weather_data")
            .option("startingOffsets", "earliest")
            .load()
        )
        spark_df.printSchema()
        logging.info("Kafka DataFrame created successfully")
    except Exception as e:
        logging.warning(f"Kafka DataFrame could not be created because: {e}")

    return spark_df


def create_cassandra_connection():
    cas_session = None
    try:
        cluster = Cluster(["localhost"])
        cas_session = cluster.connect()
        return cas_session
    except Exception as e:
        logging.error(f"Could not create Cassandra connection due to {e}")
        return None


def create_selection_df_from_kafka(spark_df):
    # Define the schema for the nested weather data
    schema = StructType(
        [
            StructField("station_id", StringType(), False),
            StructField("timestamp", StringType(), False),
            StructField(
                "location",
                StructType(
                    [  # Nested JSON for location
                        StructField("city", StringType(), False),
                        StructField("country", StringType(), False),
                        StructField("latitude", FloatType(), False),
                        StructField("longitude", FloatType(), False),
                    ]
                ),
            ),
            StructField("temperature", FloatType(), False),
            StructField("humidity", IntegerType(), False),
            StructField("pressure", FloatType(), False),
            StructField("wind_speed", FloatType(), False),
            StructField("wind_direction", StringType(), False),
            StructField("precipitation", FloatType(), False),
            StructField("weather_condition", StringType(), False),
        ]
    )

    # Parse the Kafka value (JSON) according to the nested schema
    parsed_df = spark_df.selectExpr("CAST(value AS STRING)").select(
        from_json(col("value"), schema).alias("data")
    )

    # Flatten the nested "location" field
    flattened_df = parsed_df.select(
        col("data.station_id"),
        col("data.timestamp"),
        col("data.location.city").alias("city"),
        col("data.location.country").alias("country"),
        col("data.location.latitude").alias("latitude"),
        col("data.location.longitude").alias("longitude"),
        col("data.temperature"),
        col("data.humidity"),
        col("data.pressure"),
        col("data.wind_speed"),
        col("data.wind_direction"),
        col("data.precipitation"),
        col("data.weather_condition"),
    )

    return flattened_df


def start_spark_stream():
    # Create spark connection
    spark_conn = create_spark_connection()

    if spark_conn is not None:
        # Connect to Kafka with spark connection
        spark_df = connect_to_kafka(spark_conn)
        selection_df = create_selection_df_from_kafka(spark_df)
        session = create_cassandra_connection()

        if session is not None:
            create_keyspace(session)
            create_table(session)

            print("Streaming is being started...")

            try:
                streaming_query = (
                    selection_df.writeStream.format("org.apache.spark.sql.cassandra")
                    .option("checkpointLocation", "/tmp/checkpoint")
                    .option("keyspace", "spark_streams")
                    .option("table", "weather_data")
                    .start()
                )
                logging.info("Streaming query started successfully!")
            except Exception as e:
                logging.error(f"Streaming query failed due to: {e}")

            streaming_query.awaitTermination()


if __name__ == "__main__":
    start_spark_stream()
