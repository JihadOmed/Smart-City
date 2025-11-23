from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    TimestampType,
)
from pyspark.sql.functions import from_json, col

from config import configuration


def create_spark_session() -> SparkSession:
    """
    Create SparkSession configured for:
    - Kafka source
    - Google Cloud Storage (GCS) sink
    """

    # Read configs from your configuration object
    GCP_PROJECT_ID = configuration.get("GCP_PROJECT_ID")
    GCS_BUCKET = configuration.get("GCS_BUCKET")
    SERVICE_ACCOUNT_KEY = configuration.get("GOOGLE_APPLICATION_CREDENTIALS")

    spark = (
        SparkSession.builder.appName("SmartCityStreaming")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,"
            "com.google.cloud.bigdataoss:gcs-connector:hadoop3-2.2.5",
        )
        # GCS filesystem configs
        .config(
            "spark.hadoop.fs.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem",
        )
        .config(
            "spark.hadoop.fs.AbstractFileSystem.gs.impl",
            "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS",
        )
        .config("spark.hadoop.fs.gs.project.id", GCP_PROJECT_ID)
        .config("spark.hadoop.google.cloud.auth.service.account.enable", "true")
        .config(
            "spark.hadoop.google.cloud.auth.service.account.json.keyfile",
            SERVICE_ACCOUNT_KEY,
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_schemas():


    vehicle_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),  # (lat,lon) as string for now
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("fuelType", StringType(), True),
        ]
    )

    gps_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_Id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("vehicle_type", StringType(), True),
        ]
    )

    traffic_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_Id", StringType(), True),
            StructField("camera_Id", StringType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("snapshot", StringType(), True),
        ]
    )

    weather_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("device_Id", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("weatherCondition", StringType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("windSpeed", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("airQuality", DoubleType(), True),
        ]
    )

    emergency_schema = StructType(
        [
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("incidentId", StringType(), True),
            StructField("location", StringType(), True),
            StructField("type", StringType(), True),
            StructField("status", StringType(), True),
            StructField("description", StringType(), True),
        ]
    )

    return (
        vehicle_schema,
        gps_schema,
        traffic_schema,
        weather_schema,
        emergency_schema,
    )


def read_kafka_topic(spark, topic: str, schema: StructType, bootstrap_servers: str):

    df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
        .selectExpr("CAST(value AS STRING) as json_str")
        .select(from_json(col("json_str"), schema).alias("data"))
        .select("data.*")
        .withWatermark("timestamp", "2 minutes")
    )
    return df


def stream_writer(df, checkpoint_location: str, output_path: str):

    return (
        df.writeStream.format("parquet")
        .option("checkpointLocation", checkpoint_location)
        .option("path", output_path)
        .outputMode("append")
        .start()
    )


def main():
    spark = create_spark_session()

    # Values come from your config now
    BOOTSTRAP_SERVERS = configuration.get("KAFKA_BOOSTRAP_SERVERS", "localhost:9092")
    GCS_BUCKET = configuration.get("GCS_BUCKET")

    (
        vehicle_schema,
        gps_schema,
        traffic_schema,
        weather_schema,
        emergency_schema,
    ) = get_schemas()

    # Create streaming DataFrames from Kafka topics
    vehicle_df = read_kafka_topic(
        spark, "vehicle_data", vehicle_schema, BOOTSTRAP_SERVERS
    )
    gps_df = read_kafka_topic(spark, "gps_data", gps_schema, BOOTSTRAP_SERVERS)
    traffic_df = read_kafka_topic(
        spark, "traffic_data", traffic_schema, BOOTSTRAP_SERVERS
    )
    weather_df = read_kafka_topic(
        spark, "weather_data", weather_schema, BOOTSTRAP_SERVERS
    )
    emergency_df = read_kafka_topic(
        spark, "emergency_data", emergency_schema, BOOTSTRAP_SERVERS
    )

    # Define GCS output paths (e.g. Bronze layer)
    base_path = f"gs://{GCS_BUCKET}/smart_city_streaming"

    vehicle_query = stream_writer(
        vehicle_df,
        checkpoint_location=f"{base_path}/checkpoints/vehicle",
        output_path=f"{base_path}/vehicle",
    )

    gps_query = stream_writer(
        gps_df,
        checkpoint_location=f"{base_path}/checkpoints/gps",
        output_path=f"{base_path}/gps",
    )

    traffic_query = stream_writer(
        traffic_df,
        checkpoint_location=f"{base_path}/checkpoints/traffic",
        output_path=f"{base_path}/traffic",
    )

    weather_query = stream_writer(
        weather_df,
        checkpoint_location=f"{base_path}/checkpoints/weather",
        output_path=f"{base_path}/weather",
    )

    emergency_query = stream_writer(
        emergency_df,
        checkpoint_location=f"{base_path}/checkpoints/emergency",
        output_path=f"{base_path}/emergency",
    )

    # Keep the queries running
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()