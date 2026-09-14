from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    to_timestamp,
)
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    StringType,
    StructField,
    StructType,
)


# ---------------------------------------------------------
# Configuration
# ---------------------------------------------------------

KAFKA_BROKER = "localhost:9092"
KAFKA_TOPIC = "raw-telemetry"


# ---------------------------------------------------------
# Spark session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("IoTTelemetryStreaming")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Telemetry JSON schema
# ---------------------------------------------------------

telemetry_schema = StructType(
    [
        StructField(
            "event_id",
            StringType(),
            False,
        ),
        StructField(
            "device_id",
            StringType(),
            False,
        ),
        StructField(
            "event_time",
            StringType(),
            False,
        ),
        StructField(
            "temperature",
            DoubleType(),
            True,
        ),
        StructField(
            "vibration",
            DoubleType(),
            True,
        ),
        StructField(
            "voltage",
            DoubleType(),
            True,
        ),
        StructField(
            "is_injected_anomaly",
            BooleanType(),
            True,
        ),
        StructField(
            "anomaly_type",
            StringType(),
            True,
        ),
    ]
)


# ---------------------------------------------------------
# Read raw Kafka stream
# ---------------------------------------------------------

raw_stream = (
    spark.readStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BROKER,
    )
    .option(
        "subscribe",
        KAFKA_TOPIC,
    )
    .option(
        "startingOffsets",
        "latest",
    )
    .load()
)


# ---------------------------------------------------------
# Parse Kafka key + JSON value
# ---------------------------------------------------------

parsed_stream = (
    raw_stream
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("kafka_timestamp"),
    )
    .withColumn(
        "data",
        from_json(
            col("json_value"),
            telemetry_schema,
        ),
    )
    .select(
        col("data.event_id"),
        col("data.device_id"),
        to_timestamp(
            col("data.event_time")
        ).alias("event_time"),
        col("data.temperature"),
        col("data.vibration"),
        col("data.voltage"),
        col("data.is_injected_anomaly"),
        col("data.anomaly_type"),
        col("kafka_key"),
        col("partition"),
        col("offset"),
        col("kafka_timestamp"),
    )
)


# ---------------------------------------------------------
# Print stream
# ---------------------------------------------------------

query = (
    parsed_stream.writeStream
    .format("console")
    .outputMode("append")
    .option("truncate", "false")
    .start()
)


query.awaitTermination()