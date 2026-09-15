from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    from_json,
    struct,
    to_json,
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

INPUT_TOPIC = "raw-telemetry"
OUTPUT_TOPIC = "clean-telemetry"

CHECKPOINT_LOCATION = ".checkpoints/dedup_v2"


# ---------------------------------------------------------
# Spark session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("IoTTelemetryDeduplication")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# ---------------------------------------------------------
# Telemetry schema
# ---------------------------------------------------------

telemetry_schema = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("device_id", StringType(), False),
        StructField("event_time", StringType(), False),
        StructField("temperature", DoubleType(), True),
        StructField("vibration", DoubleType(), True),
        StructField("voltage", DoubleType(), True),
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
# Read raw Kafka telemetry
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
        INPUT_TOPIC,
    )
    .option(
        "startingOffsets",
        "latest",
    )
    .load()
)


# ---------------------------------------------------------
# Parse JSON
# ---------------------------------------------------------

parsed_stream = (
    raw_stream
    .select(
        col("value")
        .cast("string")
        .alias("json_value")
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

        col("data.event_time").alias(
            "event_time_raw"
        ),

        col("data.temperature"),
        col("data.vibration"),
        col("data.voltage"),
        col("data.is_injected_anomaly"),
        col("data.anomaly_type"),
    )
    .withColumn(
        "event_time",
        to_timestamp(
            col("event_time_raw")
        ),
    )
)


# ---------------------------------------------------------
# Stateful deduplication
# ---------------------------------------------------------

deduplicated_stream = (
    parsed_stream
    .withWatermark(
        "event_time",
        "10 minutes",
    )
    .dropDuplicatesWithinWatermark(
        ["event_id"]
    )
)


# ---------------------------------------------------------
# Serialize clean telemetry back to Kafka
# ---------------------------------------------------------

clean_output = (
    deduplicated_stream
    .select(
        col("device_id")
        .cast("string")
        .alias("key"),

        to_json(
            struct(
                col("event_id"),
                col("device_id"),

                col("event_time_raw").alias(
                    "event_time"
                ),

                col("temperature"),
                col("vibration"),
                col("voltage"),
                col("is_injected_anomaly"),
                col("anomaly_type"),
            )
        ).alias("value"),
    )
)


# ---------------------------------------------------------
# Write deduplicated events to Kafka
# ---------------------------------------------------------

query = (
    clean_output
    .writeStream
    .format("kafka")
    .option(
        "kafka.bootstrap.servers",
        KAFKA_BROKER,
    )
    .option(
        "topic",
        OUTPUT_TOPIC,
    )
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION,
    )
    .outputMode("append")
    .trigger(
        processingTime="5 seconds",
    )
    .start()
)


query.awaitTermination()