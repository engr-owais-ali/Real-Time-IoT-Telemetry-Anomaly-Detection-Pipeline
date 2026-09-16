from pyspark.sql import SparkSession
import os
import psycopg
from pyspark.sql.functions import (
    avg,
    col,
    count,
    from_json,
    max as spark_max,
    min as spark_min,
    round as spark_round,
    stddev,
    sum as spark_sum,
    to_timestamp,
    when,
    window,
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

KAFKA_BROKER = os.getenv(
    "KAFKA_BROKER",
    "localhost:9092",
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "clean-telemetry",
)

STARTING_OFFSETS = os.getenv(
    "STARTING_OFFSETS",
    "latest",
)

CHECKPOINT_LOCATION = os.getenv(
    "CHECKPOINT_LOCATION",
    ".checkpoints/window_features_pg_v1",
)

POSTGRES_DSN = os.getenv(
    "POSTGRES_DSN",
    "postgresql://telemetry:telemetry@localhost:5432/telemetry",
)

STREAM_QUERY_NAME = os.getenv(
    "STREAM_QUERY_NAME",
    "window_features_pg_v1",
)

# ---------------------------------------------------------
# Spark session
# ---------------------------------------------------------

spark = (
    SparkSession.builder
    .appName("IoTTelemetryStreaming")
    .master("local[*]")
    .config(
        "spark.sql.session.timeZone",
        "UTC",
    )
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


def write_batch_to_postgres(batch_df, batch_id):

    rows = batch_df.collect()

    if not rows:
        return

    with psycopg.connect(POSTGRES_DSN) as conn:

        with conn.cursor() as cursor:

            # -----------------------------------------
            # Claim this Spark micro-batch
            # -----------------------------------------

            cursor.execute(
                """
                INSERT INTO processed_stream_batches (
                    query_name,
                    batch_id
                )
                VALUES (%s, %s)
                ON CONFLICT DO NOTHING
                RETURNING batch_id;
                """,
                (
                    STREAM_QUERY_NAME,
                    batch_id,
                ),
            )

            inserted = cursor.fetchone()

            # This batch already reached PostgreSQL
            if inserted is None:
                print(
                    f"Batch {batch_id} already committed. "
                    f"Skipping."
                )
                return

            # -----------------------------------------
            # Upsert window aggregates
            # -----------------------------------------

            records = [
                row.asDict(recursive=True)
                for row in rows
            ]

            cursor.executemany(
                UPSERT_SQL,
                records,
            )

    print(
        f"✅ PostgreSQL batch={batch_id} "
        f"rows={len(rows)}"
    )



# ---------------------------------------------------------
# Incoming telemetry schema
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
# Read from Kafka
# ---------------------------------------------------------
print(f"KAFKA_BROKER = {KAFKA_BROKER!r}")
print(f"KAFKA_TOPIC  = {KAFKA_TOPIC!r}")

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
        STARTING_OFFSETS,
    )
    .load()
)


# ---------------------------------------------------------
# Parse Kafka JSON
# ---------------------------------------------------------

parsed_stream = (
    raw_stream
    .select(
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("json_value"),
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
# Simple event-level anomaly detection
# ---------------------------------------------------------

enriched_stream = (
    parsed_stream
    .withColumn(
        "detected_anomaly",
        (
            (col("temperature") > 85)
            | (col("vibration") > 0.80)
            | (col("voltage") < 210)
        ),
    )
)


# ---------------------------------------------------------
# Add watermark
# ---------------------------------------------------------

watermarked_stream = (
    enriched_stream
    .withWatermark(
        "event_time",
        "10 minutes",
    )
)


# ---------------------------------------------------------
# 5-minute sliding-window features
# Slide every 1 minute
# ---------------------------------------------------------

windowed_features = (
    watermarked_stream
    .groupBy(
        window(
            col("event_time"),
            "5 minutes",
            "1 minute",
        ),
        col("device_id"),
    )
    .agg(
        count("*").alias("event_count"),

        spark_round(
            avg("temperature"),
            2,
        ).alias("avg_temperature"),

        spark_round(
            spark_max("temperature"),
            2,
        ).alias("max_temperature"),

        spark_round(
            avg("vibration"),
            3,
        ).alias("avg_vibration"),

        spark_round(
            spark_max("vibration"),
            3,
        ).alias("max_vibration"),

        spark_round(
            stddev("vibration"),
            3,
        ).alias("stddev_vibration"),

        spark_round(
            avg("voltage"),
            2,
        ).alias("avg_voltage"),

        spark_round(
            spark_min("voltage"),
            2,
        ).alias("min_voltage"),

        spark_sum(
            when(
                col("detected_anomaly"),
                1,
            ).otherwise(0)
        ).alias("detected_anomaly_count"),

        spark_sum(
            when(
                col("is_injected_anomaly"),
                1,
            ).otherwise(0)
        ).alias("injected_anomaly_count"),
    )
)


# ---------------------------------------------------------
# Add window-level anomaly flag
# ---------------------------------------------------------

windowed_features = (
    windowed_features
    .withColumn(
        "has_anomaly",
        col("detected_anomaly_count") > 0,
    )
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        col("device_id"),
        col("event_count"),
        col("avg_temperature"),
        col("max_temperature"),
        col("avg_vibration"),
        col("max_vibration"),
        col("stddev_vibration"),
        col("avg_voltage"),
        col("min_voltage"),
        col("detected_anomaly_count"),
        col("injected_anomaly_count"),
        col("has_anomaly"),
    )
)

UPSERT_SQL = """
INSERT INTO window_features (
    window_start,
    window_end,
    device_id,
    event_count,
    avg_temperature,
    max_temperature,
    avg_vibration,
    max_vibration,
    stddev_vibration,
    avg_voltage,
    min_voltage,
    detected_anomaly_count,
    injected_anomaly_count,
    has_anomaly
)
VALUES (
    %(window_start)s,
    %(window_end)s,
    %(device_id)s,
    %(event_count)s,
    %(avg_temperature)s,
    %(max_temperature)s,
    %(avg_vibration)s,
    %(max_vibration)s,
    %(stddev_vibration)s,
    %(avg_voltage)s,
    %(min_voltage)s,
    %(detected_anomaly_count)s,
    %(injected_anomaly_count)s,
    %(has_anomaly)s
)
ON CONFLICT (
    window_start,
    window_end,
    device_id
)
DO UPDATE SET
    event_count = EXCLUDED.event_count,
    avg_temperature = EXCLUDED.avg_temperature,
    max_temperature = EXCLUDED.max_temperature,
    avg_vibration = EXCLUDED.avg_vibration,
    max_vibration = EXCLUDED.max_vibration,
    stddev_vibration = EXCLUDED.stddev_vibration,
    avg_voltage = EXCLUDED.avg_voltage,
    min_voltage = EXCLUDED.min_voltage,
    detected_anomaly_count =
        EXCLUDED.detected_anomaly_count,
    injected_anomaly_count =
        EXCLUDED.injected_anomaly_count,
    has_anomaly = EXCLUDED.has_anomaly,
    updated_at = NOW();
"""


# ---------------------------------------------------------
# Console output
# ---------------------------------------------------------

query = (
    windowed_features
    .writeStream
    .outputMode("update")
    .foreachBatch(write_batch_to_postgres)
    .option(
        "checkpointLocation",
        CHECKPOINT_LOCATION,
    )
    .trigger(
        processingTime="5 seconds",
    )
    .start()
)


query.awaitTermination()