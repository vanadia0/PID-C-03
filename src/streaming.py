import os
import json
from datetime import datetime, timezone

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pymongo import MongoClient


PAYLOAD_SCHEMA = StructType(
    [
        StructField("device_id", StringType(), False),
        StructField("device_type", StringType(), False),
        StructField("building_id", StringType(), False),
        StructField("timestamp", StringType(), False),
        StructField(
            "data",
            StructType(
                [
                    StructField("port_in_mbps", DoubleType(), True),
                    StructField("port_out_mbps", DoubleType(), True),
                    StructField("errors", LongType(), True),
                    StructField("uptime_s", LongType(), True),
                    StructField("clients", LongType(), True),
                    StructField("rssi_avg_dbm", DoubleType(), True),
                    StructField("channel_util_pct", DoubleType(), True),
                    StructField("latency_ms", DoubleType(), True),
                    StructField("packet_loss_pct", DoubleType(), True),
                    StructField("target", StringType(), True),
                ]
            ),
            True,
        ),
    ]
)


def write_batch(batch_df: DataFrame, batch_id: int) -> None:
    del batch_id
    rows = batch_df.collect()
    if not rows:
        return

    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017")
    mongo_db = os.getenv("MONGO_DB", "network")
    with MongoClient(mongo_uri) as client:
        database = client[mongo_db]
        received_at = datetime.now(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")
        documents = []
        for row in rows:
                payload = json.loads(row.raw_payload)
            documents.append(
                {
                    "device_id": row.device_id,
                    "device_type": row.device_type,
                    "building_id": row.building_id,
                    "payload": payload,
                    "received_at": received_at,
                    "quality_status": "pending",
                }
            )
        if documents:
            database.telemetry_raw.insert_many(documents)


def main() -> None:
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "campus.telemetry")
    mongo_uri = os.getenv("MONGO_URI", "mongodb://mongodb:27017")

    spark = (
        SparkSession.builder.appName("campus-ingest")
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )

    stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "latest")
        .load()
    )

    payload = (
        stream.select(F.from_json(F.col("value").cast("string"), PAYLOAD_SCHEMA).alias("payload"))
        .withColumn("raw_payload", F.col("value").cast("string"))
        .select("payload.*", "raw_payload")
        .filter(
            F.col("device_id").isNotNull()
            & F.col("device_type").isNotNull()
            & F.col("building_id").isNotNull()
        )
        .withColumn("event_time", F.to_timestamp("timestamp"))
    )

    raw_query = payload.writeStream.foreachBatch(write_batch).outputMode("append").start()

    aggregate = (
        payload.withWatermark("event_time", "2 minutes")
        .groupBy(
            F.window("event_time", "1 minute"),
            "building_id",
            "device_type",
        )
        .agg(F.count("*").alias("message_count"))
    )

    aggregate_query = (
        aggregate.writeStream.format("console")
        .outputMode("update")
        .option("truncate", False)
        .start()
    )

    raw_query.awaitTermination()
    aggregate_query.awaitTermination()


if __name__ == "__main__":
    main()
