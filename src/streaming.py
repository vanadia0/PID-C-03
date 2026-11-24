import json
import math
import os
import re
import statistics
from collections import defaultdict, deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        ArrayType,
        DoubleType,
        LongType,
        MapType,
        StringType,
        StructField,
        StructType,
    )
except ImportError:
    DataFrame = Any
    SparkSession = None
    F = None
    ArrayType = DoubleType = LongType = MapType = StringType = StructField = StructType = None

try:
    from pymongo import MongoClient
except ImportError:
    MongoClient = None


QUALITY_TYPES = (
    "counter_reset",
    "inconsistent_unit",
    "missing_heartbeat",
)

_HEARTBEATS: dict[str, datetime] = {}
_TRAFFIC_HISTORY: dict[tuple[str, str], deque[tuple[datetime, float]]] = defaultdict(
    lambda: deque(maxlen=3600)
)


def normalize_throughput_mbps(value: Any) -> tuple[float | None, str]:
    """Normalize supported throughput units and reject ambiguous values."""
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        number = float(value)
        return (number if math.isfinite(number) and number >= 0 else None, "known")

    if isinstance(value, str):
        text = value.strip().lower()
        match = re.fullmatch(r"([+-]?(?:\d+(?:\.\d*)?|\.\d+))\s*(mbps|mb/s|mbit/s|kbps|kb/s|gbps|gb/s)", text)
        if not match:
            return None, "ambiguous"
        number = float(match.group(1))
        unit = match.group(2)
        if unit.startswith("k"):
            number /= 1000.0
        elif unit.startswith("g"):
            number *= 1000.0
        if math.isfinite(number) and number >= 0:
            return number, "known"
    return None, "ambiguous"


def normalize_payload(payload: dict[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Return a quality-safe copy and quality events detected in the payload."""
    normalized = json.loads(json.dumps(payload))
    events: list[dict[str, Any]] = []
    quality_events = normalized.get("quality_events") or []
    data = normalized.get("data") or {}
    device_id = normalized.get("device_id")
    device_type = normalized.get("device_type")
    building_id = normalized.get("building_id")
    timestamp = normalized.get("timestamp")

    if "counter_reset" in quality_events:
        events.append({
            "event_type": "counter_reset",
            "device_id": device_id,
            "building_id": building_id,
            "timestamp": timestamp,
            "status": "captured",
        })
        if device_type == "switch":
            data["errors"] = 0
            data["uptime_s"] = 0
    elif device_type == "switch" and data.get("errors") == 0 and data.get("uptime_s") == 0:
        events.append({
            "event_type": "counter_reset",
            "device_id": device_id,
            "building_id": building_id,
            "timestamp": timestamp,
            "status": "captured",
        })

    if "missing_heartbeat" in quality_events:
        events.append({
            "event_type": "missing_heartbeat",
            "device_id": device_id,
            "building_id": building_id,
            "timestamp": timestamp,
            "status": "captured",
        })

    if device_type == "switch":
        for field in ("port_in_mbps", "port_out_mbps"):
            if field in data:
                value, unit_status = normalize_throughput_mbps(data[field])
                if unit_status == "known":
                    data[field] = value
                else:
                    data[field] = None
                    events.append({
                        "event_type": "inconsistent_unit",
                        "device_id": device_id,
                        "building_id": building_id,
                        "timestamp": timestamp,
                        "status": "captured",
                        "detail": f"ambiguous or unsupported unit in {field}",
                    })
        if "inconsistent_unit" in quality_events and not any(
            event["event_type"] == "inconsistent_unit" for event in events
        ):
            events.append({
                "event_type": "inconsistent_unit",
                "device_id": device_id,
                "building_id": building_id,
                "timestamp": timestamp,
                "status": "captured",
                "detail": "inconsistent throughput unit",
            })

    normalized["quality_status"] = "needs_review" if events else "valid"
    normalized["quality_events"] = sorted({
        event["event_type"] for event in events
    })
    normalized["data"] = data
    return normalized, events


def calculate_moving_average_band(values: Iterable[float]) -> tuple[float, float, float]:
    """Calculate mean +/- 3 standard deviations over a one-hour sample."""
    samples = [float(value) for value in values]
    if not samples:
        return 0.0, 0.0, 0.0
    mean = statistics.fmean(samples)
    deviation = statistics.stdev(samples) if len(samples) > 1 else 0.0
    lower = mean - 3.0 * deviation
    upper = mean + 3.0 * deviation
    return mean, lower, upper


def find_traffic_anomalies(
    traffic_samples: dict[tuple[str, str], Iterable[float]],
    current_values: dict[tuple[str, str], float],
) -> list[dict[str, Any]]:
    anomalies: list[dict[str, Any]] = []
    for key, current in current_values.items():
        _mean, lower, upper = calculate_moving_average_band(traffic_samples.get(key, []))
        if current < lower or current > upper:
            anomalies.append({
                "building_id": key[0],
                "device_id": key[1],
                "actual_value": current,
                "lower_bound": lower,
                "upper_bound": upper,
            })
    return anomalies


def offline_devices(
    heartbeats: dict[str, datetime],
    now: datetime,
    threshold_seconds: float = 120.0,
) -> list[str]:
    return [
        device_id
        for device_id, last_seen in heartbeats.items()
        if (now - last_seen).total_seconds() > threshold_seconds
    ]


def percentile(values: Iterable[float], percentile_value: float) -> float:
    samples = sorted(float(value) for value in values)
    if not samples:
        return 0.0
    if len(samples) == 1:
        return samples[0]
    rank = max(0, min(len(samples) - 1, math.ceil(percentile_value * len(samples)) - 1))
    return samples[rank]


def evaluate_sla_alerts(
    building_id: str,
    latency_p95_ms: float,
    packet_loss_pct: float,
) -> list[dict[str, Any]]:
    alerts: list[dict[str, Any]] = []
    if latency_p95_ms > 100.0:
        alerts.append({
            "alert_type": "latency_p95",
            "building_id": building_id,
            "condition": "latency_p95_ms > 100",
            "latency_p95_ms": latency_p95_ms,
        })
    if packet_loss_pct > 2.0:
        alerts.append({
            "alert_type": "packet_loss",
            "building_id": building_id,
            "condition": "packet_loss_pct > 2",
            "packet_loss_pct": packet_loss_pct,
        })
    return alerts


if StructType is not None:
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
                        StructField("port_in_mbps", StringType(), True),
                        StructField("port_out_mbps", StringType(), True),
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
            StructField("quality_events", ArrayType(StringType()), True),
            StructField("run_metadata", MapType(StringType(), StringType()), True),
            StructField("message_type", StringType(), True),
        ]
    )
else:
    PAYLOAD_SCHEMA = None


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
        quality_events: list[dict[str, Any]] = []
        injected_counts = {quality_type: 0 for quality_type in QUALITY_TYPES}
        captured_counts = {quality_type: 0 for quality_type in QUALITY_TYPES}
        traffic_points: dict[tuple[str, str], float] = {}
        probe_points: dict[str, list[float]] = defaultdict(list)
        packet_loss_points: dict[str, list[float]] = defaultdict(list)
        one_minute: dict[tuple[str, str], list[float]] = defaultdict(list)
        wifi_points: dict[str, list[float]] = defaultdict(list)
        traffic_events: dict[tuple[str, str], tuple[datetime, float]] = {}
        now = datetime.now(timezone.utc)

        for row in rows:
            payload = json.loads(row.raw_payload)
            for quality_type in payload.get("quality_events", []):
                if quality_type in injected_counts:
                    injected_counts[quality_type] += 1
            normalized, row_events = normalize_payload(payload)
            quality_events.extend(row_events)
            for event in row_events:
                event_type = event["event_type"]
                if event_type in captured_counts:
                    captured_counts[event_type] += 1
            data = normalized.get("data") or {}

            event_timestamp = normalized.get("timestamp")
            try:
                event_time = datetime.fromisoformat(event_timestamp.replace("Z", "+00:00"))
            except (AttributeError, TypeError, ValueError):
                event_time = now
            device_id = normalized.get("device_id")
            missing_heartbeat = (
                normalized.get("message_type") == "missing_heartbeat"
                or "missing_heartbeat" in normalized.get("quality_events", [])
            )
            if device_id and not missing_heartbeat:
                _HEARTBEATS[device_id] = max(
                    _HEARTBEATS.get(device_id, event_time),
                    event_time,
                )

            if normalized.get("device_type") == "switch":
                port_in = data.get("port_in_mbps")
                if isinstance(port_in, (int, float)):
                    key = (normalized["building_id"], device_id)
                    traffic_points[key] = float(port_in)
                    traffic_events[key] = (event_time, float(port_in))
                    one_minute[(normalized["building_id"], "port_in_mbps")].append(float(port_in))
                    one_minute[(normalized["building_id"], "port_out_mbps")].append(
                        float(data.get("port_out_mbps") or 0)
                    )
            elif normalized.get("device_type") == "access_point":
                clients = data.get("clients")
                if isinstance(clients, (int, float)):
                    wifi_points[normalized["building_id"]].append(float(clients))
            elif normalized.get("device_type") == "latency_probe":
                latency = data.get("latency_ms")
                loss = data.get("packet_loss_pct")
                if isinstance(latency, (int, float)):
                    probe_points[normalized["building_id"]].append(float(latency))
                if isinstance(loss, (int, float)):
                    packet_loss_points[normalized["building_id"]].append(float(loss))

            documents.append(
                {
                    "device_id": row.device_id,
                    "device_type": row.device_type,
                    "building_id": row.building_id,
                    "payload": payload,
                    "received_at": received_at,
                    "quality_status": normalized["quality_status"],
                    "quality_events": normalized["quality_events"],
                    "normalized_payload": normalized,
                }
            )
        if documents:
            database.telemetry_raw.insert_many(documents)

        minute_documents = []
        for (building_id, metric), values in one_minute.items():
            minute_documents.append({
                "building_id": building_id,
                "metric": metric,
                "value": statistics.fmean(values),
                "timestamp": received_at,
                "window": "1-minute",
            })
        for building_id, values in wifi_points.items():
            minute_documents.append({
                "building_id": building_id,
                "metric": "clients",
                "value": statistics.fmean(values),
                "timestamp": received_at,
                "window": "1-minute",
            })
        if minute_documents:
            database.realtime_metrics.insert_many(minute_documents)

        alerts = []
        for building_id, latencies in probe_points.items():
            p95 = percentile(latencies, 0.95)
            losses = packet_loss_points.get(building_id, [])
            packet_loss = statistics.fmean(losses) if losses else 0.0
            database.sla_windows.insert_one({
                "building_id": building_id,
                "window": "5-minute",
                "timestamp": received_at,
                "latency_p95_ms": p95,
                "packet_loss_pct": packet_loss,
            })
            for alert in evaluate_sla_alerts(building_id, p95, packet_loss):
                alert["timestamp"] = received_at
                alerts.append(alert)
        if alerts:
            database.alerts.insert_many(alerts)

        for event in quality_events:
            event["captured_at"] = received_at
        if quality_events:
            database.quality_events.insert_many(quality_events)
        database.quality_reports.insert_one({
            "timestamp": received_at,
            "injected": injected_counts,
            "captured": captured_counts,
        })

        traffic_samples = {
            key: [value for _timestamp, value in history]
            for key, history in _TRAFFIC_HISTORY.items()
        }
        anomalies = find_traffic_anomalies(traffic_samples, traffic_points)
        if anomalies:
            database.traffic_anomalies.insert_many([
                {
                    **anomaly,
                    "timestamp": received_at,
                    "event_type": "traffic_anomaly",
                }
                for anomaly in anomalies
            ])
        for key, (event_time, value) in traffic_events.items():
            cutoff = event_time.timestamp() - 3600
            history = _TRAFFIC_HISTORY[key]
            while history and history[0][0].timestamp() < cutoff:
                history.popleft()
            history.append((event_time, value))

        offline = offline_devices(_HEARTBEATS, now)
        if offline:
            database.alerts.insert_many([
                {
                    "alert_type": "heartbeat_offline",
                    "device_id": device_id,
                    "timestamp": received_at,
                    "condition": "no heartbeat for more than 120 seconds",
                    "status": "offline",
                }
                for device_id in offline
            ])


def main() -> None:
    if SparkSession is None:
        raise SystemExit("PySpark is required to start campus streaming")
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
