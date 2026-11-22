import json
import os
import signal
import time
from datetime import datetime, timezone
from typing import Any


def build_raw_document(
    payload: dict[str, Any],
    *,
    mqtt_topic: str,
    received_at: datetime,
) -> dict[str, Any]:
    if received_at.tzinfo is None:
        received_at = received_at.replace(tzinfo=timezone.utc)

    return {
        "device_id": payload["device_id"],
        "device_type": payload["device_type"],
        "building_id": payload["building_id"],
        "payload": payload,
        "mqtt_topic": mqtt_topic,
        "received_at": received_at.astimezone(timezone.utc)
        .isoformat(timespec="seconds")
        .replace("+00:00", "Z"),
        "quality_status": "pending",
    }


def build_kafka_record(
    payload: dict[str, Any],
    *,
    mqtt_topic: str,
    kafka_topic: str,
) -> tuple[str, dict[str, Any], str]:
    del mqtt_topic
    return payload["device_id"], payload, kafka_topic


def run_bridge() -> None:
    try:
        import paho.mqtt.client as mqtt
        from kafka import KafkaProducer
    except ImportError as exc:
        raise SystemExit("Dependensi bridge belum terpasang") from exc

    mqtt_host = os.getenv("MQTT_HOST", "mqtt")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    kafka_bootstrap = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
    kafka_topic = os.getenv("KAFKA_TOPIC", "campus.telemetry")

    producer = KafkaProducer(
        bootstrap_servers=kafka_bootstrap,
        value_serializer=lambda value: json.dumps(value).encode("utf-8"),
        key_serializer=lambda value: value.encode("utf-8") if value else None,
    )

    def on_message(_client: Any, _userdata: Any, message: Any) -> None:
        try:
            payload = json.loads(message.payload.decode("utf-8"))
            key, value, topic = build_kafka_record(
                payload,
                mqtt_topic=message.topic,
                kafka_topic=kafka_topic,
            )
            producer.send(topic, key=key, value=value)
        except (json.JSONDecodeError, KeyError, UnicodeDecodeError):
            return

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.on_message = on_message
    client.connect(mqtt_host, mqtt_port, keepalive=60)
    client.subscribe("campus/telemetry/#", qos=1)
    client.loop_start()

    stopping = False

    def stop(_signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)

    try:
        while not stopping:
            producer.flush(timeout=1)
            time.sleep(1)
    finally:
        producer.flush()
        producer.close()
        client.loop_stop()
        client.disconnect()
