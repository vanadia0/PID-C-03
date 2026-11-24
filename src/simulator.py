import json
import random
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def load_inventory(path: str | Path) -> dict[str, Any]:
    with Path(path).open("r", encoding="utf-8") as inventory_file:
        return json.load(inventory_file)


def calculate_load_factor(now: datetime, *, flash_crowd: bool = False) -> float:
    hour = now.astimezone(timezone.utc).hour
    factor = 1.0 if 7 <= hour < 18 else 0.25
    return factor * 1.8 if flash_crowd else factor


@dataclass(frozen=True)
class QualityConfig:
    counter_reset_probability: float = 0.0
    inconsistent_unit_probability: float = 0.0
    missing_heartbeat_probability: float = 0.0

    def __post_init__(self) -> None:
        for name in (
            "counter_reset_probability",
            "inconsistent_unit_probability",
            "missing_heartbeat_probability",
        ):
            if not 0.0 <= getattr(self, name) <= 1.0:
                raise ValueError(f"{name} must be between 0 and 1")


def inject_quality(
    payload: dict[str, Any],
    config: QualityConfig,
    *,
    rng_seed: int | None = None,
) -> tuple[dict[str, Any] | None, list[str]]:
    rng = random.Random(rng_seed)
    events: list[str] = []
    injected = json.loads(json.dumps(payload))

    if rng.random() < config.counter_reset_probability:
        events.append("counter_reset")
        if payload["device_type"] == "switch":
            injected["data"]["errors"] = 0
            injected["data"]["uptime_s"] = 0

    if rng.random() < config.inconsistent_unit_probability:
        events.append("inconsistent_unit")
        data = injected["data"]
        if payload["device_type"] == "switch":
            data["port_in_mbps"] = f"{data['port_in_mbps'] * 1000:.0f} kbps"
        elif payload["device_type"] == "access_point":
            data["channel_util_pct"] = f"{data['channel_util_pct']:.2f} percent"
        else:
            data["latency_ms"] = f"{data['latency_ms']:.2f} ms"

    if rng.random() < config.missing_heartbeat_probability:
        events.append("missing_heartbeat")
        return None, events

    return injected, events


def generate_payload(
    device: dict[str, Any],
    *,
    now: datetime,
    load_factor: float = 1.0,
    rng_seed: int | None = None,
) -> dict[str, Any]:
    rng = random.Random(rng_seed)
    device_type = device["device_type"]

    if device_type == "switch":
        base = rng.uniform(20.0, 60.0) * load_factor
        data = {
            "port_in_mbps": round(base, 2),
            "port_out_mbps": round(base * rng.uniform(0.35, 0.75), 2),
            "errors": rng.randint(0, 3),
            "uptime_s": 86400 + rng.randint(0, 3600),
        }
    elif device_type == "access_point":
        data = {
            "clients": max(0, round(rng.randint(10, 45) * load_factor)),
            "rssi_avg_dbm": round(rng.uniform(-72.0, -48.0), 2),
            "channel_util_pct": round(rng.uniform(15.0, 65.0) * load_factor, 2),
        }
    elif device_type == "latency_probe":
        data = {
            "latency_ms": round(rng.uniform(8.0, 35.0) * load_factor, 2),
            "packet_loss_pct": round(rng.uniform(0.0, 0.8) * load_factor, 2),
            "target": "gateway-campus",
        }
    else:
        raise ValueError(f"Unsupported device type: {device_type}")

    if now.tzinfo is None:
        timestamp = now.isoformat(timespec="seconds") + "Z"
    else:
        timestamp = now.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")

    return {
        "device_id": device["device_id"],
        "device_type": device_type,
        "building_id": device["building_id"],
        "timestamp": timestamp,
        "data": data,
    }


def _env_float(name: str, default: float) -> float:
    return float(os.getenv(name, str(default)))


def run() -> None:
    try:
        import paho.mqtt.client as mqtt
    except ImportError as exc:
        raise SystemExit("Dependensi paho-mqtt belum terpasang") from exc

    inventory_path = Path(os.getenv("INVENTORY_PATH", "/app/config/campus.json"))
    mqtt_host = os.getenv("MQTT_HOST", "mqtt")
    mqtt_port = int(os.getenv("MQTT_PORT", "1883"))
    interval_seconds = float(os.getenv("INTERVAL_SECONDS", "1"))
    flash_crowd = os.getenv("FLASH_CROWD", "false").lower() == "true"
    run_id = os.getenv("RUN_ID") or str(uuid.uuid4())
    quality_config = QualityConfig(
        counter_reset_probability=_env_float("COUNTER_RESET_PROBABILITY", 0.0),
        inconsistent_unit_probability=_env_float("INCONSISTENT_UNIT_PROBABILITY", 0.0),
        missing_heartbeat_probability=_env_float("MISSING_HEARTBEAT_PROBABILITY", 0.0),
    )
    inventory = load_inventory(inventory_path)
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    client.connect(mqtt_host, mqtt_port, keepalive=60)
    client.loop_start()

    try:
        while True:
            now = datetime.now(timezone.utc)
            load_factor = calculate_load_factor(now, flash_crowd=flash_crowd)
            for index, device in enumerate(inventory["devices"]):
                payload = generate_payload(
                    device,
                    now=now,
                    load_factor=load_factor,
                    rng_seed=int(now.timestamp()) + index,
                )
                payload, events = inject_quality(
                    payload,
                    quality_config,
                    rng_seed=int(now.timestamp()) * 100 + index,
                )
                if payload is not None:
                    payload["quality_events"] = events
                    payload["run_metadata"] = {
                        "run_id": run_id,
                        "flash_crowd": str(flash_crowd).lower(),
                    }
                if payload is None:
                    continue
                topic = f"campus/telemetry/{payload['building_id']}/{payload['device_type']}"
                client.publish(topic, json.dumps(payload), qos=1)
            time.sleep(interval_seconds)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    run()
