import os
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from pymongo import MongoClient

POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://pid:pidpassword@localhost:5432/network_warehouse")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://pid:pidpassword@localhost:27018/network_telemetry?authSource=admin")
MONGO_DATABASE = os.getenv("MONGO_DATABASE", "network_telemetry")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "raw_network_metrics")
MONGO_ALERT_COLLECTION = os.getenv("MONGO_ALERT_COLLECTION", "streaming_alerts")


def clean_payload(payload):
    issues = []
    data = payload.get("data") or {}
    timestamp = payload.get("timestamp")

    if not payload.get("device_id") or not payload.get("building_id"):
        issues.append("missing")
        return None, issues

    try:
        if not isinstance(timestamp, datetime):
            timestamp = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
    except (TypeError, ValueError):
        issues.append("format")
        return None, issues

    normalized = {}
    for key, value in data.items():
        if value is None:
            continue
        if key == "target":
            continue
        if isinstance(value, str):
            text = value.strip().lower()
            try:
                if key in {"traffic_mbps", "port_in_mbps", "port_out_mbps"}:
                    if text.endswith("mbps"):
                        normalized[key] = float(text[:-4])
                    elif text.endswith("kbps"):
                        normalized[key] = float(text[:-4]) / 1000.0
                        issues.append("unit_normalized")
                    elif text.endswith("bps"):
                        normalized[key] = float(text[:-3]) / 1_000_000.0
                        issues.append("unit_normalized")
                    else:
                        normalized[key] = float(text)
                elif key == "latency_ms":
                    normalized[key] = float(text.removesuffix("ms").strip())
                elif key in {"packet_loss_pct", "channel_util_pct"}:
                    normalized[key] = float(text.removesuffix("%").strip())
                elif key == "rssi_avg_dbm":
                    normalized[key] = float(text.removesuffix("dbm").strip())
                else:
                    normalized[key] = float(text)
            except ValueError:
                issues.append("ambiguous_unit" if key in {
                    "traffic_mbps", "port_in_mbps", "port_out_mbps"
                } else "format")
        elif isinstance(value, (int, float)):
            normalized[key] = float(value)

    if "traffic_mbps" not in normalized:
        port_values = [
            normalized[key]
            for key in ("port_in_mbps", "port_out_mbps")
            if key in normalized
        ]
        if port_values:
            normalized["traffic_mbps"] = sum(port_values)

    for key in ("traffic_mbps", "port_in_mbps", "port_out_mbps"):
        if normalized.get(key) is not None and normalized[key] < 0:
            issues.append("out_of_range")
            normalized.pop(key, None)
    for key in ("packet_loss_pct", "channel_util_pct"):
        if normalized.get(key) is not None and not 0 <= normalized[key] <= 100:
            issues.append("out_of_range")
            normalized.pop(key, None)
    return {"timestamp": timestamp, "device_id": payload["device_id"], "building_id": payload["building_id"],
            "device_type": payload.get("device_type"), "data": normalized}, issues


def seed_mongo(count=24):
    client = MongoClient(MONGODB_URI)
    collection = client[MONGO_DATABASE][MONGO_COLLECTION]
    collection.delete_many({"seed": True})
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    documents = []
    quality_inserted = {"missing": 0, "counter_reset": 0, "unit_normalized": 0, "ambiguous_unit": 0}

    for day in range(7):
        hour_start = now - timedelta(days=7-day)
        for hour in range(24):
            ts = hour_start + timedelta(hours=hour)
            hour_of_day = ts.hour
            for i in range(count):
                building = "GEDUNG-A" if i < 12 else "GEDUNG-B"
                device_type = "switch" if i < 8 else ("access_point" if i < 20 else "latency_probe")
                device_id = f"{device_type[:2].upper()}-{i+1:02d}"
                campus_load = 1.0 if 8 <= hour_of_day <= 16 else 0.45
                traffic = random.uniform(20, 55) * campus_load
                if hour_of_day == 10 and building == "GEDUNG-B" and day == 6:
                    traffic *= 2.4
                latency = random.uniform(18, 48)
                loss = random.uniform(0.1, 1.1)
                if (day == 6 and building == "GEDUNG-B" and hour_of_day == 10):
                    latency = random.uniform(105, 180)
                    loss = random.uniform(2.2, 4.5)

                data = {
                    "traffic_mbps": traffic,
                    "clients": random.randint(25, 180) if device_type == "access_point" else None,
                    "latency_ms": latency if device_type == "latency_probe" else None,
                    "packet_loss_pct": loss if device_type == "latency_probe" else None,
                }
                quality_status = "valid"
                if i == 2 and hour == 12:
                    data["traffic_mbps"] = "1200kbps"
                    quality_status = "unit_normalized"
                    quality_inserted["unit_normalized"] += 1
                elif i == 5 and hour == 15:
                    data["traffic_mbps"] = "unknown"
                    quality_status = "ambiguous_unit"
                    quality_inserted["ambiguous_unit"] += 1
                elif i == 7 and hour == 18:
                    data["errors"] = 0
                    quality_status = "counter_reset"
                    quality_inserted["counter_reset"] += 1

                documents.append({
                    "device_id": device_id,
                    "device_type": device_type,
                    "building_id": building,
                    "timestamp": ts,
                    "data": data,
                    "quality_status": quality_status,
                    "seed": True,
                })
    collection.insert_many(documents)
    return len(documents), quality_inserted


def load_warehouse():
    mongo = MongoClient(MONGODB_URI)
    collection = mongo[MONGO_DATABASE][MONGO_COLLECTION]
    alert_collection = mongo[MONGO_DATABASE][MONGO_ALERT_COLLECTION]
    rows = list(collection.find({"seed": {"$ne": True}}))
    if not rows:
        rows = list(collection.find({"seed": True}))
    if not rows:
        seed_mongo()
        rows = list(collection.find({"seed": True}))
    if rows:
        quality_inserted = {"missing": 0, "counter_reset": 0, "unit_normalized": 0, "ambiguous_unit": 0}
        for row in rows:
            status = row.get("quality_status")
            if status in quality_inserted:
                quality_inserted[status] += 1
    else:
        raise RuntimeError(f"No payloads found in MongoDB collection {MONGO_DATABASE}.{MONGO_COLLECTION}")

    cleaned = []
    caught = {key: 0 for key in quality_inserted}
    for row in rows:
        payload, issues = clean_payload(row)
        if payload:
            cleaned.append(payload)
        for issue in issues:
            caught[issue] = caught.get(issue, 0) + 1

    for row in rows:
        status = row.get("quality_status")
        if status == "counter_reset" and status in caught:
            caught[status] += 1

    if not cleaned:
        raise RuntimeError("No valid telemetry payloads were found; warehouse load was aborted")

    stream_alerts = []
    for document in alert_collection.find({}):
        timestamp = document.get("timestamp")
        try:
            if not isinstance(timestamp, datetime):
                timestamp = datetime.fromisoformat(str(timestamp).replace("Z", "+00:00"))
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
            timestamp = timestamp.astimezone(timezone.utc).replace(tzinfo=None)
        except (TypeError, ValueError) as error:
            raise RuntimeError(f"Invalid timestamp in streaming alert {document.get('_id')}") from error

        building_id = document.get("building_id")
        alert_code = document.get("alert_code") or document.get("alert_type")
        message = document.get("message")
        if not building_id or not alert_code or not message:
            raise RuntimeError(
                f"Streaming alert {document.get('_id')} must contain building_id, alert_code, and message"
            )
        stream_alerts.append({
            "timestamp": timestamp,
            "hour": timestamp.replace(minute=0, second=0, microsecond=0),
            "building_id": str(building_id),
            "device_id": document.get("device_id"),
            "device_type": document.get("device_type") or "unknown",
            "alert_code": str(alert_code).upper(),
            "severity": str(document.get("severity") or "warning").lower(),
            "message": str(message),
        })

    frame = pd.DataFrame([
        {
            "timestamp": item["timestamp"],
            "building_id": item["building_id"],
            "device_id": item["device_id"],
            "device_type": item["device_type"],
            "traffic_mbps": item["data"].get("traffic_mbps"),
            "clients": item["data"].get("clients"),
            "latency_ms": item["data"].get("latency_ms"),
            "packet_loss_pct": item["data"].get("packet_loss_pct"),
        }
        for item in cleaned
    ])

    conn = psycopg2.connect(POSTGRES_DSN)
    conn.autocommit = False
    cur = conn.cursor()
    cur.execute("TRUNCATE fact_alert, fact_sla_window, fact_device_metric_hourly, fact_data_quality, dim_time, dim_device, dim_building, dim_alert_type CASCADE")

    building_ids = sorted(set(frame["building_id"].dropna()) | {
        alert["building_id"] for alert in stream_alerts
    })
    buildings = [(building_id, building_id.replace("-", " ").replace("_", " ").title())
                 for building_id in building_ids]
    execute_values(cur, "INSERT INTO dim_building (building_id, building_name) VALUES %s", buildings)
    cur.execute("SELECT building_key, building_id FROM dim_building")
    building_keys = dict((building_id, key) for key, building_id in cur.fetchall())

    devices = frame[["device_id", "device_type", "building_id"]].drop_duplicates()
    alert_devices = pd.DataFrame([
        (alert["device_id"], alert["device_type"], alert["building_id"])
        for alert in stream_alerts
        if alert["device_id"]
    ], columns=["device_id", "device_type", "building_id"])
    if not alert_devices.empty:
        devices = pd.concat([devices, alert_devices], ignore_index=True).drop_duplicates(
            subset=["device_id"], keep="first"
        )
    execute_values(cur, "INSERT INTO dim_device (device_id, device_type, building_id) VALUES %s",
                   list(devices.itertuples(index=False, name=None)))
    cur.execute("SELECT device_key, device_id FROM dim_device")
    device_keys = dict((device_id, key) for key, device_id in cur.fetchall())
    missing_devices = [
        (row.device_id, row.device_type, row.building_id)
        for row in devices.itertuples(index=False)
        if row.device_id not in device_keys
    ]
    if missing_devices:
        execute_values(
            cur,
            "INSERT INTO dim_device (device_id, device_type, building_id) VALUES %s "
            "ON CONFLICT (device_id) DO NOTHING",
            missing_devices,
        )
        cur.execute("SELECT device_key, device_id FROM dim_device")
        device_keys = dict((device_id, key) for key, device_id in cur.fetchall())

    alert_type_names = {
        "LATENCY_P95": "Latensi P95 melewati 100 ms",
        "PACKET_LOSS": "Packet loss melewati 2%",
        "TRAFFIC_ANOMALY": "Trafik di luar moving-average band",
        "HEARTBEAT": "Device kehilangan heartbeat",
    }
    for alert in stream_alerts:
        alert_type_names.setdefault(alert["alert_code"], alert["alert_code"].replace("_", " ").title())
    for code, name in alert_type_names.items():
        cur.execute(
            "INSERT INTO dim_alert_type (alert_code, alert_name) "
            "VALUES (%s, %s) ON CONFLICT (alert_code) "
            "DO UPDATE SET alert_name = EXCLUDED.alert_name",
            (code, name),
        )
    cur.execute("SELECT alert_type_key, alert_code FROM dim_alert_type")
    alert_keys = dict((code, key) for key, code in cur.fetchall())

    frame["hour"] = frame["timestamp"].apply(
        lambda value: value.replace(minute=0, second=0, microsecond=0, tzinfo=None)
    )
    alert_hours = {alert["hour"] for alert in stream_alerts}
    all_hours = sorted(set(frame["hour"].unique()) | alert_hours)
    execute_values(cur,
                   "INSERT INTO dim_time (full_timestamp, date_value, hour_value, day_name) VALUES %s "
                   "ON CONFLICT (full_timestamp) DO NOTHING",
                   [(hour, hour.date(), hour.hour, hour.strftime("%A")) for hour in all_hours])
    cur.execute("SELECT time_key, full_timestamp FROM dim_time")
    time_keys = dict((value, key) for key, value in cur.fetchall())

    hourly = frame.groupby(["hour", "building_id", "device_id"], dropna=False).agg(
        avg_traffic_mbps=("traffic_mbps", "mean"),
        max_traffic_mbps=("traffic_mbps", "max"),
        avg_clients=("clients", "mean"),
        avg_latency_ms=("latency_ms", "mean"),
        avg_packet_loss_pct=("packet_loss_pct", "mean"),
    ).reset_index()

    metric_rows = []
    for row in hourly.itertuples(index=False):
        if row.device_id not in device_keys:
            cur.execute(
                "INSERT INTO dim_device (device_id, device_type, building_id) "
                "VALUES (%s, %s, %s) ON CONFLICT (device_id) DO UPDATE "
                "SET device_type = EXCLUDED.device_type, building_id = EXCLUDED.building_id "
                "RETURNING device_key",
                (row.device_id, str(row.device_id).split("-", 1)[0].lower(), row.building_id),
            )
            device_keys[row.device_id] = cur.fetchone()[0]
        metric_rows.append((
            time_keys[row.hour],
            building_keys[row.building_id],
            device_keys[row.device_id],
            row.avg_traffic_mbps if pd.notna(row.avg_traffic_mbps) else None,
            row.max_traffic_mbps if pd.notna(row.max_traffic_mbps) else None,
            row.avg_clients if pd.notna(row.avg_clients) else None,
            row.avg_latency_ms if pd.notna(row.avg_latency_ms) else None,
            row.avg_packet_loss_pct if pd.notna(row.avg_packet_loss_pct) else None,
            caught["ambiguous_unit"],
        ))
    execute_values(cur,
                   "INSERT INTO fact_device_metric_hourly "
                   "(time_key, building_key, device_key, avg_traffic_mbps, max_traffic_mbps, "
                   "avg_clients, avg_latency_ms, avg_packet_loss_pct, quality_rejected_count) "
                   "VALUES %s",
                   metric_rows)

    sla = frame.groupby(["hour", "building_id"]).agg(
        latency_p95_ms=("latency_ms", lambda values: values.quantile(0.95)),
        packet_loss_pct=("packet_loss_pct", "mean"),
    ).reset_index()
    sla_rows = []
    alert_rows = [
        (
            time_keys[alert["hour"]],
            building_keys[alert["building_id"]],
            device_keys.get(alert["device_id"]),
            alert_keys[alert["alert_code"]],
            alert["severity"],
            alert["message"],
        )
        for alert in stream_alerts
    ]
    for row in sla.itertuples(index=False):
        latency = row.latency_p95_ms if pd.notna(row.latency_p95_ms) else None
        loss = row.packet_loss_pct if pd.notna(row.packet_loss_pct) else None
        compliant = latency is not None and loss is not None and latency <= 100 and loss <= 2
        sla_rows.append((
            time_keys[row.hour],
            building_keys[row.building_id],
            latency,
            loss,
            100.0 if compliant else (0.0 if latency is not None and loss is not None else None),
            compliant,
        ))
        if latency is not None and latency > 100:
            alert_rows.append((time_keys[row.hour], building_keys[row.building_id], None,
                               alert_keys["LATENCY_P95"], "high",
                               f"P95 latensi gedung {row.building_id} mencapai {latency:.1f} ms"))
        if loss is not None and loss > 2:
            alert_rows.append((time_keys[row.hour], building_keys[row.building_id], None,
                               alert_keys["PACKET_LOSS"], "high",
                               f"Packet loss gedung {row.building_id} mencapai {loss:.2f}%"))
    execute_values(cur,
                   "INSERT INTO fact_sla_window "
                   "(time_key, building_key, latency_p95_ms, packet_loss_pct, sla_compliance_pct, compliant) "
                   "VALUES %s",
                   sla_rows)
    if alert_rows:
        execute_values(cur,
                       "INSERT INTO fact_alert "
                       "(time_key, building_key, device_key, alert_type_key, severity, message) "
                       "VALUES %s",
                       alert_rows)

    quality_rows = []
    for issue_type, inserted in quality_inserted.items():
        caught_count = caught.get(issue_type, 0)
        quality_rows.append((time_keys[max(time_keys, key=lambda key: time_keys[key])], issue_type,
                             inserted, caught_count))
    execute_values(cur,
                   "INSERT INTO fact_data_quality (time_key, issue_type, inserted_count, caught_count) VALUES %s",
                   quality_rows)

    conn.commit()
    cur.close()
    conn.close()
    return len(cleaned), quality_inserted, caught


if __name__ == "__main__":
    count, inserted, caught = load_warehouse()
    print({"cleaned_rows": count, "inserted": inserted, "caught": caught})
