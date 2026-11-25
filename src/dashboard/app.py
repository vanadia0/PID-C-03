import os
from datetime import datetime, timezone

import psycopg2
from flask import Flask, jsonify, render_template, request
from pymongo import MongoClient

app = Flask(__name__)
POSTGRES_DSN = os.getenv("POSTGRES_DSN", "postgresql://pid:pidpassword@localhost:5432/network_warehouse")
MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://pid:pidpassword@localhost:27017/network_telemetry?authSource=admin")


def query(sql, params=None):
    with psycopg2.connect(POSTGRES_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            columns = [desc[0] for desc in cur.description]
            return [dict(zip(columns, row)) for row in cur.fetchall()]


@app.get("/")
def index():
    return render_template("index.html")


@app.get("/api/status")
def status():
    checks = {
        "mongo": False,
        "warehouse": False,
    }
    try:
        mongo = MongoClient(MONGODB_URI, serverSelectionTimeoutMS=2000)
        mongo.admin.command("ping")
        checks["mongo"] = True
    except Exception:
        pass
    try:
        query("SELECT 1")
        checks["warehouse"] = True
    except Exception:
        pass
    return jsonify({"status": "online" if all(checks.values()) else "degraded", "checks": checks})


@app.post("/api/ingest")
def ingest():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "JSON payload wajib"}), 400
    required = {"device_id", "device_type", "building_id", "timestamp", "data"}
    missing = required - payload.keys()
    if missing:
        return jsonify({"error": f"Field wajib hilang: {', '.join(sorted(missing))}"}), 400
    payload["received_at"] = datetime.now(timezone.utc)
    payload.setdefault("quality_status", "valid")
    mongo = MongoClient(MONGODB_URI)
    result = mongo.network_telemetry.raw_network_metrics.insert_one(payload)
    return jsonify({"inserted_id": str(result.inserted_id)})


@app.post("/api/stream/alerts")
def ingest_stream_alert():
    payload = request.get_json(silent=True)
    if not payload:
        return jsonify({"error": "JSON payload wajib"}), 400
    required = {"timestamp", "building_id", "alert_code", "message"}
    missing = required - payload.keys()
    if missing:
        return jsonify({"error": f"Field wajib hilang: {', '.join(sorted(missing))}"}), 400
    try:
        timestamp = datetime.fromisoformat(str(payload["timestamp"]).replace("Z", "+00:00"))
    except ValueError:
        return jsonify({"error": "timestamp harus berupa ISO-8601"}), 400

    payload["timestamp"] = timestamp
    payload["received_at"] = datetime.now(timezone.utc)
    mongo = MongoClient(MONGODB_URI)
    result = mongo.network_telemetry.streaming_alerts.insert_one(payload)
    return jsonify({"inserted_id": str(result.inserted_id)}), 201


@app.get("/api/sla")
def sla():
    return jsonify(query("""
        SELECT b.building_name, t.date_value,
               ROUND(AVG(f.sla_compliance_pct)::numeric, 2) AS compliance_pct,
               ROUND(MAX(f.latency_p95_ms)::numeric, 2) AS latency_p95_ms,
               ROUND(MAX(f.packet_loss_pct)::numeric, 3) AS packet_loss_pct
        FROM fact_sla_window f
        JOIN dim_building b ON b.building_key = f.building_key
        JOIN dim_time t ON t.time_key = f.time_key
        GROUP BY b.building_name, t.date_value
        ORDER BY t.date_value, b.building_name
    """))


@app.get("/api/buildings")
def buildings():
    return jsonify(query("""
        SELECT b.building_name,
               ROUND(AVG(f.latency_p95_ms)::numeric, 2) AS avg_latency_p95_ms,
               ROUND(AVG(f.packet_loss_pct)::numeric, 3) AS avg_packet_loss_pct,
               ROUND(AVG(f.sla_compliance_pct)::numeric, 2) AS avg_compliance_pct
        FROM fact_sla_window f
        JOIN dim_building b ON b.building_key = f.building_key
        GROUP BY b.building_name
        ORDER BY avg_compliance_pct ASC
    """))


@app.get("/api/alerts")
def alerts():
    return jsonify(query("""
        SELECT f.alert_id, b.building_name, d.alert_name, f.severity,
               f.message, t.full_timestamp
        FROM fact_alert f
        JOIN dim_building b ON b.building_key = f.building_key
        JOIN dim_alert_type d ON d.alert_type_key = f.alert_type_key
        JOIN dim_time t ON t.time_key = f.time_key
        ORDER BY t.full_timestamp DESC
        LIMIT 100
    """))


@app.get("/api/quality")
def quality():
    return jsonify(query("""
        SELECT issue_type, SUM(inserted_count) AS inserted_count,
               SUM(caught_count) AS caught_count
        FROM fact_data_quality
        GROUP BY issue_type
        ORDER BY issue_type
    """))


@app.get("/api/metrics")
def metrics():
    return jsonify(query("""
        SELECT b.building_name, t.full_timestamp,
               ROUND(AVG(f.avg_traffic_mbps)::numeric, 2) AS avg_traffic_mbps,
               ROUND(AVG(f.avg_clients)::numeric, 2) AS avg_clients
        FROM fact_device_metric_hourly f
        JOIN dim_building b ON b.building_key = f.building_key
        JOIN dim_time t ON t.time_key = f.time_key
        GROUP BY b.building_name, t.full_timestamp
        ORDER BY t.full_timestamp DESC
        LIMIT 200
    """))


@app.post("/api/run-etl")
def run_etl():
    import subprocess
    result = subprocess.run(
        ["python", "/app/etl/etl_network.py"],
        capture_output=True,
        text=True,
        env=os.environ.copy(),
    )
    if result.returncode != 0:
        return jsonify({"error": result.stderr}), 500
    return jsonify({"output": result.stdout})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000)
