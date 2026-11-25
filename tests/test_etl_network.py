import importlib.util
import pathlib
import sys


MODULE_PATH = pathlib.Path(__file__).parents[1] / "src" / "etl" / "etl_network.py"
SPEC = importlib.util.spec_from_file_location("etl_network", MODULE_PATH)
etl = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(etl)
sys.path.insert(0, str(MODULE_PATH.parents[1] / "dashboard"))
from app import app as dashboard_app


def test_normalizes_kbps():
    payload = {
        "device_id": "SW-01",
        "device_type": "switch",
        "building_id": "GEDUNG-A",
        "timestamp": "2026-11-24T10:00:00Z",
        "data": {"traffic_mbps": "1200kbps"},
    }
    cleaned, issues = etl.clean_payload(payload)
    assert cleaned["data"]["traffic_mbps"] == 1.2
    assert "unit_normalized" in issues


def test_rejects_ambiguous_unit():
    payload = {
        "device_id": "SW-01",
        "device_type": "switch",
        "building_id": "GEDUNG-A",
        "timestamp": "2026-11-24T10:00:00Z",
        "data": {"traffic_mbps": "unknown"},
    }
    cleaned, issues = etl.clean_payload(payload)
    assert cleaned["data"].get("traffic_mbps") is None
    assert "ambiguous_unit" in issues


def test_requires_device_and_building():
    payload = {
        "device_id": None,
        "device_type": "switch",
        "building_id": None,
        "timestamp": "2026-11-24T10:00:00Z",
        "data": {},
    }
    cleaned, issues = etl.clean_payload(payload)
    assert cleaned is None
    assert "missing" in issues


def test_accepts_network_variant_payload():
    payload = {
        "device_id": "SW-01",
        "device_type": "switch",
        "building_id": "GEDUNG-A",
        "timestamp": "2026-11-24T10:00:00Z",
        "data": {
            "port_in_mbps": "1200kbps",
            "port_out_mbps": 2.5,
            "errors": 3,
            "uptime_s": 600,
        },
    }
    cleaned, issues = etl.clean_payload(payload)
    assert cleaned["data"]["traffic_mbps"] == 3.7
    assert cleaned["data"]["errors"] == 3
    assert "unit_normalized" in issues


def test_skips_probe_target_and_normalizes_units():
    payload = {
        "device_id": "PROBE-01",
        "device_type": "latency_probe",
        "building_id": "GEDUNG-A",
        "timestamp": "2026-11-24T10:00:00Z",
        "data": {
            "latency_ms": "45ms",
            "packet_loss_pct": "0.5%",
            "target": "gateway.local",
        },
    }
    cleaned, issues = etl.clean_payload(payload)
    assert cleaned["data"]["latency_ms"] == 45
    assert cleaned["data"]["packet_loss_pct"] == 0.5
    assert "target" not in cleaned["data"]
    assert not issues


def test_stream_alert_endpoint_requires_warehouse_fields():
    response = dashboard_app.test_client().post(
        "/api/stream/alerts",
        json={"building_id": "GEDUNG-A", "message": "test"},
    )
    assert response.status_code == 400
    assert "alert_code" in response.get_json()["error"]
