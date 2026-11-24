import sys
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.streaming import (
    calculate_moving_average_band,
    evaluate_sla_alerts,
    find_traffic_anomalies,
    normalize_payload,
    normalize_throughput_mbps,
    offline_devices,
)


class CampusStreamingTests(unittest.TestCase):
    def test_normalize_throughput_supports_units_and_rejects_ambiguous_values(self):
        self.assertEqual(normalize_throughput_mbps("1500 kbps")[0], 1.5)
        self.assertEqual(normalize_throughput_mbps("2 gbps")[0], 2000.0)
        self.assertEqual(normalize_throughput_mbps("some bps")[1], "ambiguous")

    def test_normalize_payload_captures_reset_unit_and_missing_heartbeat(self):
        payload = {
            "device_id": "SW-B01-01",
            "device_type": "switch",
            "building_id": "B-01",
            "timestamp": "2026-11-23T10:30:00Z",
            "data": {
                "port_in_mbps": "1500 kbps",
                "port_out_mbps": 18.2,
                "errors": 0,
                "uptime_s": 0,
            },
            "quality_events": ["counter_reset", "inconsistent_unit", "missing_heartbeat"],
            "message_type": "missing_heartbeat",
        }

        normalized, events = normalize_payload(payload)

        self.assertEqual(normalized["data"]["port_in_mbps"], 1.5)
        self.assertEqual(normalized["quality_status"], "needs_review")
        self.assertEqual(
            {event["event_type"] for event in events},
            {"counter_reset", "inconsistent_unit", "missing_heartbeat"},
        )

    def test_moving_average_band_detects_values_outside_three_deviations(self):
        mean, lower, upper = calculate_moving_average_band([10.0] * 10 + [12.0])
        self.assertGreater(upper, lower)

        anomalies = find_traffic_anomalies(
            {("B-01", "SW-B01-01"): [10.0] * 20},
            {("B-01", "SW-B01-01"): 100.0},
        )
        self.assertEqual(len(anomalies), 1)
        self.assertEqual(anomalies[0]["building_id"], "B-01")
        self.assertEqual(anomalies[0]["device_id"], "SW-B01-01")
        self.assertEqual(anomalies[0]["lower_bound"], 10.0)
        self.assertEqual(anomalies[0]["upper_bound"], 10.0)

    def test_offline_devices_requires_more_than_two_minutes(self):
        now = datetime(2026, 11, 23, 10, 30, tzinfo=timezone.utc)
        heartbeats = {
            "SW-B01-01": now - timedelta(seconds=121),
            "SW-B01-02": now - timedelta(seconds=119),
        }
        self.assertEqual(offline_devices(heartbeats, now), ["SW-B01-01"])

    def test_sla_thresholds_match_ticket(self):
        alerts = evaluate_sla_alerts("B-01", 101.0, 2.1)
        self.assertEqual(
            {alert["alert_type"] for alert in alerts},
            {"latency_p95", "packet_loss"},
        )
        self.assertEqual(evaluate_sla_alerts("B-01", 100.0, 2.0), [])


if __name__ == "__main__":
    unittest.main()
