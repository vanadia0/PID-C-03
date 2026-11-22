import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.ingest import build_kafka_record, build_raw_document


class IngestTests(unittest.TestCase):
    def test_build_kafka_record_uses_device_id_as_key(self):
        payload = {
            "device_id": "AP-B02-03",
            "device_type": "access_point",
            "building_id": "B-02",
            "timestamp": "2026-11-23T10:30:00Z",
            "data": {"clients": 24},
        }

        key, value, topic = build_kafka_record(
            payload,
            mqtt_topic="campus/telemetry/B-02/access_point",
            kafka_topic="campus.telemetry",
        )

        self.assertEqual(key, "AP-B02-03")
        self.assertEqual(value, payload)
        self.assertEqual(topic, "campus.telemetry")

    def test_build_raw_document_enriches_payload_for_mongodb(self):
        payload = {
            "device_id": "SW-B01-01",
            "device_type": "switch",
            "building_id": "B-01",
            "timestamp": "2026-11-23T10:30:00Z",
            "data": {
                "port_in_mbps": 42.5,
                "port_out_mbps": 18.2,
                "errors": 1,
                "uptime_s": 86410,
            },
        }

        document = build_raw_document(
            payload,
            mqtt_topic="campus/telemetry/B-01/switch",
            received_at=datetime(2026, 11, 23, 10, 30, 1, tzinfo=timezone.utc),
        )

        self.assertEqual(document["device_id"], "SW-B01-01")
        self.assertEqual(document["building_id"], "B-01")
        self.assertEqual(document["device_type"], "switch")
        self.assertEqual(document["payload"], payload)
        self.assertEqual(document["mqtt_topic"], "campus/telemetry/B-01/switch")
        self.assertEqual(document["received_at"], "2026-11-23T10:30:01Z")
        self.assertEqual(document["quality_status"], "pending")


if __name__ == "__main__":
    unittest.main()
