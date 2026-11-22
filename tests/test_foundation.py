import json
import unittest
from collections import Counter
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


class CampusFoundationTests(unittest.TestCase):
    def test_device_inventory_has_two_buildings_and_required_device_counts(self):
        inventory = json.loads((ROOT / "config" / "campus.json").read_text())

        buildings = inventory["buildings"]
        devices = inventory["devices"]

        self.assertEqual(len(buildings), 2)
        self.assertEqual(
            Counter(device["device_type"] for device in devices),
            {"switch": 8, "access_point": 12, "latency_probe": 4},
        )
        self.assertEqual(len({device["device_id"] for device in devices}), 24)
        building_ids = {building["building_id"] for building in buildings}
        self.assertEqual(
            {device["building_id"] for device in devices},
            building_ids,
        )

    def test_payload_schema_requires_common_and_device_specific_fields(self):
        schema = json.loads(
            (ROOT / "schemas" / "telemetry.schema.json").read_text()
        )

        self.assertEqual(
            schema["required"],
            ["device_id", "device_type", "building_id", "timestamp", "data"],
        )
        self.assertEqual(
            set(schema["properties"]["device_type"]["enum"]),
            {"switch", "access_point", "latency_probe"},
        )

        required_data = {
            branch["if"]["properties"]["device_type"]["const"]: set(
                branch["then"]["properties"]["data"]["required"]
            )
            for branch in schema["allOf"]
        }
        self.assertEqual(
            required_data,
            {
                "switch": {
                    "port_in_mbps",
                    "port_out_mbps",
                    "errors",
                    "uptime_s",
                },
                "access_point": {
                    "clients",
                    "rssi_avg_dbm",
                    "channel_util_pct",
                },
                "latency_probe": {
                    "latency_ms",
                    "packet_loss_pct",
                    "target",
                },
            },
        )


if __name__ == "__main__":
    unittest.main()
