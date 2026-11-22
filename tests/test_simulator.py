import sys
import unittest
from datetime import datetime, timezone
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.simulator import (
    QualityConfig,
    calculate_load_factor,
    generate_payload,
    inject_quality,
    load_inventory,
)


class CampusSimulatorTests(unittest.TestCase):
    def test_quality_injection_can_force_reset_unit_and_missing_heartbeat(self):
        switch = {
            "device_id": "SW-B01-01",
            "device_type": "switch",
            "building_id": "B-01",
        }
        payload = generate_payload(
            switch,
            now=datetime(2026, 11, 23, 10, 30, tzinfo=timezone.utc),
            rng_seed=7,
        )
        config = QualityConfig(
            counter_reset_probability=1.0,
            inconsistent_unit_probability=1.0,
            missing_heartbeat_probability=1.0,
        )

        injected_payload, events = inject_quality(payload, config, rng_seed=7)

        self.assertIsNone(injected_payload)
        self.assertEqual(
            set(events),
            {"counter_reset", "inconsistent_unit", "missing_heartbeat"},
        )

    def test_load_factor_is_higher_during_class_hours_and_flash_crowd(self):
        class_hour = datetime(2026, 11, 23, 14, 0, tzinfo=timezone.utc)
        night = datetime(2026, 11, 23, 2, 0, tzinfo=timezone.utc)

        normal = calculate_load_factor(class_hour)
        crowded = calculate_load_factor(class_hour, flash_crowd=True)

        self.assertGreater(normal, calculate_load_factor(night))
        self.assertGreater(crowded, normal)

    def test_inventory_and_payload_contract_for_every_device_type(self):
        inventory = load_inventory(ROOT / "config" / "campus.json")

        for device in inventory["devices"]:
            payload = generate_payload(
                device,
                now=datetime(2026, 11, 23, 10, 30, tzinfo=timezone.utc),
                load_factor=1.0,
                rng_seed=42,
            )

            self.assertEqual(payload["device_id"], device["device_id"])
            self.assertEqual(payload["device_type"], device["device_type"])
            self.assertEqual(payload["building_id"], device["building_id"])
            self.assertEqual(payload["timestamp"], "2026-11-23T10:30:00Z")
            self.assertIsInstance(payload["data"], dict)

            if device["device_type"] == "switch":
                self.assertEqual(
                    set(payload["data"]),
                    {"port_in_mbps", "port_out_mbps", "errors", "uptime_s"},
                )
            elif device["device_type"] == "access_point":
                self.assertEqual(
                    set(payload["data"]),
                    {"clients", "rssi_avg_dbm", "channel_util_pct"},
                )
            else:
                self.assertEqual(
                    set(payload["data"]),
                    {"latency_ms", "packet_loss_pct", "target"},
                )


if __name__ == "__main__":
    unittest.main()
