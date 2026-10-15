import unittest
from datetime import datetime, timedelta, timezone

from backend.app import _item_health, _node_modbus_health


class HealthStateTests(unittest.TestCase):
    def test_gateway_connection_does_not_hide_failed_poll_item(self):
        now = datetime.now(timezone.utc)
        node = {"node_type": "ANALOG_INPUT", "gateway_id": "gw", "polling_enabled": 1}
        items = [
            {"enabled": 1, "interval_ms": 1000, "timeout_ms": 500, "last_success_at": now.isoformat(), "last_status": "GOOD"},
            {"enabled": 1, "interval_ms": 1000, "timeout_ms": 500, "last_success_at": None, "last_status": "TIMEOUT"},
        ]
        self.assertEqual(_node_modbus_health(node, items, True), "TIMEOUT")

    def test_freshness_uses_each_polling_interval(self):
        now = datetime.now(timezone.utc)
        daily_config = {"enabled": 1, "interval_ms": 86400000, "timeout_ms": 1500,
                        "last_success_at": (now - timedelta(hours=23)).isoformat(), "last_status": "GOOD"}
        fast_measurement = {"enabled": 1, "interval_ms": 1000, "timeout_ms": 500,
                            "last_success_at": (now - timedelta(seconds=45)).isoformat(), "last_status": "GOOD"}
        self.assertEqual(_item_health(daily_config, now), "RESPONDING")
        self.assertEqual(_item_health(fast_measurement, now), "STALE")

    def test_output_node_device_health_remains_unknown(self):
        node = {"node_type": "ANALOG_OUTPUT", "gateway_id": "gw", "polling_enabled": 1}
        self.assertEqual(_node_modbus_health(node, [], True), "UNKNOWN")


if __name__ == "__main__":
    unittest.main()
