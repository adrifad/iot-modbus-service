import tempfile
import unittest
from pathlib import Path
from uuid import uuid4

from backend.store import Store, utc_now


class StoreTests(unittest.TestCase):
    def test_state_directory_database_survives_store_restart(self):
        with tempfile.TemporaryDirectory() as temp:
            state = Path(temp) / "state"
            path = state / "iot-modbus.db"
            first = Store(str(path))
            now = utc_now()
            gateway_id = str(uuid4())
            first.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)",
                      (gateway_id, "Persistent gateway", "MODBUS_TCP", now, now))
            first.close()

            second = Store(str(path))
            try:
                self.assertEqual(second.one("SELECT name FROM gateways WHERE id=?", (gateway_id,))["name"], "Persistent gateway")
                self.assertEqual(second.one("SELECT MAX(version) version FROM schema_migrations")["version"], 3)
                self.assertTrue(path.exists())
            finally:
                second.close()


if __name__ == "__main__":
    unittest.main()
