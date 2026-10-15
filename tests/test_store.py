import tempfile
import sqlite3
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
            first.run("UPDATE gateways SET auto_reconnect_enabled=1,trusted_ingress_id='west-wg',trusted_peer_ip='10.70.0.11',listener_port=5101,session_generation=7,last_session_id='session-7',last_connected_at=? WHERE id=?",(now,gateway_id))
            first.close()

            second = Store(str(path))
            try:
                self.assertEqual(second.one("SELECT name FROM gateways WHERE id=?", (gateway_id,))["name"], "Persistent gateway")
                binding=second.one("SELECT auto_reconnect_enabled,trusted_ingress_id,trusted_peer_ip,listener_port,session_generation,last_session_id FROM gateways WHERE id=?",(gateway_id,))
                self.assertEqual(binding,{"auto_reconnect_enabled":1,"trusted_ingress_id":"west-wg","trusted_peer_ip":"10.70.0.11","listener_port":5101,"session_generation":7,"last_session_id":"session-7"})
                self.assertEqual(second.one("SELECT MAX(version) version FROM schema_migrations")["version"], 13)
                self.assertTrue(path.exists())
            finally:
                second.close()

    def test_v3_database_migration_normalizes_compatible_ao_without_erasing_history(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/"legacy.db"
            raw=sqlite3.connect(path)
            raw.executescript("""
                CREATE TABLE schema_migrations(version INTEGER PRIMARY KEY,applied_at TEXT NOT NULL);
                INSERT INTO schema_migrations VALUES(3,'2026-01-01T00:00:00+00:00');
                CREATE TABLE gateways(id TEXT PRIMARY KEY,name TEXT NOT NULL,model TEXT NOT NULL DEFAULT '',transport_mode TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
                CREATE TABLE nodes(id TEXT PRIMARY KEY,name TEXT NOT NULL,node_type TEXT NOT NULL,hardware_model TEXT NOT NULL DEFAULT '',gateway_id TEXT,slave_id INTEGER NOT NULL DEFAULT 1,profile_id TEXT,polling_enabled INTEGER NOT NULL DEFAULT 1,created_at TEXT NOT NULL,updated_at TEXT NOT NULL);
                CREATE TABLE channels(id TEXT PRIMARY KEY,node_id TEXT NOT NULL,channel_index INTEGER NOT NULL,channel_type TEXT NOT NULL,label TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 1,config_json TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,UNIQUE(node_id,channel_index,channel_type));
                CREATE TABLE mappings(id TEXT PRIMARY KEY,name TEXT NOT NULL,source_node_id TEXT NOT NULL,source_channel INTEGER NOT NULL,destination_node_id TEXT NOT NULL,destination_channel INTEGER NOT NULL,source_value_type TEXT NOT NULL,config_json TEXT NOT NULL,enabled INTEGER NOT NULL DEFAULT 0,status TEXT NOT NULL DEFAULT 'PLANNED');
                CREATE TABLE measurements(id INTEGER PRIMARY KEY,node_id TEXT NOT NULL,channel INTEGER NOT NULL,measured_at TEXT NOT NULL);
                CREATE TABLE calibrations(node_id TEXT NOT NULL,channel INTEGER NOT NULL,revision INTEGER NOT NULL,gain REAL NOT NULL,offset_ma REAL NOT NULL,enabled INTEGER NOT NULL,created_at TEXT NOT NULL,PRIMARY KEY(node_id,channel,revision));
                INSERT INTO nodes VALUES('legacy-ao','Legacy AO','ANALOG_OUTPUT','EBYTE ME31-XXXA0006',NULL,1,NULL,1,'2025-01-01','2025-01-01');
                INSERT INTO measurements(node_id,channel,measured_at) VALUES('legacy-ao',1,'2025-01-01');
                INSERT INTO calibrations VALUES('legacy-ao',1,7,1.1,0.1,1,'2025-01-01');
            """)
            raw.commit();raw.close()
            migrated=Store(str(path))
            try:
                node=migrated.one("SELECT hardware_model,profile_id FROM nodes WHERE id='legacy-ao'")
                self.assertEqual(node["hardware_model"],"EBYTE ME31-XXXA0006")
                self.assertEqual(node["profile_id"],"ebyte-me31-xxxa0006-v1.1")
                self.assertEqual(migrated.one("SELECT COUNT(*) count FROM channels WHERE node_id='legacy-ao'")["count"],6)
                self.assertEqual(migrated.one("SELECT COUNT(*) count FROM measurements WHERE node_id='legacy-ao'")["count"],1)
                self.assertEqual(migrated.one("SELECT revision FROM calibrations WHERE node_id='legacy-ao'")["revision"],7)
            finally:
                migrated.close()
            reopened=Store(str(path))
            try:
                self.assertEqual(reopened.one("SELECT MAX(version) version FROM schema_migrations")["version"],13)
                self.assertEqual(reopened.one("SELECT COUNT(*) count FROM channels WHERE node_id='legacy-ao'")["count"],6)
            finally:
                reopened.close()

    def test_migration_7_allows_multiple_enabled_disarmed_pipeline_definitions(self):
        with tempfile.TemporaryDirectory() as temp:
            path=Path(temp)/"ownership.db"
            store=Store(str(path))
            now=utc_now()
            values=[]
            for pipeline_id,source_channel in (("one",1),("two",2)):
                values.append((pipeline_id,pipeline_id,"ai",source_channel,"ao",1,"CALIBRATED_MA","{}",0,"DISABLED",now,now))
            store.run("""INSERT INTO mappings(id,name,source_node_id,source_channel,destination_node_id,destination_channel,
                source_value_type,config_json,enabled,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",values[0])
            store.run("""INSERT INTO mappings(id,name,source_node_id,source_channel,destination_node_id,destination_channel,
                source_value_type,config_json,enabled,status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?)""",values[1])
            store.run("DROP INDEX mappings_active_destination")
            store.run("CREATE UNIQUE INDEX mappings_active_destination ON mappings(destination_node_id,destination_channel) WHERE enabled=1")
            store.run("DELETE FROM schema_migrations WHERE version>=7")
            store.close()
            migrated=Store(str(path))
            try:
                migrated.run("UPDATE mappings SET enabled=1,status='DISARMED'")
                self.assertEqual(migrated.one("SELECT COUNT(*) count FROM mappings WHERE enabled=1 AND armed=0")["count"],2)
            finally:
                migrated.close()


if __name__ == "__main__":
    unittest.main()
