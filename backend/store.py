"""SQLite development store with explicit, restart-safe configuration tables."""

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from pathlib import Path
from datetime import datetime, timezone


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class Store:
    def __init__(self, path: str | None = None):
        self.path = path or os.getenv("DATABASE_PATH", "./data/iot-modbus.db")
        if self.path != ":memory:":
            Path(self.path).parent.mkdir(parents=True, exist_ok=True)
        self.db = sqlite3.connect(self.path, check_same_thread=False)
        self._lock = threading.RLock()
        self.db.row_factory = sqlite3.Row
        self.db.execute("PRAGMA busy_timeout=5000")
        self.db.execute("PRAGMA foreign_keys=ON")
        self.db.execute("PRAGMA journal_mode=WAL")
        self.migrate()

    def migrate(self):
        self.db.executescript("""
        CREATE TABLE IF NOT EXISTS schema_migrations (version INTEGER PRIMARY KEY, applied_at TEXT NOT NULL);
        CREATE TABLE IF NOT EXISTS gateways (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, model TEXT NOT NULL DEFAULT '',
            transport_mode TEXT NOT NULL CHECK(transport_mode IN ('MODBUS_TCP','MODBUS_RTU','TRANSPARENT_SERIAL')),
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
            last_connected_at TEXT, last_disconnected_at TEXT, last_activity_at TEXT,
            connection_errors INTEGER NOT NULL DEFAULT 0, reconnection_count INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS nodes (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, node_type TEXT NOT NULL CHECK(node_type IN ('ANALOG_INPUT','ANALOG_OUTPUT')),
            hardware_model TEXT NOT NULL DEFAULT '', gateway_id TEXT REFERENCES gateways(id) ON DELETE SET NULL,
            slave_id INTEGER NOT NULL DEFAULT 1 CHECK(slave_id BETWEEN 1 AND 247),
            profile_id TEXT, polling_enabled INTEGER NOT NULL DEFAULT 1,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS device_profiles (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, definition_json TEXT NOT NULL, created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS register_definitions (
            id INTEGER PRIMARY KEY AUTOINCREMENT, profile_id TEXT NOT NULL REFERENCES device_profiles(id) ON DELETE CASCADE,
            name TEXT NOT NULL, address INTEGER NOT NULL, function_code INTEGER NOT NULL, quantity INTEGER,
            data_type TEXT NOT NULL, category TEXT NOT NULL, definition_json TEXT NOT NULL, UNIQUE(profile_id,name)
        );
        CREATE TABLE IF NOT EXISTS channels (
            id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            channel_index INTEGER NOT NULL, channel_type TEXT NOT NULL CHECK(channel_type IN ('ANALOG_INPUT','ANALOG_OUTPUT')),
            label TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 1, config_json TEXT,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL, UNIQUE(node_id,channel_index,channel_type)
        );
        CREATE TABLE IF NOT EXISTS polling_items (
            id TEXT PRIMARY KEY, node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            profile_id TEXT, register_name TEXT NOT NULL, function_code INTEGER NOT NULL CHECK(function_code IN (3,4)),
            address INTEGER NOT NULL CHECK(address BETWEEN 0 AND 65535), quantity INTEGER NOT NULL CHECK(quantity BETWEEN 1 AND 125),
            data_type TEXT NOT NULL DEFAULT 'uint16', byte_order TEXT NOT NULL DEFAULT 'big', word_order TEXT NOT NULL DEFAULT 'unverified',
            interval_ms INTEGER NOT NULL CHECK(interval_ms BETWEEN 250 AND 86400000), timeout_ms INTEGER NOT NULL CHECK(timeout_ms BETWEEN 100 AND 60000),
            retries INTEGER NOT NULL CHECK(retries BETWEEN 0 AND 5), scale REAL NOT NULL DEFAULT 1, unit TEXT NOT NULL DEFAULT '',
            measurement_metadata_json TEXT NOT NULL DEFAULT '{}', float_verified INTEGER NOT NULL DEFAULT 0,
            enabled INTEGER NOT NULL DEFAULT 1, last_value TEXT, last_success_at TEXT, last_latency_ms REAL,
            last_status TEXT NOT NULL DEFAULT 'UNKNOWN', error_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS calibrations (
            node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE, channel INTEGER NOT NULL CHECK(channel BETWEEN 1 AND 6),
            revision INTEGER NOT NULL, gain REAL NOT NULL DEFAULT 1, offset_ma REAL NOT NULL DEFAULT 0, enabled INTEGER NOT NULL DEFAULT 1,
            scaling_json TEXT, method TEXT NOT NULL DEFAULT 'GAIN_OFFSET', modified_by TEXT NOT NULL DEFAULT 'admin',
            created_at TEXT NOT NULL, PRIMARY KEY(node_id, channel, revision)
        );
        CREATE TABLE IF NOT EXISTS measurements (
            id INTEGER PRIMARY KEY AUTOINCREMENT, node_id TEXT NOT NULL REFERENCES nodes(id) ON DELETE CASCADE,
            channel INTEGER NOT NULL, register_value INTEGER NOT NULL, raw_registers_json TEXT NOT NULL DEFAULT '[]', raw_ma REAL NOT NULL,
            calibration_revision INTEGER NOT NULL DEFAULT 0, calibrated_ma REAL NOT NULL,
            engineering_value REAL, engineering_unit TEXT, quality TEXT NOT NULL, measured_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS measurements_node_time ON measurements(node_id, measured_at DESC);
        CREATE TABLE IF NOT EXISTS events (
            id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, gateway_id TEXT, node_id TEXT,
            direction TEXT, protocol TEXT, function_code INTEGER, address INTEGER, quantity INTEGER,
            latency_ms REAL, result TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', request_hex TEXT, response_hex TEXT
        );
        CREATE TABLE IF NOT EXISTS audit_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, actor TEXT NOT NULL,
            action TEXT NOT NULL, entity_type TEXT NOT NULL, entity_id TEXT NOT NULL, detail_json TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS mappings (
            id TEXT PRIMARY KEY, name TEXT NOT NULL, source_node_id TEXT NOT NULL, source_channel INTEGER NOT NULL,
            destination_node_id TEXT NOT NULL, destination_channel INTEGER NOT NULL, source_value_type TEXT NOT NULL,
            config_json TEXT NOT NULL, enabled INTEGER NOT NULL DEFAULT 0, status TEXT NOT NULL DEFAULT 'PLANNED'
        );
        CREATE TABLE IF NOT EXISTS system_settings (
            key TEXT PRIMARY KEY, value TEXT NOT NULL, updated_at TEXT NOT NULL
        );
        """)
        migration = self.db.execute("SELECT MAX(version) FROM schema_migrations").fetchone()[0] or 0
        if migration < 1:
            measurement_columns = {row[1] for row in self.db.execute("PRAGMA table_info(measurements)")}
            if "raw_registers_json" not in measurement_columns:
                self.db.execute("ALTER TABLE measurements ADD COLUMN raw_registers_json TEXT NOT NULL DEFAULT '[]'")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(1,?)", (utc_now(),))
        if migration < 2:
            self._ensure_column("polling_items", "measurement_metadata_json", "TEXT NOT NULL DEFAULT '{}'")
            self._ensure_column("polling_items", "float_verified", "INTEGER NOT NULL DEFAULT 0")
            self._ensure_column("gateways", "reconnection_count", "INTEGER NOT NULL DEFAULT 0")
            from .profiles import register
            for item in self.db.execute("SELECT id,profile_id,register_name FROM polling_items").fetchall():
                definition = register(item["profile_id"], item["register_name"]) if item["profile_id"] else None
                metadata = json.dumps(definition.get("measurement", {}) if definition else {})
                self.db.execute("UPDATE polling_items SET measurement_metadata_json=? WHERE id=?", (metadata,item["id"]))
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(2,?)", (utc_now(),))
        if migration < 3:
            self._ensure_column("gateways", "registration_token_hash", "TEXT")
            self._ensure_column("gateways", "identity_method", "TEXT NOT NULL DEFAULT 'MANUAL'")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(3,?)", (utc_now(),))
        if migration < 4:
            for name, declaration in (
                ("armed", "INTEGER NOT NULL DEFAULT 0"),
                ("last_source_value", "REAL"),
                ("last_requested_value", "REAL"),
                ("last_acknowledged_value", "REAL"),
                ("last_success_at", "TEXT"),
                ("last_attempt_at", "TEXT"),
                ("last_error", "TEXT"),
                ("failsafe_attempted", "INTEGER NOT NULL DEFAULT 0"),
                ("created_at", "TEXT"),
                ("updated_at", "TEXT"),
            ):
                self._ensure_column("mappings", name, declaration)
            now = utc_now()
            self.db.execute("UPDATE mappings SET created_at=COALESCE(created_at,?),updated_at=COALESCE(updated_at,?)", (now, now))
            self.db.execute("UPDATE mappings SET armed=0,status=CASE WHEN enabled=1 THEN 'DISARMED' ELSE 'DISABLED' END")
            # Only armed pipelines own physical outputs. Legacy configured
            # mappings are disarmed on migration and can coexist safely.
            self.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS mappings_active_destination ON mappings(destination_node_id,destination_channel) WHERE enabled=1 AND armed=1")
            self.db.execute("CREATE INDEX IF NOT EXISTS mappings_source_lookup ON mappings(source_node_id,source_channel,enabled)")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(4,?)", (now,))
        if migration < 5:
            # Normalize only recognized ME31 nodes. Measurement rows, calibration
            # revisions, and event history are deliberately left untouched.
            compatible = self.db.execute("SELECT id,node_type,hardware_model FROM nodes").fetchall()
            for node in compatible:
                model = (node["hardware_model"] or "").strip().upper().replace("EBYTE ", "")
                if node["node_type"] == "ANALOG_INPUT" and model in {"ME31-XAXX0600", "ME31-XAXX0600-V1.1"}:
                    profile_id, hardware_model, prefix, channel_type = "ebyte-me31-xaxx0600-v1.1", "EBYTE ME31-XAXX0600", "AI", "ANALOG_INPUT"
                elif node["node_type"] == "ANALOG_OUTPUT" and model in {"ME31-XXXA0006", "ME31-XXXA0006-V1.1"}:
                    profile_id, hardware_model, prefix, channel_type = "ebyte-me31-xxxa0006-v1.1", "EBYTE ME31-XXXA0006", "AO", "ANALOG_OUTPUT"
                else:
                    continue
                self.db.execute("UPDATE nodes SET hardware_model=?,profile_id=?,updated_at=? WHERE id=?", (hardware_model, profile_id, utc_now(), node["id"]))
                for channel in range(1, 7):
                    self.db.execute("INSERT OR IGNORE INTO channels(id,node_id,channel_index,channel_type,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                                    (f"{node['id']}:{prefix}{channel}", node["id"], channel, channel_type, f"{prefix}{channel}", utc_now(), utc_now()))
                self.db.execute("DELETE FROM channels WHERE node_id=? AND channel_type=? AND (channel_index<1 OR channel_index>6)", (node["id"], channel_type))
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(5,?)", (utc_now(),))
        if migration < 6:
            self.db.execute("""CREATE TABLE IF NOT EXISTS output_commands (
                id INTEGER PRIMARY KEY AUTOINCREMENT, at TEXT NOT NULL, node_id TEXT NOT NULL,
                channel INTEGER NOT NULL, source TEXT NOT NULL, target_ma REAL NOT NULL,
                readback_ma REAL, result TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '',
                request_hex TEXT, response_hex TEXT
            )""")
            self.db.execute("CREATE INDEX IF NOT EXISTS output_commands_node_time ON output_commands(node_id,channel,at DESC)")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(6,?)", (utc_now(),))
        if migration < 7:
            # Replace the original configuration-level reservation with actual
            # runtime ownership. Disarmed pipelines do not control a device.
            self.db.execute("DROP INDEX IF EXISTS mappings_active_destination")
            self.db.execute("CREATE UNIQUE INDEX mappings_active_destination ON mappings(destination_node_id,destination_channel) WHERE enabled=1 AND armed=1")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(7,?)", (utc_now(),))
        if migration < 8:
            for name, declaration in (
                ("consecutive_write_failures", "INTEGER NOT NULL DEFAULT 0"),
                ("last_failed_target", "REAL"),
                ("faulted_at", "TEXT"),
                ("failsafe_status", "TEXT NOT NULL DEFAULT 'NOT_APPLIED'"),
                ("last_raw_ma", "REAL"),
                ("last_calibrated_ma", "REAL"),
                ("last_engineering_value", "REAL"),
                ("source_quality", "TEXT"),
                ("last_communication_at", "TEXT"),
            ):
                self._ensure_column("mappings", name, declaration)
            self._ensure_column("gateways", "last_disconnect_reason", "TEXT")
            for name, declaration in (
                ("pipeline_id", "TEXT"),
                ("source_node_id", "TEXT"),
                ("source_channel", "INTEGER"),
                ("destination_node_id", "TEXT"),
                ("destination_channel", "INTEGER"),
                ("classification", "TEXT"),
            ):
                self._ensure_column("events", name, declaration)
            self._ensure_column("output_commands", "pipeline_id", "TEXT")
            self._ensure_column("output_commands", "classification", "TEXT NOT NULL DEFAULT 'MANUAL'")
            self.db.execute("CREATE INDEX IF NOT EXISTS events_pipeline_time ON events(pipeline_id,at DESC)")
            defaults = {
                "output_range_timeout_ms": os.getenv("OUTPUT_RANGE_TIMEOUT_MS", "5000"),
                "output_write_timeout_ms": os.getenv("OUTPUT_WRITE_TIMEOUT_MS", "5000"),
                "output_readback_timeout_ms": os.getenv("OUTPUT_READBACK_TIMEOUT_MS", "5000"),
            }
            for key, raw_value in defaults.items():
                try:
                    value = min(30000, max(250, int(raw_value)))
                except (TypeError, ValueError):
                    value = 5000
                self.db.execute("INSERT OR IGNORE INTO system_settings(key,value,updated_at) VALUES(?,?,?)", (key,str(value),utc_now()))
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(8,?)", (utc_now(),))
        if migration < 9:
            self._ensure_column("gateways", "identity_method", "TEXT NOT NULL DEFAULT 'MANUAL'")
            for name, declaration in (
                ("auto_reconnect_enabled", "INTEGER NOT NULL DEFAULT 0"),
                ("trusted_ingress_id", "TEXT"),
                ("trusted_peer_ip", "TEXT"),
                ("listener_port", "INTEGER"),
                ("identity_verified", "INTEGER NOT NULL DEFAULT 0"),
                ("connection_state", "TEXT NOT NULL DEFAULT 'DISCONNECTED'"),
                ("commissioning_state", "TEXT NOT NULL DEFAULT 'UNPROVISIONED'"),
                ("session_generation", "INTEGER NOT NULL DEFAULT 0"),
                ("last_session_id", "TEXT"),
                ("last_reconnected_at", "TEXT"),
                ("last_successful_modbus_at", "TEXT"),
            ):
                self._ensure_column("gateways", name, declaration)
            self.db.execute("CREATE UNIQUE INDEX IF NOT EXISTS gateways_listener_port_unique ON gateways(listener_port) WHERE listener_port IS NOT NULL")
            # Existing records retain operator commissioning. No auto-trust is
            # inferred from historical peers, source IPs, or credentials.
            self.db.execute("UPDATE gateways SET commissioning_state=CASE WHEN identity_method='MANUAL' THEN 'MANUAL_REQUIRED' ELSE commissioning_state END")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(9,?)", (utc_now(),))
            migration = 9
        if migration < 10:
            self._ensure_column("mappings", "last_output_verified_session_id", "TEXT")
            self._ensure_column("mappings", "last_output_verified_generation", "INTEGER")
            self._ensure_column("mappings", "last_output_verified_at", "TEXT")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(10,?)", (utc_now(),))
            migration = 10
        if migration < 11:
            self._ensure_column("gateways", "last_modbus_generation", "INTEGER")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(11,?)", (utc_now(),))
            migration = 11
        if migration < 12:
            self._ensure_column("events", "session_id", "TEXT")
            self._ensure_column("events", "session_generation", "INTEGER")
            self._ensure_column("events", "identity_method", "TEXT")
            self._ensure_column("events", "peer", "TEXT")
            self.db.execute("CREATE INDEX IF NOT EXISTS events_gateway_session ON events(gateway_id,session_generation,at DESC)")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(12,?)", (utc_now(),))
            migration = 12
        if migration < 13:
            self._ensure_column("mappings", "last_output_verified_setpoint_ma", "REAL")
            self.db.execute("INSERT INTO schema_migrations(version,applied_at) VALUES(13,?)", (utc_now(),))
        from .profiles import PROFILES
        for profile in PROFILES:
            self.db.execute("INSERT OR IGNORE INTO device_profiles(id,name,definition_json,created_at) VALUES(?,?,?,?)",
                            (profile["id"],profile["name"],json.dumps(profile,sort_keys=True),utc_now()))
            for definition in profile["registers"]:
                self.db.execute("INSERT OR IGNORE INTO register_definitions(profile_id,name,address,function_code,quantity,data_type,category,definition_json) VALUES(?,?,?,?,?,?,?,?)",
                                (profile["id"],definition["name"],definition["address"],definition["function_code"],definition.get("quantity"),definition.get("data_type","raw"),definition.get("category","Custom Registers"),json.dumps(definition,sort_keys=True)))
        self.db.commit()

    def _ensure_column(self, table: str, name: str, declaration: str):
        columns = {row[1] for row in self.db.execute(f"PRAGMA table_info({table})")}
        if name not in columns:
            self.db.execute(f"ALTER TABLE {table} ADD COLUMN {name} {declaration}")

    def rows(self, sql: str, params=()):
        with self._lock:
            return [dict(row) for row in self.db.execute(sql, params).fetchall()]

    def one(self, sql: str, params=()):
        with self._lock:
            row = self.db.execute(sql, params).fetchone()
            return dict(row) if row else None

    def run(self, sql: str, params=()):
        with self._lock:
            cur = self.db.execute(sql, params)
            self.db.commit()
            return cur

    def close(self):
        with self._lock:
            self.db.close()

    @contextmanager
    def transaction(self):
        """Serialize a short multi-statement transaction across async callers/worker threads."""
        with self._lock:
            try:
                yield self.db
                self.db.commit()
            except Exception:
                self.db.rollback()
                raise

    def audit(self, action: str, entity_type: str, entity_id: str, detail: dict, actor="admin"):
        self.run("INSERT INTO audit_log(at,actor,action,entity_type,entity_id,detail_json) VALUES(?,?,?,?,?,?)",
                 (utc_now(), actor, action, entity_type, entity_id, json.dumps(detail, sort_keys=True)))

    def setting(self, key: str, default=None):
        row = self.one("SELECT value FROM system_settings WHERE key=?", (key,))
        return row["value"] if row else default

    def set_setting(self, key: str, value: str):
        self.run("INSERT INTO system_settings(key,value,updated_at) VALUES(?,?,?) ON CONFLICT(key) DO UPDATE SET value=excluded.value,updated_at=excluded.updated_at",
                 (key, str(value), utc_now()))

    def event(self, result: str, detail: str, **kwargs):
        fields = ["gateway_id", "node_id", "direction", "protocol", "function_code", "address", "quantity", "latency_ms", "request_hex", "response_hex", "pipeline_id", "source_node_id", "source_channel", "destination_node_id", "destination_channel", "classification", "session_id", "session_generation", "identity_method", "peer"]
        values = [kwargs.get(field) for field in fields]
        self.run(f"INSERT INTO events(at,{','.join(fields)},result,detail) VALUES(?,{','.join('?' for _ in fields)},?,?)",
                 [utc_now(), *values, result, detail])
