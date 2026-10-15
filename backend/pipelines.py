"""Persistent, explicitly armed AI-to-AO pipeline validation and execution."""

import asyncio
import json
import math
import time
from datetime import datetime, timezone

from .gateway import OutputReadbackError
from .output import OutputError
from .store import utc_now


class PipelineError(ValueError):
    pass


class PipelineService:
    TICK_SECONDS = 0.2

    def __init__(self, store, sessions, output_driver):
        self.store = store
        self.sessions = sessions
        self.output = output_driver
        self.running = False
        self.task: asyncio.Task | None = None
        self._next_due: dict[str, float] = {}
        self._processing: set[str] = set()

    def start(self):
        if self.running:
            return
        # Process restart requires an operator to arm again. The configured
        # enabled state remains persistent, while no stale command is replayed.
        self.store.run("UPDATE mappings SET armed=0,status=CASE WHEN enabled=1 THEN 'DISARMED' ELSE 'DISABLED' END")
        self.running = True
        self.task = asyncio.create_task(self.loop(), name="pipeline-executor")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        self._processing.clear()
        self.store.run("UPDATE mappings SET armed=0,status=CASE WHEN enabled=1 THEN 'DISARMED' ELSE 'DISABLED' END,updated_at=? WHERE armed=1", (utc_now(),))

    async def loop(self):
        while self.running:
            now = time.monotonic()
            rows = self.store.rows("SELECT * FROM mappings WHERE enabled=1 AND armed=1 ORDER BY created_at,id")
            active_ids = {row["id"] for row in rows}
            self._next_due = {key: value for key, value in self._next_due.items() if key in active_ids}
            for row in rows:
                if row["id"] in self._processing or now < self._next_due.get(row["id"], 0):
                    continue
                try:
                    config = json.loads(row["config_json"])
                    interval = max(1, int(config.get("update_interval_ms", 1000))) / 1000
                except (ValueError, TypeError, json.JSONDecodeError):
                    interval = 1
                self._next_due[row["id"]] = now + max(0.25, interval)
                self._processing.add(row["id"])
                try:
                    await self.execute(row["id"])
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    self._set_runtime(row["id"], "OUTPUT_WRITE_FAILED", last_error=str(exc))
                finally:
                    self._processing.discard(row["id"])
            await asyncio.sleep(self.TICK_SECONDS)

    def list(self):
        result = []
        for row in self.store.rows("SELECT * FROM mappings ORDER BY created_at DESC,id"):
            result.append(self.present(row))
        return result

    def get(self, pipeline_id: str):
        row = self.store.one("SELECT * FROM mappings WHERE id=?", (pipeline_id,))
        return self.present(row) if row else None

    @staticmethod
    def present(row):
        if not row:
            return None
        result = dict(row)
        try:
            result["config"] = json.loads(result.get("config_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            result["config"] = {}
        result["config_json"] = result["config"]
        result["enabled"] = bool(result["enabled"])
        result["armed"] = bool(result["armed"])
        result["failsafe_attempted"] = bool(result["failsafe_attempted"])
        return result

    def validate_definition(self, data):
        name = data["name"].strip()
        if not name or len(name) > 100:
            raise PipelineError("Pipeline name must be 1–100 characters")
        src = self.store.one("SELECT * FROM nodes WHERE id=?", (data["source_node_id"],))
        dst = self.store.one("SELECT * FROM nodes WHERE id=?", (data["destination_node_id"],))
        if not src or src["node_type"] != "ANALOG_INPUT" or src["profile_id"] != "ebyte-me31-xaxx0600-v1.1":
            raise PipelineError("Source must be a registered ME31-XAXX0600 input node")
        if not dst or dst["node_type"] != "ANALOG_OUTPUT" or dst["profile_id"] != "ebyte-me31-xxxa0006-v1.1":
            raise PipelineError("Destination must be a registered ME31-XXXA0006 output node")
        source_channel = data["source_channel"]
        destination_channel = data["destination_channel"]
        if not self._channel_enabled(src["id"], source_channel, "ANALOG_INPUT"):
            raise PipelineError("Source channel must exist and be enabled")
        if not self._channel_enabled(dst["id"], destination_channel, "ANALOG_OUTPUT"):
            raise PipelineError("Destination channel must exist and be enabled")
        if not src["polling_enabled"]:
            raise PipelineError("Source input node polling is disabled")
        src_gateway = self.store.one("SELECT transport_mode FROM gateways WHERE id=?", (src["gateway_id"],)) if src["gateway_id"] else None
        dst_gateway = self.store.one("SELECT transport_mode FROM gateways WHERE id=?", (dst["gateway_id"],)) if dst["gateway_id"] else None
        if not src_gateway or src_gateway["transport_mode"] != "MODBUS_TCP":
            raise PipelineError("ME31-XAXX0600 input nodes require a MODBUS_TCP gateway")
        if not dst_gateway or dst_gateway["transport_mode"] != "TRANSPARENT_SERIAL":
            raise PipelineError("ME31-XXXA0006 output nodes require a TRANSPARENT_SERIAL gateway")

        value_type = data["source_value_type"]
        if value_type not in {"RAW_MA", "CALIBRATED_MA", "ENGINEERING"}:
            raise PipelineError("Unsupported source value type")
        config = dict(data["config"])
        allowed_config={"transformation","gain","offset_ma","input_min","input_max","output_min_ma","output_max_ma",
                        "update_interval_ms","deadband_ma","stale_timeout_ms","timeout_ms","retries","fail_safe_policy","safe_setpoint_ma"}
        if set(config)-allowed_config:
            raise PipelineError("Pipeline config contains unsupported parameters")
        mode = config.get("transformation", "DIRECT")
        if mode not in {"DIRECT", "GAIN_OFFSET", "TWO_POINT"}:
            raise PipelineError("Transformation must be DIRECT, GAIN_OFFSET, or TWO_POINT")
        if value_type == "ENGINEERING" and mode == "DIRECT":
            raise PipelineError("Engineering values require an explicit transformation into mA")
        numeric_names = {"gain", "offset_ma", "input_min", "input_max", "output_min_ma", "output_max_ma", "deadband_ma", "safe_setpoint_ma"}
        for key in numeric_names & config.keys():
            value = config[key]
            if value is None:
                continue
            if isinstance(value, bool) or not isinstance(value, (float, int)) or not math.isfinite(value):
                raise PipelineError(f"{key} must be a finite number")
        if mode == "GAIN_OFFSET" and ("gain" not in config or "offset_ma" not in config):
            raise PipelineError("Gain and offset must both be configured")
        if mode == "GAIN_OFFSET" and (abs(config["gain"]) < 1e-6 or abs(config["gain"]) > 100):
            raise PipelineError("Gain magnitude must be between 0.000001 and 100")
        if "offset_ma" in config and not -25 <= config["offset_ma"] <= 25:
            raise PipelineError("Offset must be between -25 and 25 mA")
        if mode == "TWO_POINT":
            required = ("input_min", "input_max", "output_min_ma", "output_max_ma")
            if any(key not in config for key in required):
                raise PipelineError("Two-point scaling requires both input and output endpoints")
            if abs(config["input_max"] - config["input_min"]) < 1e-6:
                raise PipelineError("Two-point input span must be at least 0.000001")
            if any(not 0 <= config[key] <= 25 for key in ("output_min_ma", "output_max_ma")):
                raise PipelineError("Two-point output endpoints must be within 0–25 mA")
        if not 0 <= config.get("deadband_ma", 0) <= 25:
            raise PipelineError("Deadband must be between 0 and 25 mA")
        if config.get("safe_setpoint_ma") is not None and not 0 <= config["safe_setpoint_ma"] <= 25:
            raise PipelineError("Safe setpoint must be within 0–25 mA")
        interval = int(config.get("update_interval_ms", 1000))
        stale = int(config.get("stale_timeout_ms", 10000))
        timeout = int(config.get("timeout_ms", 2000))
        retries = int(config.get("retries", 1))
        if interval not in range(250, 60001):
            raise PipelineError("Update interval must be 250–60000 ms")
        if stale not in range(500, 3600001):
            raise PipelineError("Stale timeout must be 500 ms–1 hour")
        if timeout not in range(100, 10001):
            raise PipelineError("Communication timeout must be 100–10000 ms")
        if retries not in range(0, 4):
            raise PipelineError("Retry count must be 0–3")
        fail_safe = config.get("fail_safe_policy", "HOLD_LAST")
        if fail_safe not in {"HOLD_LAST", "SAFE_SETPOINT"}:
            raise PipelineError("Fail-safe policy must be HOLD_LAST or SAFE_SETPOINT")
        if fail_safe == "SAFE_SETPOINT" and config.get("safe_setpoint_ma") is None:
            raise PipelineError("SAFE_SETPOINT requires a configured safe_setpoint_ma")
        return {**data, "name": name, "config": {**config, "transformation": mode,
                "update_interval_ms": interval, "stale_timeout_ms": stale, "timeout_ms": timeout,
                "retries": retries, "deadband_ma": float(config.get("deadband_ma", 0)),
                "fail_safe_policy": fail_safe}}

    def _channel_enabled(self, node_id, channel, kind):
        if channel not in range(1, 7):
            return False
        record = self.store.one("SELECT enabled FROM channels WHERE node_id=? AND channel_index=? AND channel_type=?", (node_id, channel, kind))
        return bool(record and record["enabled"])

    def create(self, data):
        clean = self.validate_definition(data)
        pipeline_id = data.get("id") or __import__("uuid").uuid4().hex
        now = utc_now()
        self.store.run("""INSERT INTO mappings(id,name,source_node_id,source_channel,destination_node_id,destination_channel,
            source_value_type,config_json,enabled,status,armed,created_at,updated_at)
            VALUES(?,?,?,?,?,?,?,?,0,'DISABLED',0,?,?)""", (
            pipeline_id, clean["name"], clean["source_node_id"], clean["source_channel"], clean["destination_node_id"],
            clean["destination_channel"], clean["source_value_type"], json.dumps(clean["config"], sort_keys=True), now, now))
        self.store.audit("CREATE", "pipeline", pipeline_id, clean)
        return self.get(pipeline_id)

    def update(self, pipeline_id, changes):
        row = self.store.one("SELECT * FROM mappings WHERE id=?", (pipeline_id,))
        if not row:
            raise KeyError("Pipeline not found")
        if row["enabled"] or row["armed"]:
            raise PipelineError("Disable and disarm the pipeline before editing its configuration")
        try:
            current_config = json.loads(row["config_json"] or "{}")
        except json.JSONDecodeError:
            current_config = {}
        merged = {"name": row["name"], "source_node_id": row["source_node_id"],
                  "source_channel": row["source_channel"], "destination_node_id": row["destination_node_id"],
                  "destination_channel": row["destination_channel"], "source_value_type": row["source_value_type"],
                  "config": current_config}
        merged.update(changes)
        clean = self.validate_definition(merged)
        now = utc_now()
        with self.store.transaction() as db:
            db.execute("""UPDATE mappings SET name=?,source_node_id=?,source_channel=?,destination_node_id=?,destination_channel=?,
                source_value_type=?,config_json=?,status='DISABLED',last_error=NULL,updated_at=? WHERE id=? AND enabled=0 AND armed=0""",
                (clean["name"], clean["source_node_id"], clean["source_channel"], clean["destination_node_id"],
                 clean["destination_channel"], clean["source_value_type"], json.dumps(clean["config"], sort_keys=True), now, pipeline_id))
        self.store.audit("UPDATE", "pipeline", pipeline_id, clean)
        return self.get(pipeline_id)

    async def enable(self, pipeline_id, confirm):
        if not confirm:
            raise PipelineError("Explicit confirmation is required to enable this pipeline")
        row = self.store.one("SELECT * FROM mappings WHERE id=?", (pipeline_id,))
        if not row:
            raise KeyError("Pipeline not found")
        definition = self.present(row)
        self.validate_definition({"id": pipeline_id, "name": row["name"], "source_node_id": row["source_node_id"],
            "source_channel": row["source_channel"], "destination_node_id": row["destination_node_id"],
            "destination_channel": row["destination_channel"], "source_value_type": row["source_value_type"], "config": definition["config"]})
        with self.store.transaction() as db:
            db.execute("UPDATE mappings SET enabled=1,armed=0,status='DISARMED',failsafe_attempted=0,updated_at=? WHERE id=?", (utc_now(), pipeline_id))
        self._next_due.pop(pipeline_id, None)
        self.store.audit("ENABLE", "pipeline", pipeline_id, {"confirmed": True})
        return self.get(pipeline_id)

    async def arm(self, pipeline_id, confirm):
        if not confirm:
            raise PipelineError("Explicit operator confirmation is required before output control can be armed")
        row = self.store.one("SELECT * FROM mappings WHERE id=?", (pipeline_id,))
        if not row:
            raise KeyError("Pipeline not found")
        if not row["enabled"]:
            raise PipelineError("Enable the pipeline before arming it")
        definition = self.present(row)
        data = {"name": row["name"], "source_node_id": row["source_node_id"], "source_channel": row["source_channel"],
                "destination_node_id": row["destination_node_id"], "destination_channel": row["destination_channel"],
                "source_value_type": row["source_value_type"], "config": definition["config"]}
        self.validate_definition(data)
        source = self.store.one("SELECT * FROM nodes WHERE id=?", (row["source_node_id"],))
        source_session = self.sessions.for_gateway(source["gateway_id"]) if source and source["gateway_id"] else None
        if not source_session:
            raise PipelineError("Source gateway is not connected and commissioned")
        measurement = self._latest_source(row, definition["config"])
        if not measurement or measurement["quality"] != "GOOD":
            raise PipelineError("A fresh GOOD source measurement is required before arming")
        age = self._age_seconds(measurement["measured_at"])
        if age is None or age * 1000 > int(definition["config"].get("stale_timeout_ms", 10000)):
            raise PipelineError("Source measurement is older than the configured stale timeout")
        async with self.output.lock_for(row["destination_node_id"], row["destination_channel"]):
            snapshot = await self.output.inspect(row["destination_node_id"])
            channel = snapshot["channels"][row["destination_channel"] - 1]
            if not channel["range_known"]:
                raise PipelineError("Destination output range is unknown")
            with self.store.transaction() as db:
                owner = db.execute("SELECT id FROM mappings WHERE enabled=1 AND armed=1 AND destination_node_id=? AND destination_channel=? AND id<>?", (row["destination_node_id"], row["destination_channel"], pipeline_id)).fetchone()
                if owner:
                    raise PipelineError("Another enabled pipeline owns this AO channel")
                db.execute("UPDATE mappings SET armed=1,status='WAITING_FOR_SOURCE',last_error=NULL,failsafe_attempted=0,updated_at=? WHERE id=? AND enabled=1", (utc_now(), pipeline_id))
        self._next_due[pipeline_id] = 0
        self.store.audit("ARM", "pipeline", pipeline_id, {"destination_range": [channel["output_min_ma"], channel["output_max_ma"]]})
        return self.get(pipeline_id)

    async def disarm(self, pipeline_id, *, disable=False):
        row = self.store.one("SELECT id,enabled FROM mappings WHERE id=?", (pipeline_id,))
        if not row:
            raise KeyError("Pipeline not found")
        detail = self.store.one("SELECT destination_node_id,destination_channel FROM mappings WHERE id=?", (pipeline_id,))
        async with self.output.lock_for(detail["destination_node_id"], detail["destination_channel"]):
            return self._finish_disarm(pipeline_id, row, disable)

    def _finish_disarm(self, pipeline_id, row, disable):
        status = "DISABLED" if disable else "DISARMED"
        with self.store.transaction() as db:
            db.execute("UPDATE mappings SET armed=0,enabled=?,status=?,updated_at=? WHERE id=?", (0 if disable else row["enabled"], status, utc_now(), pipeline_id))
        self._next_due.pop(pipeline_id, None)
        self.store.audit("DISABLE" if disable else "DISARM", "pipeline", pipeline_id, {})
        return self.get(pipeline_id)

    async def delete(self, pipeline_id):
        row = self.store.one("SELECT * FROM mappings WHERE id=?", (pipeline_id,))
        if not row:
            raise KeyError("Pipeline not found")
        if row["armed"]:
            raise PipelineError("Disarm the pipeline before deleting it")
        async with self.output.lock_for(row["destination_node_id"], row["destination_channel"]):
            latest=self.store.one("SELECT armed FROM mappings WHERE id=?",(pipeline_id,))
            if latest and latest["armed"]:
                raise PipelineError("Disarm the pipeline before deleting it")
            self._next_due.pop(pipeline_id, None)
            self.store.run("DELETE FROM mappings WHERE id=?", (pipeline_id,))
            self.store.audit("DELETE", "pipeline", pipeline_id, {"name": row["name"]})

    async def execute(self, pipeline_id):
        row = self.store.one("SELECT * FROM mappings WHERE id=? AND enabled=1 AND armed=1", (pipeline_id,))
        if not row:
            return
        config = json.loads(row["config_json"] or "{}")
        source = self.store.one("SELECT * FROM nodes WHERE id=?", (row["source_node_id"],))
        if not source or not source["polling_enabled"]:
            await self._source_invalid(row, config, "SOURCE_INVALID", "Source node is missing or polling is disabled")
            return
        gateway = self.store.one("SELECT transport_mode FROM gateways WHERE id=?", (source["gateway_id"],)) if source["gateway_id"] else None
        if not gateway or not self.sessions.for_gateway(source["gateway_id"]):
            self._disarm_unhealthy(row, "SOURCE_DISCONNECTED", "Source gateway disconnected; re-arm after reconnection")
            return
        measurement = self._latest_source(row, config)
        if not measurement:
            await self._source_invalid(row, config, "WAITING_FOR_SOURCE", "No source measurement is available")
            return
        age = self._age_seconds(measurement["measured_at"])
        if age is None or age * 1000 > int(config.get("stale_timeout_ms", 10000)):
            await self._source_invalid(row, config, "SOURCE_STALE", "Source measurement exceeded the configured maximum age")
            return
        if measurement["quality"] != "GOOD":
            await self._source_invalid(row, config, "SOURCE_INVALID", f"Source measurement quality is {measurement['quality']}")
            return
        value = self._source_value(measurement, row["source_value_type"])
        if value is None or not math.isfinite(value):
            await self._source_invalid(row, config, "SOURCE_INVALID", "Selected source value is unavailable or non-finite")
            return
        target = self._transform(value, config)
        if not math.isfinite(target):
            await self._source_invalid(row, config, "SOURCE_INVALID", "Transformed output target is not finite")
            return
        previous = row["last_acknowledged_value"]
        self._set_runtime(pipeline_id, "RUNNING", last_source_value=value, last_requested_value=target,
                          last_error=None, failsafe_attempted=0)
        if previous is not None and abs(target - previous) <= float(config.get("deadband_ma", 0)):
            return
        destination = self.store.one("SELECT * FROM nodes WHERE id=?", (row["destination_node_id"],))
        if not destination or destination["node_type"] != "ANALOG_OUTPUT" or not destination["polling_enabled"]:
            self._set_runtime(pipeline_id, "CONFIGURATION_ERROR", last_error="Destination output node is missing or disabled")
            return
        if not self.sessions.for_gateway(destination["gateway_id"]):
            self._disarm_unhealthy(row, "OUTPUT_DISCONNECTED", "Destination gateway disconnected; output command was not sent. Re-arm after reconnection")
            return
        attempts = int(config.get("retries", 1)) + 1
        for attempt in range(attempts):
            try:
                result = await self.output.set_output(row["destination_node_id"], row["destination_channel"], target,
                    source=f"PIPELINE:{pipeline_id}", pipeline_id=pipeline_id, timeout_ms=int(config.get("timeout_ms", 2000)))
                self._set_runtime(pipeline_id, "RUNNING", last_source_value=value,
                    last_requested_value=target, last_acknowledged_value=result["acknowledged_ma"],
                    last_success_at=result["updated_at"], last_attempt_at=result["updated_at"], last_error=None, failsafe_attempted=0)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                status = "OUTPUT_READBACK_MISMATCH" if "readback mismatch" in str(exc).lower() else "OUTPUT_READBACK_FAILED" if isinstance(exc, OutputReadbackError) else "OUTPUT_WRITE_FAILED"
                self._set_runtime(pipeline_id, status, last_source_value=value, last_requested_value=target,
                                  last_attempt_at=utc_now(), last_error=str(exc))
                if "not connected" in str(exc).lower() or "disconnected" in str(exc).lower():
                    self._disarm_unhealthy(row, "OUTPUT_DISCONNECTED", f"{exc}; re-arm after reconnection")
                    return
                if attempt + 1 < attempts and not isinstance(exc, OutputReadbackError):
                    await asyncio.sleep(min(0.25 * (2 ** attempt), 1.0))

    def _latest_source(self, pipeline, config):
        rows = self.store.rows("SELECT * FROM measurements WHERE node_id=? AND channel=? ORDER BY measured_at DESC,id DESC LIMIT 1", (pipeline["source_node_id"], pipeline["source_channel"]))
        if not rows:
            return None
        measurement = rows[0]
        channel = self.store.one("SELECT enabled FROM channels WHERE node_id=? AND channel_index=? AND channel_type='ANALOG_INPUT'", (pipeline["source_node_id"], pipeline["source_channel"]))
        if not channel or not channel["enabled"]:
            measurement["quality"] = "DISABLED"
            return measurement
        # Confirm the most recent enabled measurement polling item is still
        # responding. A recent historical row cannot hide a current poll error.
        candidates = self.store.rows("SELECT * FROM polling_items WHERE node_id=? AND enabled=1", (pipeline["source_node_id"],))
        mapped = []
        for item in candidates:
            try:
                metadata = json.loads(item.get("measurement_metadata_json") or "{}")
            except (TypeError, json.JSONDecodeError):
                continue
            if pipeline["source_channel"] in metadata.get("channel_indices", []):
                mapped.append(item)
        if not mapped:
            measurement["quality"] = "DISABLED"
            return measurement
        latest = max(mapped, key=lambda item: item.get("last_success_at") or "")
        if latest["last_status"] != "GOOD" or not latest.get("last_success_at"):
            measurement["quality"] = latest["last_status"] if latest["last_status"] != "GOOD" else "UNKNOWN"
            return measurement
        item_age = self._age_seconds(latest["last_success_at"])
        if item_age is None or item_age > max(30, (latest["interval_ms"] * 2 + latest["timeout_ms"]) / 1000):
            measurement["quality"] = "STALE"
        return measurement

    @staticmethod
    def _source_value(measurement, value_type):
        if value_type == "RAW_MA":
            return float(measurement["raw_ma"])
        if value_type == "CALIBRATED_MA":
            return float(measurement["calibrated_ma"])
        if value_type == "ENGINEERING":
            return float(measurement["engineering_value"]) if measurement.get("engineering_value") is not None else None
        return None

    @staticmethod
    def _transform(value, config):
        mode = config.get("transformation", "DIRECT")
        if mode == "DIRECT":
            return float(value)
        if mode == "GAIN_OFFSET":
            return float(value) * float(config["gain"]) + float(config["offset_ma"])
        if mode == "TWO_POINT":
            span = float(config["input_max"]) - float(config["input_min"])
            if not math.isfinite(span) or span == 0:
                raise PipelineError("Two-point input span must be finite and nonzero")
            return float(config["output_min_ma"]) + ((float(value) - float(config["input_min"])) / span) * (float(config["output_max_ma"]) - float(config["output_min_ma"]))
        raise PipelineError("Unsupported transformation")

    async def _source_invalid(self, row, config, status, detail):
        current = self.store.one("SELECT status,failsafe_attempted FROM mappings WHERE id=?", (row["id"],)) or {}
        self._set_runtime(row["id"], status, last_error=detail)
        if config.get("fail_safe_policy", "HOLD_LAST") != "SAFE_SETPOINT" or current.get("failsafe_attempted"):
            return
        try:
            safe = float(config["safe_setpoint_ma"])
            result = await self.output.set_output(row["destination_node_id"], row["destination_channel"], safe,
                source=f"PIPELINE_FAILSAFE:{row['id']}", pipeline_id=row["id"], timeout_ms=int(config.get("timeout_ms", 2000)))
            self._set_runtime(row["id"], status, last_requested_value=safe,
                last_acknowledged_value=result["acknowledged_ma"], last_success_at=result["updated_at"],
                last_attempt_at=result["updated_at"], last_error=f"{detail}; configured safe setpoint acknowledged",
                failsafe_attempted=1)
        except Exception as exc:
            disconnected = "not connected" in str(exc).lower() or "disconnected" in str(exc).lower()
            self._set_runtime(row["id"], "OUTPUT_DISCONNECTED" if disconnected else status,
                last_attempt_at=utc_now(), last_error=f"{detail}; safe setpoint was not confirmed: {exc}", failsafe_attempted=1)
            if disconnected:
                self._disarm_unhealthy(row, "OUTPUT_DISCONNECTED", "Safe setpoint could not be applied because the destination gateway is disconnected; re-arm after reconnection")

    def _disarm_unhealthy(self, row, status, detail):
        with self.store.transaction() as db:
            db.execute("UPDATE mappings SET armed=0,status=?,last_error=?,updated_at=? WHERE id=?", (status, detail, utc_now(), row["id"]))
        self._next_due.pop(row["id"], None)
        self.store.audit("AUTO_DISARM", "pipeline", row["id"], {"status": status, "reason": detail})

    def _set_runtime(self, pipeline_id, status, **values):
        allowed = {"last_source_value", "last_requested_value", "last_acknowledged_value", "last_success_at", "last_attempt_at", "last_error", "failsafe_attempted"}
        if set(values) - allowed:
            raise ValueError("unsupported runtime field")
        values["status"] = status
        values["updated_at"] = utc_now()
        assignments = ",".join(f"{key}=?" for key in values)
        self.store.run(f"UPDATE mappings SET {assignments} WHERE id=?", (*[int(v) if k == "failsafe_attempted" else v for k,v in values.items()], pipeline_id))

    @staticmethod
    def _age_seconds(value):
        try:
            stamp = datetime.fromisoformat(value)
            if stamp.tzinfo is None:
                stamp = stamp.replace(tzinfo=timezone.utc)
            age = (datetime.now(timezone.utc) - stamp).total_seconds()
            return age if age >= 0 else 0
        except (TypeError, ValueError):
            return None

    def history(self, pipeline_id, limit=100):
        row = self.store.one("SELECT id FROM mappings WHERE id=?", (pipeline_id,))
        if not row:
            raise KeyError("Pipeline not found")
        return self.store.rows("SELECT * FROM output_commands WHERE source IN (?,?) ORDER BY id DESC LIMIT ?", (f"PIPELINE:{pipeline_id}", f"PIPELINE_FAILSAFE:{pipeline_id}", min(max(int(limit), 1), 500)))

