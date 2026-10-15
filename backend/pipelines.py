"""Persistent, explicitly armed AI-to-AO pipeline validation and execution."""

import asyncio
import json
import math
import os
import time
from datetime import datetime, timezone

from .output import OutputOutcomeUnknown
from .node_lifecycle import NodeOperationError
from .store import utc_now


class PipelineError(ValueError):
    pass


def _bounded_environment_int(name: str, default: int, minimum: int, maximum: int) -> int:
    try:
        value = int(os.getenv(name, str(default)))
    except (TypeError, ValueError):
        value = default
    return max(minimum, min(maximum, value))


class PipelineService:
    TICK_SECONDS = 0.2
    MAX_CONCURRENT = _bounded_environment_int("PIPELINE_MAX_CONCURRENCY", 8, 1, 64)

    def __init__(self, store, sessions, output_driver):
        self.store = store
        self.sessions = sessions
        self.output = output_driver
        self.running = False
        self.task: asyncio.Task | None = None
        self._next_due: dict[str, float] = {}
        self._workers: dict[str, asyncio.Task] = {}
        self._pipeline_locks: dict[str, asyncio.Lock] = {}

    def _pipeline_lock(self, pipeline_id: str) -> asyncio.Lock:
        return self._pipeline_locks.setdefault(pipeline_id, asyncio.Lock())

    def start(self):
        if self.running:
            return
        # Process restart requires an operator to arm again. The configured
        # enabled state remains persistent, while no stale command is replayed.
        self.store.run("UPDATE mappings SET armed=0,status=CASE WHEN status='OUTPUT_FAULT_LOCKOUT' THEN status WHEN enabled=1 THEN 'DISARMED' ELSE 'DISABLED' END,last_error=CASE WHEN status='OUTPUT_FAULT_LOCKOUT' THEN last_error ELSE 'Backend restart disarmed output control; operator re-arm required' END")
        self.running = True
        self.task = asyncio.create_task(self.loop(), name="pipeline-executor")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None
        workers = list(self._workers.values())
        if workers:
            grace = _bounded_environment_int("PIPELINE_SHUTDOWN_GRACE_MS", 10000, 500, 30000) / 1000
            done, pending = await asyncio.wait(workers, timeout=grace)
            for task in pending:
                task.cancel()
            if pending:
                await asyncio.gather(*pending, return_exceptions=True)
        self._workers.clear()
        self.store.run("UPDATE mappings SET armed=0,status=CASE WHEN status='OUTPUT_FAULT_LOCKOUT' THEN status WHEN enabled=1 THEN 'DISARMED' ELSE 'DISABLED' END,last_error=CASE WHEN status='OUTPUT_FAULT_LOCKOUT' THEN last_error ELSE 'Backend shutdown disarmed output control; no safe setpoint was issued' END,updated_at=? WHERE armed=1", (utc_now(),))

    async def loop(self):
        while self.running:
            now = time.monotonic()
            rows = self.store.rows("SELECT * FROM mappings WHERE enabled=1 AND armed=1 ORDER BY created_at,id")
            active_ids = {row["id"] for row in rows}
            self._next_due = {key: value for key, value in self._next_due.items() if key in active_ids}
            self._workers = {key: task for key,task in self._workers.items() if not task.done()}
            for row in rows:
                if row["id"] in self._workers or now < self._next_due.get(row["id"], 0):
                    continue
                if len(self._workers) >= self.MAX_CONCURRENT:
                    break
                try:
                    config = json.loads(row["config_json"])
                    interval = max(1, int(config.get("update_interval_ms", 1000))) / 1000
                except (ValueError, TypeError, json.JSONDecodeError):
                    interval = 1
                self._next_due[row["id"]] = now + max(0.25, interval)
                task = asyncio.create_task(self._worker(row["id"]), name=f"pipeline:{row['id']}")
                self._workers[row["id"]] = task
            await asyncio.sleep(self.TICK_SECONDS)

    async def _worker(self, pipeline_id: str):
        current = asyncio.current_task()
        try:
            await self.execute(pipeline_id)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self._set_runtime(pipeline_id, "OUTPUT_WRITE_FAILED", last_error=str(exc), last_attempt_at=utc_now())
        finally:
            if self._workers.get(pipeline_id) is current:
                self._workers.pop(pipeline_id, None)

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
        src = self.store.one("SELECT * FROM nodes WHERE id=? AND archived_at IS NULL", (data["source_node_id"],))
        dst = self.store.one("SELECT * FROM nodes WHERE id=? AND archived_at IS NULL", (data["destination_node_id"],))
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
                        "update_interval_ms","deadband_ma","stale_timeout_ms","timeout_ms","retries","fail_safe_policy","safe_setpoint_ma",
                        "max_consecutive_write_failures","write_retry_backoff_ms"}
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
        timeout = int(config.get("timeout_ms", 5000))
        retries = int(config.get("retries", 1))
        max_failures = int(config.get("max_consecutive_write_failures", 3))
        retry_backoff = int(config.get("write_retry_backoff_ms", 300))
        if interval not in range(250, 60001):
            raise PipelineError("Update interval must be 250–60000 ms")
        if stale not in range(500, 3600001):
            raise PipelineError("Stale timeout must be 500 ms–1 hour")
        if timeout not in range(250, 30001):
            raise PipelineError("Output write timeout must be 250–30000 ms")
        if retries not in range(0, 4):
            raise PipelineError("Retry count must be 0–3")
        if max_failures not in range(1, 11):
            raise PipelineError("Maximum consecutive write failures must be 1–10")
        if retry_backoff not in range(100, 5001):
            raise PipelineError("Write retry backoff must be 100–5000 ms")
        fail_safe = config.get("fail_safe_policy", "HOLD_LAST")
        if fail_safe not in {"HOLD_LAST", "SAFE_SETPOINT"}:
            raise PipelineError("Fail-safe policy must be HOLD_LAST or SAFE_SETPOINT")
        if fail_safe == "SAFE_SETPOINT" and config.get("safe_setpoint_ma") is None:
            raise PipelineError("SAFE_SETPOINT requires a configured safe_setpoint_ma")
        return {**data, "name": name, "config": {**config, "transformation": mode,
                "update_interval_ms": interval, "stale_timeout_ms": stale, "timeout_ms": timeout,
                "retries": retries, "max_consecutive_write_failures": max_failures,
                "write_retry_backoff_ms": retry_backoff, "deadband_ma": float(config.get("deadband_ma", 0)),
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
        async with self._pipeline_lock(pipeline_id):
            return await self._arm_locked(pipeline_id, confirm)

    async def _arm_locked(self, pipeline_id, confirm):
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
        source_value = self._source_value(measurement, row["source_value_type"])
        if source_value is None or not math.isfinite(source_value):
            raise PipelineError("A finite source value is required before arming")
        try:
            target = self._transform(source_value, definition["config"])
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            raise PipelineError(f"Source transformation is invalid: {exc}") from exc
        if not math.isfinite(target):
            raise PipelineError("Transformed output target must be finite")
        destination_node = self.store.one("SELECT gateway_id FROM nodes WHERE id=?", (row["destination_node_id"],))
        destination_session_before = self.sessions.for_gateway(destination_node["gateway_id"]) if destination_node and destination_node["gateway_id"] else None
        if not destination_session_before:
            raise PipelineError("Destination gateway is not connected and commissioned")
        async with self.output.lock_for(row["destination_node_id"], row["destination_channel"]):
            snapshot = await self.output.inspect(row["destination_node_id"])
            channel = snapshot["channels"][row["destination_channel"] - 1]
            if not channel["range_known"]:
                raise PipelineError("Destination output range is unknown")
            if not channel["output_min_ma"] <= target <= channel["output_max_ma"]:
                raise PipelineError("Current source target is outside the destination's verified output range")
            # last_acknowledged_value may be from a previous process/session.
            # Seed deadband from this exact device readback and session
            # generation. The next scheduler tick writes if target differs.
            session = self.sessions.for_gateway(destination_node["gateway_id"])
            if session is not destination_session_before:
                raise PipelineError("Destination session changed during setpoint verification; retry arming after the new session is healthy")
            current_setpoint = float(channel["setpoint_ma"])
            with self.store.transaction() as db:
                owner = db.execute("SELECT id FROM mappings WHERE enabled=1 AND armed=1 AND destination_node_id=? AND destination_channel=? AND id<>?", (row["destination_node_id"], row["destination_channel"], pipeline_id)).fetchone()
                if owner:
                    raise PipelineError("Another enabled pipeline owns this AO channel")
                db.execute("UPDATE mappings SET armed=1,status='WAITING_FOR_SOURCE',last_error=NULL,failsafe_attempted=0,failsafe_status='NOT_APPLIED',consecutive_write_failures=0,faulted_at=NULL,last_output_verified_setpoint_ma=?,last_output_verified_session_id=?,last_output_verified_generation=?,last_output_verified_at=?,last_raw_ma=?,last_calibrated_ma=?,last_engineering_value=?,source_quality=?,updated_at=? WHERE id=? AND enabled=1", (current_setpoint,session.session_id,session.generation,utc_now(),measurement.get("raw_ma"),measurement.get("calibrated_ma"),measurement.get("engineering_value"),measurement.get("quality"),utc_now(),pipeline_id))
        self._next_due[pipeline_id] = 0
        self.store.audit("ARM", "pipeline", pipeline_id, {"destination_range": [channel["output_min_ma"], channel["output_max_ma"]], "reported_setpoint_ma": current_setpoint, "target_ma": target, "session_generation": session.generation})
        return self.get(pipeline_id)

    async def disarm(self, pipeline_id, *, disable=False):
        row = self.store.one("SELECT id,enabled FROM mappings WHERE id=?", (pipeline_id,))
        if not row:
            raise KeyError("Pipeline not found")
        detail = self.store.one("SELECT destination_node_id,destination_channel FROM mappings WHERE id=?", (pipeline_id,))
        async with self._pipeline_lock(pipeline_id):
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
        async with self._pipeline_lock(pipeline_id):
            latest=self.store.one("SELECT armed FROM mappings WHERE id=?",(pipeline_id,))
            if latest and latest["armed"]:
                raise PipelineError("Disarm the pipeline before deleting it")
            async with self.output.lock_for(row["destination_node_id"], row["destination_channel"]):
                self._next_due.pop(pipeline_id, None)
                self.store.run("DELETE FROM mappings WHERE id=?", (pipeline_id,))
                self.store.audit("DELETE", "pipeline", pipeline_id, {"name": row["name"]})

    async def execute(self, pipeline_id):
        async with self._pipeline_lock(pipeline_id):
            row = self.store.one("""SELECT p.* FROM mappings p JOIN nodes s ON s.id=p.source_node_id
                JOIN nodes d ON d.id=p.destination_node_id
                WHERE p.id=? AND p.enabled=1 AND p.armed=1 AND s.archived_at IS NULL AND d.archived_at IS NULL""", (pipeline_id,))
            if not row:
                return
            try:
                async with self.output.operations.operation(row["source_node_id"], row["destination_node_id"]):
                    await self._execute_locked(row)
            except NodeOperationError:
                return

    async def _execute_locked(self, row):
        pipeline_id = row["id"]
        config = json.loads(row["config_json"] or "{}")
        source = self.store.one("SELECT * FROM nodes WHERE id=?", (row["source_node_id"],))
        if not source or not source["polling_enabled"]:
            await self._transition_failure_locked(row, "SOURCE_INVALID", "Source node is missing or polling is disabled", source_failure=True)
            return
        if not source["gateway_id"] or not self.sessions.for_gateway(source["gateway_id"]):
            await self._transition_failure_locked(row, "SOURCE_DISCONNECTED", "Source gateway is disconnected or uncommissioned", source_failure=True)
            return
        measurement = self._latest_source(row, config)
        if not measurement:
            await self._transition_failure_locked(row, "WAITING_FOR_SOURCE", "No source measurement or polling channel mapping is available", source_failure=True)
            return
        age = self._age_seconds(measurement["measured_at"])
        if age is None or age * 1000 > int(config.get("stale_timeout_ms", 10000)):
            await self._transition_failure_locked(row, "SOURCE_STALE", "Source measurement exceeded the configured maximum age", source_failure=True, measurement=measurement)
            return
        if measurement["quality"] != "GOOD":
            await self._transition_failure_locked(row, "SOURCE_INVALID", f"Source measurement quality is {measurement['quality']}", source_failure=True, measurement=measurement)
            return
        try:
            value = self._source_value(measurement, row["source_value_type"])
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            await self._transition_failure_locked(row, "SOURCE_INVALID", f"Selected source value is unavailable: {exc}", source_failure=True, measurement=measurement)
            return
        if value is None or not math.isfinite(value):
            await self._transition_failure_locked(row, "SOURCE_INVALID", "Selected source value is unavailable or non-finite", source_failure=True, measurement=measurement)
            return
        try:
            target = self._transform(value, config)
        except (KeyError, TypeError, ValueError, OverflowError) as exc:
            await self._transition_failure_locked(row, "SOURCE_INVALID", f"Source transformation failed: {exc}", source_failure=True, measurement=measurement)
            return
        if not math.isfinite(target):
            await self._transition_failure_locked(row, "SOURCE_INVALID", "Transformed output target is not finite", source_failure=True, measurement=measurement)
            return
        destination = self.store.one("SELECT * FROM nodes WHERE id=?", (row["destination_node_id"],))
        if not destination or destination["node_type"] != "ANALOG_OUTPUT" or not destination["polling_enabled"]:
            await self._transition_failure_locked(row, "CONFIGURATION_ERROR", "Destination output node is missing or disabled", source_failure=False, destination_failure=True, measurement=measurement)
            return
        if not destination["gateway_id"] or not self.sessions.for_gateway(destination["gateway_id"]):
            await self._transition_failure_locked(row, "OUTPUT_DISCONNECTED", "Destination gateway is disconnected; output command was not sent", source_failure=False, destination_failure=True, measurement=measurement)
            return
        destination_session = self.sessions.for_gateway(destination["gateway_id"])
        if row.get("last_output_verified_generation") != destination_session.generation or row.get("last_output_verified_session_id") != destination_session.session_id:
            await self._transition_failure_locked(row, "OUTPUT_DISCONNECTED", "Destination session changed since live setpoint verification; operator re-arm is required", source_failure=False, destination_failure=True, measurement=measurement)
            return
        previous = row.get("last_output_verified_setpoint_ma")
        self._set_runtime(pipeline_id, "RUNNING", last_source_value=value, last_raw_ma=measurement.get("raw_ma"),
                          last_calibrated_ma=measurement.get("calibrated_ma"), last_engineering_value=measurement.get("engineering_value"),
                          source_quality=measurement.get("quality"), last_requested_value=target, last_error=None,
                          failsafe_attempted=0, failsafe_status="NOT_APPLIED")
        if previous is not None and abs(target - previous) <= float(config.get("deadband_ma", 0)):
            return
        attempts = int(config.get("retries", 1)) + 1
        last_error = None
        for attempt in range(attempts):
            try:
                result = await self.output.set_output(row["destination_node_id"], row["destination_channel"], target,
                    source=f"PIPELINE:{pipeline_id}", pipeline_id=pipeline_id,
                    write_timeout_ms=int(config.get("timeout_ms", 5000)))
                self._set_runtime(pipeline_id, "RUNNING", last_source_value=value, last_raw_ma=measurement.get("raw_ma"),
                    last_calibrated_ma=measurement.get("calibrated_ma"), last_engineering_value=measurement.get("engineering_value"),
                    source_quality=measurement.get("quality"), last_requested_value=target,
                    last_acknowledged_value=result["acknowledged_ma"], last_success_at=result["updated_at"],
                    last_output_verified_setpoint_ma=result["acknowledged_ma"],
                    last_output_verified_session_id=destination_session.session_id,
                    last_output_verified_generation=destination_session.generation,
                    last_output_verified_at=result["updated_at"],
                    last_attempt_at=result["updated_at"], last_communication_at=result["updated_at"], last_error=None,
                    consecutive_write_failures=0, last_failed_target=None, faulted_at=None)
                return
            except asyncio.CancelledError:
                raise
            except OutputOutcomeUnknown as exc:
                self._record_write_failure(row, target, "WRITE_OUTCOME_UNKNOWN", str(exc), measurement, unknown=True)
                return
            except Exception as exc:
                last_error = exc
                if "not connected" in str(exc).lower() or "disconnected" in str(exc).lower():
                    await self._transition_failure_locked(row, "OUTPUT_DISCONNECTED", str(exc), source_failure=False, destination_failure=True, measurement=measurement)
                    return
                if attempt + 1 < attempts:
                    await asyncio.sleep(int(config.get("write_retry_backoff_ms", 300)) / 1000)
        if last_error is not None:
            status = "OUTPUT_READBACK_MISMATCH" if "readback mismatch" in str(last_error).lower() else "OUTPUT_WRITE_FAILED"
            self._record_write_failure(row, target, status, str(last_error), measurement, unknown=False)

    async def gateway_disconnected(self, gateway_id: str, reason: str, *, operator_initiated=False):
        rows = self.store.rows("""SELECT DISTINCT p.id,s.gateway_id source_gateway,d.gateway_id destination_gateway
            FROM mappings p JOIN nodes s ON s.id=p.source_node_id JOIN nodes d ON d.id=p.destination_node_id
            WHERE p.enabled=1 AND p.armed=1 AND (s.gateway_id=? OR d.gateway_id=?)""", (gateway_id,gateway_id))
        # Process a bounded number at once. GatewaySession serializes each bus;
        # this avoids creating a task per affected mapping on a disconnect.
        for item in rows:
            source_failed = item["source_gateway"] == gateway_id
            destination_failed = item["destination_gateway"] == gateway_id
            label = "operator disconnected" if operator_initiated else "connection lost"
            detail = f"Gateway {gateway_id} {label}: {reason}"
            status = "SOURCE_DISCONNECTED" if source_failed else "OUTPUT_DISCONNECTED"
            if source_failed and destination_failed:
                status = "SOURCE_DISCONNECTED"
            await self.transition_failure(item["id"], status, detail,
                source_failure=source_failed, destination_failure=destination_failed)

    async def transition_failure(self, pipeline_id, status, detail, *, source_failure, destination_failure=False, measurement=None):
        async with self._pipeline_lock(pipeline_id):
            row = self.store.one("SELECT * FROM mappings WHERE id=? AND enabled=1 AND armed=1", (pipeline_id,))
            if not row:
                return
            await self._transition_failure_locked(row, status, detail, source_failure=source_failure,
                destination_failure=destination_failure, measurement=measurement)

    async def _transition_failure_locked(self, row, status, detail, *, source_failure, destination_failure=False, measurement=None):
        pipeline_id = row["id"]
        current = self.store.one("SELECT * FROM mappings WHERE id=?", (pipeline_id,))
        if not current or not current["enabled"] or not current["armed"]:
            return
        config = json.loads(current["config_json"] or "{}")
        policy = config.get("fail_safe_policy", "HOLD_LAST")
        dest = self.store.one("SELECT gateway_id FROM nodes WHERE id=?", (current["destination_node_id"],))
        destination_session = self.sessions.for_gateway(dest["gateway_id"]) if dest and dest["gateway_id"] else None
        source_session = None
        source = self.store.one("SELECT gateway_id FROM nodes WHERE id=?", (current["source_node_id"],))
        if source and source["gateway_id"]:
            source_session = self.sessions.for_gateway(source["gateway_id"])
        if destination_failure or not destination_session:
            detail += "; destination communication is unavailable"
            safe_requested = source_failure and policy == "SAFE_SETPOINT"
            final_status = "FAILSAFE_UNCONFIRMED" if safe_requested else "OUTPUT_DISCONNECTED"
            self._finish_fault(current, final_status, detail, failsafe_status="UNCONFIRMED" if safe_requested else "NOT_APPLIED", measurement=measurement)
            self._log_transition(current, final_status, detail, classification="FAILSAFE_UNCONFIRMED" if safe_requested else "DESTINATION_FAILURE")
            return
        if source_failure and policy == "SAFE_SETPOINT" and not current["failsafe_attempted"]:
            self._set_runtime(pipeline_id, "FAILSAFE_PENDING", failsafe_attempted=1, failsafe_status="PENDING",
                last_error=detail, last_attempt_at=utc_now(), source_quality=measurement.get("quality") if measurement else "DISCONNECTED")
            self._log_transition(current, "FAILSAFE_PENDING", detail, classification="FAILSAFE_REQUESTED")
            safe = float(config.get("safe_setpoint_ma", 0))
            try:
                result = await self.output.set_output(current["destination_node_id"], current["destination_channel"], safe,
                    source=f"PIPELINE_FAILSAFE:{pipeline_id}", pipeline_id=pipeline_id,
                    write_timeout_ms=int(config.get("timeout_ms", 5000)))
                confirmed_detail = f"Fail-safe setpoint {safe:.3f} mA was acknowledged and reported by the module; physical current was not measured"
                self._finish_fault(current, "FAILSAFE_ACKNOWLEDGED", f"{detail}; {confirmed_detail}", failsafe_status="ACKNOWLEDGED",
                    measurement=measurement, target=safe, acknowledged=result["acknowledged_ma"], at=result["updated_at"])
                self._log_transition(current, "FAILSAFE_ACKNOWLEDGED", confirmed_detail, classification="FAILSAFE_ACKNOWLEDGED")
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                unknown = isinstance(exc, OutputOutcomeUnknown)
                state = "FAILSAFE_UNCONFIRMED"
                explanation = f"{detail}; fail-safe request was not confirmed: {exc}"
                if unknown:
                    explanation += "; output setpoint may have changed"
                self._finish_fault(current, state, explanation, failsafe_status="UNCONFIRMED", measurement=measurement, target=safe)
                self._log_transition(current, state, explanation, classification="FAILSAFE_UNCONFIRMED")
            return
        if source_failure and not source_session:
            detail += "; source gateway is disconnected"
        fail_status = "SOURCE_DISCONNECTED" if status == "SOURCE_DISCONNECTED" else status
        self._finish_fault(current, fail_status, detail, failsafe_status="HELD_LAST" if policy == "HOLD_LAST" else "UNCONFIRMED", measurement=measurement)
        self._log_transition(current, fail_status, detail, classification="SOURCE_FAILURE" if source_failure else "DESTINATION_FAILURE")

    def _finish_fault(self, row, status, detail, *, failsafe_status, measurement=None, target=None, acknowledged=None, at=None):
        now = at or utc_now()
        values = {
            "armed": 0, "status": status, "failsafe_status": failsafe_status, "last_error": detail,
            "last_attempt_at": now, "updated_at": utc_now(),
        }
        if target is not None:
            values["last_requested_value"] = target
        if acknowledged is not None:
            values["last_acknowledged_value"] = acknowledged
            values["last_success_at"] = now
            values["last_communication_at"] = now
        if measurement:
            values["source_quality"] = measurement.get("quality")
            values["last_raw_ma"] = measurement.get("raw_ma")
            values["last_calibrated_ma"] = measurement.get("calibrated_ma")
            values["last_engineering_value"] = measurement.get("engineering_value")
        assignments = ",".join(f"{key}=?" for key in values)
        self.store.run(f"UPDATE mappings SET {assignments} WHERE id=?", (*values.values(), row["id"]))
        self._next_due.pop(row["id"], None)
        self.store.audit("PIPELINE_FAULT", "pipeline", row["id"], {"status": status, "detail": detail, "failsafe_status": failsafe_status})

    def _record_write_failure(self, row, target, status, detail, measurement, *, unknown):
        latest = self.store.one("SELECT consecutive_write_failures,config_json FROM mappings WHERE id=?", (row["id"],))
        count = int(latest["consecutive_write_failures"] or 0) + 1
        config = json.loads(latest["config_json"] or "{}")
        maximum = int(config.get("max_consecutive_write_failures", 3))
        lockout = unknown or count >= maximum
        final = "WRITE_OUTCOME_UNKNOWN" if unknown else "OUTPUT_FAULT_LOCKOUT" if count >= maximum else status
        values = {"consecutive_write_failures": count, "last_failed_target": target, "last_attempt_at": utc_now(),
                  "last_requested_value": target, "last_error": detail, "status": final,
                  "faulted_at": utc_now() if lockout else None, "armed": 0 if lockout else 1,
                  "updated_at": utc_now()}
        if measurement:
            values.update({"source_quality": measurement.get("quality"), "last_source_value": self._source_value(measurement, row["source_value_type"]),
                           "last_raw_ma": measurement.get("raw_ma"), "last_calibrated_ma": measurement.get("calibrated_ma"),
                           "last_engineering_value": measurement.get("engineering_value")})
        assignments = ",".join(f"{key}=?" for key in values)
        self.store.run(f"UPDATE mappings SET {assignments} WHERE id=?", (*values.values(), row["id"]))
        if lockout:
            self._next_due.pop(row["id"], None)
        else:
            self._next_due[row["id"]] = time.monotonic() + int(config.get("write_retry_backoff_ms", 300)) / 1000
        self._log_transition(row, final, detail, classification="WRITE_OUTCOME_UNKNOWN" if unknown else "OUTPUT_FAULT")

    def _log_transition(self, row, status, detail, *, classification):
        self.store.event(status, detail, node_id=row["destination_node_id"], pipeline_id=row["id"],
            source_node_id=row["source_node_id"], source_channel=row["source_channel"],
            destination_node_id=row["destination_node_id"], destination_channel=row["destination_channel"],
            classification=classification)

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

    def _set_runtime(self, pipeline_id, status, **values):
        allowed = {"last_source_value", "last_raw_ma", "last_calibrated_ma", "last_engineering_value", "source_quality",
                   "last_requested_value", "last_acknowledged_value", "last_success_at", "last_attempt_at", "last_communication_at",
                   "last_error", "failsafe_attempted", "failsafe_status", "consecutive_write_failures", "last_failed_target", "faulted_at",
                   "last_output_verified_session_id", "last_output_verified_generation", "last_output_verified_at", "last_output_verified_setpoint_ma"}
        if set(values) - allowed:
            raise ValueError("unsupported runtime field")
        values["status"] = status
        values["updated_at"] = utc_now()
        assignments = ",".join(f"{key}=?" for key in values)
        self.store.run(f"UPDATE mappings SET {assignments} WHERE id=?", (*[int(v) if k in {"failsafe_attempted", "consecutive_write_failures"} else v for k,v in values.items()], pipeline_id))

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

