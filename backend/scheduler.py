"""Persistent polling scheduler with explicit decoding and per-gateway batching."""

import asyncio
import json
import math
import os
import time
from datetime import datetime, timedelta, timezone

from .calibration import Calibration, Scaling, calibrate, scale
from .modbus import ModbusError, ModbusExceptionResponse, decode_registers
from .node_lifecycle import NodeOperationError, NodeOperationGate
from .store import utc_now


class PollScheduler:
    def __init__(self, store, sessions, operations: NodeOperationGate | None = None):
        self.store = store
        self.sessions = sessions
        self.operations = operations or NodeOperationGate(store)
        self.task = None
        self.due: dict[str, float] = {}
        self.intervals: dict[str, int] = {}
        # One asyncio task may own several item IDs when their reads are batched.
        self.tasks: dict[str, asyncio.Task] = {}
        self.running = False
        self.success = 0
        self.failed = 0
        self.latencies: list[float] = []
        self.last_cleanup = 0.0
        self.paused_nodes: set[str] = set()

    def start(self):
        if self.task is None or self.task.done():
            self.running = True
            self.task = asyncio.create_task(self.loop(), name="modbus-poll-scheduler")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
            try:
                await self.task
            except asyncio.CancelledError:
                pass
            self.task = None
        workers = {task for task in self.tasks.values() if not task.done() and task is not asyncio.current_task()}
        for task in workers:
            task.cancel()
        if workers:
            await asyncio.gather(*workers, return_exceptions=True)
        self.tasks.clear()

    def cancel_item(self, item_id: str):
        task = self.tasks.get(item_id)
        if task and not task.done():
            task.cancel()
        self.due.pop(item_id, None)
        self.intervals.pop(item_id, None)

    def cancel_node(self, node_id: str):
        ids = {row["id"] for row in self.store.rows("SELECT id FROM polling_items WHERE node_id=?", (node_id,))}
        for item_id in ids:
            task = self.tasks.get(item_id)
            if task and not task.done():
                task.cancel()
            self.due.pop(item_id, None)
            self.intervals.pop(item_id, None)

    def pause_node(self, node_id: str):
        """Stop scheduling new work without cancelling an active RTU request."""
        self.paused_nodes.add(node_id)
        for row in self.store.rows("SELECT id FROM polling_items WHERE node_id=?", (node_id,)):
            self.due.pop(row["id"], None)
            self.intervals.pop(row["id"], None)

    def resume_node(self, node_id: str):
        self.paused_nodes.discard(node_id)

    async def loop(self):
        while self.running:
            now_mono = time.monotonic()
            if now_mono - self.last_cleanup > 3600:
                self._cleanup_retention()
                self.last_cleanup = now_mono

            items = self.store.rows("""
                SELECT p.*,n.gateway_id,n.slave_id,n.polling_enabled
                FROM polling_items p JOIN nodes n ON n.id=p.node_id
                WHERE p.enabled=1 AND n.polling_enabled=1 AND n.archived_at IS NULL
                ORDER BY p.node_id,p.address,p.id
            """)
            current = {item["id"] for item in items}
            for item_id, task in list(self.tasks.items()):
                if item_id not in current and not task.done():
                    task.cancel()
                if task.done() and self.tasks.get(item_id) is task:
                    self.tasks.pop(item_id, None)
            for item_id in list(self.due):
                if item_id not in current:
                    self.due.pop(item_id, None)
                    self.intervals.pop(item_id, None)

            due_items = []
            for item in items:
                item_id = item["id"]
                if item["node_id"] in self.paused_nodes:
                    continue
                task = self.tasks.get(item_id)
                interval = item["interval_ms"]
                if self.intervals.get(item_id) != interval:
                    self.intervals[item_id] = interval
                    self.due[item_id] = now_mono
                if task and not task.done():
                    continue
                if now_mono < self.due.get(item_id, 0):
                    continue
                self.due[item_id] = now_mono + interval / 1000
                due_items.append(item)

            for group in self._batch_due_items(due_items):
                task = asyncio.create_task(self._poll_group(group), name=f"poll-{group[0]['id']}")
                for item in group:
                    self.tasks[item["id"]] = task
            await asyncio.sleep(0.1)

    def _cleanup_retention(self):
        now = datetime.now(timezone.utc)
        event_cutoff = (now - timedelta(days=max(1, int(os.getenv("EVENT_RETENTION_DAYS", "90"))))).isoformat()
        measurement_cutoff = (now - timedelta(days=max(1, int(os.getenv("MEASUREMENT_RETENTION_DAYS", "365"))))).isoformat()
        with self.store.transaction() as db:
            db.execute("DELETE FROM events WHERE at < ? AND (node_id IS NULL OR node_id IN (SELECT id FROM nodes WHERE archived_at IS NULL))", (event_cutoff,))
            db.execute("DELETE FROM measurements WHERE measured_at < ? AND node_id IN (SELECT id FROM nodes WHERE archived_at IS NULL)", (measurement_cutoff,))

    @staticmethod
    def _can_batch(item):
        try:
            metadata = json.loads(item.get("measurement_metadata_json") or "{}")
        except (TypeError, json.JSONDecodeError):
            return False
        return (item["function_code"] == 4 and item["quantity"] == 1 and item["data_type"] == "uint16"
                and metadata.get("type") == "analog_current" and metadata.get("conversion") == "register_scale"
                and len(metadata.get("channel_indices", [])) == 1)

    def _batch_due_items(self, items):
        """Batch only contiguous compatible ME31 integer measurement registers."""
        ordered = sorted(items, key=lambda row: (row.get("gateway_id") or "", row["node_id"], row["slave_id"], row["function_code"], row["address"]))
        groups = []
        index = 0
        while index < len(ordered):
            first = ordered[index]
            if not self._can_batch(first):
                groups.append([first]); index += 1; continue
            key = (first["gateway_id"], first["node_id"], first["slave_id"], first["function_code"], first["interval_ms"], first["timeout_ms"], first["retries"], first["scale"], first["profile_id"])
            batch = [first]
            cursor = index + 1
            last_address = first["address"]
            while cursor < len(ordered):
                candidate = ordered[cursor]
                candidate_key = (candidate["gateway_id"], candidate["node_id"], candidate["slave_id"], candidate["function_code"], candidate["interval_ms"], candidate["timeout_ms"], candidate["retries"], candidate["scale"], candidate["profile_id"])
                if not self._can_batch(candidate) or candidate_key != key or candidate["address"] != last_address + 1:
                    break
                if len(batch) >= 125:
                    break
                batch.append(candidate)
                last_address = candidate["address"]
                cursor += 1
            groups.append(batch)
            index = cursor if len(batch) > 1 else index + 1
        return groups

    async def _poll_group(self, items):
        current = asyncio.current_task()
        node_id = items[0]["node_id"]
        try:
            async with self.operations.operation(node_id):
                await self._poll_group_active(items)
        except NodeOperationError:
            # An archive barrier has begun. The request was never sent.
            return False
        finally:
            for item in items:
                if self.tasks.get(item["id"]) is current:
                    self.tasks.pop(item["id"], None)

    async def _poll_group_active(self, items):
        first = items[0]
        session = None
        try:
            session = self.sessions.for_gateway(first["gateway_id"])
            if session is None:
                raise ConnectionError("no commissioned gateway session")
            address = first["address"]
            quantity = sum(item["quantity"] for item in items)
            values = None
            for attempt in range(first["retries"] + 1):
                try:
                    values, frame, request, latency = await session.read_registers(
                        first["slave_id"], first["function_code"], address, quantity, first["timeout_ms"])
                    break
                except (TimeoutError, ConnectionError, ValueError, ModbusError) as exc:
                    if isinstance(exc, ModbusExceptionResponse) or attempt >= first["retries"]:
                        raise
            self.success += 1
            self.latencies.append(latency)
            self.latencies = self.latencies[-500:]
            now = utc_now()
            processed = []
            offset = 0
            errors = []
            measurement_errors = []
            for item in items:
                raw_values = values[offset:offset + item["quantity"]]
                offset += item["quantity"]
                try:
                    decoded = decode_registers(raw_values, item["data_type"], byte_order=item["byte_order"], word_order=item["word_order"])
                    value_json = json.dumps(self._scale_register_value(decoded, item["scale"]), allow_nan=False)
                except (ModbusError, ValueError, TypeError, OverflowError) as exc:
                    errors.append((item, f"{item['register_name']}: {exc}"))
                    continue
                try:
                    measurements = self._measurement_rows(item, raw_values, decoded, now)
                except Exception as exc:
                    measurements = None
                    measurement_errors.append((item, str(exc)))
                processed.append((item, value_json, measurements))
            with self.store.transaction() as db:
                db.execute("UPDATE gateways SET last_activity_at=? WHERE id=?", (now, first["gateway_id"]))
                for item, value_json, measurements in processed:
                    item_status = "MEASUREMENT_ERROR" if measurements is None else "GOOD"
                    db.execute("UPDATE polling_items SET last_value=?,last_success_at=?,last_latency_ms=?,last_status=?,error_count=error_count+?,updated_at=? WHERE id=?",
                               (value_json, now, latency, item_status, int(measurements is None), now, item["id"]))
                    if measurements:
                        db.executemany("""INSERT INTO measurements(node_id,channel,register_value,raw_registers_json,raw_ma,calibration_revision,calibrated_ma,engineering_value,engineering_unit,quality,measured_at)
                            VALUES(?,?,?,?,?,?,?,?,?,?,?)""", measurements)
                for item, detail in errors:
                    db.execute("UPDATE polling_items SET last_status='DECODE_ERROR',error_count=error_count+1,updated_at=? WHERE id=?", (now,item["id"]))
            self.store.event("SUCCESS", "Validated Modbus read", gateway_id=first["gateway_id"],node_id=first["node_id"],direction="RX",protocol=session.transport_mode,function_code=first["function_code"],address=address,quantity=quantity,latency_ms=latency,request_hex=request.hex(" "),response_hex=frame.hex(" "))
            for item, detail in errors:
                self.failed += 1
                self.store.event("DECODE_ERROR",detail,gateway_id=item["gateway_id"],node_id=item["node_id"],direction="RX",protocol=session.transport_mode,function_code=item["function_code"],address=item["address"],quantity=item["quantity"],latency_ms=latency,request_hex=request.hex(" "),response_hex=frame.hex(" "))
            for item, detail in measurement_errors:
                self.failed += 1
                self.store.event("MEASUREMENT_PROCESSING_ERROR",detail,gateway_id=item["gateway_id"],node_id=item["node_id"],direction="RX",protocol=session.transport_mode,function_code=item["function_code"],address=item["address"],quantity=item["quantity"],latency_ms=latency,request_hex=request.hex(" "),response_hex=frame.hex(" "))
            return
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            self.failed += 1
            status = self._error_status(exc)
            now = utc_now()
            with self.store.transaction() as db:
                for item in items:
                    db.execute("UPDATE polling_items SET last_status=?,error_count=error_count+1,updated_at=? WHERE id=?", (status,now,item["id"]))
            self.store.event(status,str(exc),gateway_id=first["gateway_id"],node_id=first["node_id"],direction="RX",protocol=session.transport_mode if session else "MODBUS",function_code=first["function_code"],address=first["address"],quantity=sum(item["quantity"] for item in items))
        finally:
            current = asyncio.current_task()
            for item in items:
                if self.tasks.get(item["id"]) is current:
                    self.tasks.pop(item["id"], None)

    @staticmethod
    def _error_status(exc):
        if isinstance(exc, ModbusExceptionResponse): return "MODBUS_ERROR"
        if isinstance(exc, TimeoutError): return "TIMEOUT"
        if isinstance(exc, ConnectionError): return "DISCONNECTED"
        if isinstance(exc, ModbusError): return "INVALID_RESPONSE"
        return "POLL_ERROR"

    @staticmethod
    def _scale_register_value(value, factor):
        if isinstance(value, list):
            return [item * factor if isinstance(item, (int, float)) and not isinstance(item, bool) else item for item in value]
        return value * factor if isinstance(value, (int, float)) and not isinstance(value, bool) else value

    def _measurement_rows(self, item, raw_values, decoded, measured_at):
        try:
            metadata = json.loads(item.get("measurement_metadata_json") or "{}")
        except (TypeError, json.JSONDecodeError) as exc:
            raise ValueError("measurement metadata is invalid") from exc
        if not metadata:
            return []
        if metadata.get("type") != "analog_current" or metadata.get("conversion") != "register_scale":
            raise ValueError("unsupported measurement metadata")
        channels = metadata.get("channel_indices")
        registers_per_channel = metadata.get("registers_per_channel", 1)
        values = decoded if isinstance(decoded, list) else [decoded]
        if not isinstance(channels, list) or len(values) != len(channels):
            raise ValueError("decoded channel count does not match register metadata")
        enabled = {row["channel_index"] for row in self.store.rows("SELECT channel_index FROM channels WHERE node_id=? AND channel_type='ANALOG_INPUT' AND enabled=1", (item["node_id"],))}
        records = []
        for index, channel in enumerate(channels):
            if channel not in range(1, 7):
                raise ValueError("measurement channel index is outside ME31 AI1–AI6")
            if channel not in enabled:
                continue
            value = float(values[index]) * item["scale"]
            if not math.isfinite(value):
                raise ValueError("analog current is not finite")
            latest = self.store.one("SELECT gain,offset_ma,enabled,revision,scaling_json FROM calibrations WHERE node_id=? AND channel=? ORDER BY revision DESC LIMIT 1", (item["node_id"], channel))
            config = Calibration(latest["gain"],latest["offset_ma"],bool(latest["enabled"])) if latest else Calibration()
            corrected = calibrate(value,config)
            engineering, quality, engineering_unit = None, "GOOD", None
            if latest and latest["scaling_json"]:
                scaling = Scaling(**json.loads(latest["scaling_json"]))
                engineering, quality = scale(corrected,scaling)
                engineering_unit = scaling.unit
            start = index * registers_per_channel
            channel_registers = raw_values[start:start + registers_per_channel]
            if len(channel_registers) != registers_per_channel:
                raise ValueError("raw register count does not match channel mapping")
            records.append((item["node_id"],channel,int(channel_registers[0]),json.dumps(channel_registers),value,latest["revision"] if latest else 0,corrected,engineering,engineering_unit,quality,measured_at))
        return records

    async def poll(self, item):
        """One-shot read path used by API callers and protocol integration tests."""
        return await self._poll_group([item])

    async def read_once(self, item):
        current = self.tasks.get(item["id"])
        if current and not current.done():
            await asyncio.shield(current)
            return
        task = asyncio.current_task()
        self.tasks[item["id"]] = task
        result = await self.poll(item)
        if result is False:
            raise NodeOperationError("Node archival is in progress; no Modbus request was sent")
        return result
