"""Persistent polling loop with per-item due times and per-gateway serialized transactions."""

import asyncio
import json
import os
import time

from .calibration import Calibration, Scaling, calibrate, scale
from .modbus import ModbusExceptionResponse, ModbusError, decode_registers
from .store import utc_now


class PollScheduler:
    def __init__(self, store, sessions):
        self.store = store
        self.sessions = sessions
        self.task = None
        self.due = {}
        self.active = set()
        self.tasks = {}
        self.running = False
        self.success = 0
        self.failed = 0
        self.latencies = []
        self.last_cleanup = 0.0

    def start(self):
        if not self.task:
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

    async def loop(self):
        while self.running:
            if time.monotonic() - self.last_cleanup > 3600:
                event_days=int(os.getenv("EVENT_RETENTION_DAYS","90"))
                measurement_days=int(os.getenv("MEASUREMENT_RETENTION_DAYS","365"))
                self.store.run("DELETE FROM events WHERE at < datetime('now',?)",(f"-{event_days} days",))
                self.store.run("DELETE FROM measurements WHERE measured_at < datetime('now',?)",(f"-{measurement_days} days",))
                self.last_cleanup=time.monotonic()
            items = self.store.rows("SELECT p.*,n.gateway_id,n.slave_id,n.polling_enabled FROM polling_items p JOIN nodes n ON n.id=p.node_id WHERE p.enabled=1 AND n.polling_enabled=1 ORDER BY p.node_id,p.id")
            current = {item["id"] for item in items}
            for item_id, task in list(self.tasks.items()):
                if item_id not in current:
                    task.cancel()
            self.active.intersection_update(current)
            now = time.monotonic()
            for item in items:
                if item["id"] in self.active or now < self.due.get(item["id"], 0):
                    continue
                self.active.add(item["id"])
                self.due[item["id"]] = now + item["interval_ms"] / 1000
                self.tasks[item["id"]] = asyncio.create_task(self.poll(item), name=f"poll-{item['id']}")
            await asyncio.sleep(0.1)

    async def poll(self, item):
        started = time.perf_counter()
        status, error = "UNKNOWN", ""
        try:
            session = self.sessions.for_gateway(item["gateway_id"])
            if session is None:
                raise ConnectionError("no commissioned gateway session")
            values = None
            for attempt in range(item["retries"] + 1):
                try:
                    values, frame, request, latency = await session.read_registers(
                        item["slave_id"], item["function_code"], item["address"], item["quantity"], item["timeout_ms"])
                    break
                except (TimeoutError, ConnectionError, ValueError, ModbusError) as exc:
                    error = str(exc)
                    if isinstance(exc, ModbusExceptionResponse):
                        raise
                    if attempt >= item["retries"]:
                        raise
            status = "GOOD"
            self.success += 1
            self.latencies.append(latency)
            self.latencies = self.latencies[-500:]
            now = utc_now()
            decoded_raw = decode_registers(values, item["data_type"], byte_order=item["byte_order"], word_order=item["word_order"])
            decoded = ([value * item["scale"] for value in decoded_raw]
                       if isinstance(decoded_raw, list) and decoded_raw and isinstance(decoded_raw[0], (int, float))
                       else decoded_raw * item["scale"] if isinstance(decoded_raw, (int, float)) else decoded_raw)
            value_json = json.dumps(decoded)
            self.store.run("UPDATE polling_items SET last_value=?,last_success_at=?,last_latency_ms=?,last_status='GOOD',updated_at=? WHERE id=?",
                           (value_json, now, latency, now, item["id"]))
            self.store.run("UPDATE gateways SET last_activity_at=? WHERE id=?", (now, item["gateway_id"]))
            channels = []
            if item["register_name"] == "AI current block" and item["data_type"] == "uint16_array":
                channels = [(channel, values[channel - 1], values[channel - 1] * item["scale"], [values[channel - 1]]) for channel in range(1, min(6, len(values)) + 1)]
            elif item["register_name"].startswith("AI") and item["data_type"] == "uint16":
                channel = int(item["register_name"][2])
                channels = [(channel, values[0], decoded, values)]
            for channel, register_value, raw_ma, raw_registers in channels:
                latest = self.store.one("SELECT gain,offset_ma,enabled,revision,scaling_json FROM calibrations WHERE node_id=? AND channel=? ORDER BY revision DESC LIMIT 1", (item["node_id"], channel))
                cal = Calibration(latest["gain"], latest["offset_ma"], bool(latest["enabled"])) if latest else Calibration()
                calibrated = calibrate(raw_ma, cal)
                engineering, quality = None, "GOOD"
                if latest and latest["scaling_json"]:
                    scaling = Scaling(**json.loads(latest["scaling_json"]))
                    engineering, quality = scale(calibrated, scaling)
                self.store.run("INSERT INTO measurements(node_id,channel,register_value,raw_registers_json,raw_ma,calibration_revision,calibrated_ma,engineering_value,engineering_unit,quality,measured_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
                               (item["node_id"],channel,register_value,json.dumps(raw_registers),raw_ma,latest["revision"] if latest else 0,calibrated,engineering,scaling.unit if latest and latest["scaling_json"] else None,quality,now))
            self.store.event("SUCCESS", "Validated Modbus read", gateway_id=item["gateway_id"],node_id=item["node_id"],direction="RX",protocol=session.transport_mode,function_code=item["function_code"],address=item["address"],quantity=item["quantity"],latency_ms=latency,request_hex=request.hex(" "),response_hex=frame.hex(" "))
        except Exception as exc:
            self.failed += 1
            status = "MODBUS_ERROR" if isinstance(exc, ModbusExceptionResponse) else "TIMEOUT" if isinstance(exc, TimeoutError) else "DISCONNECTED" if isinstance(exc, ConnectionError) else "INVALID_RESPONSE"
            error = str(exc)
            now = utc_now()
            self.store.run("UPDATE polling_items SET last_status=?,error_count=error_count+1,updated_at=? WHERE id=?", (status,now,item["id"]))
            self.store.event(status,error,gateway_id=item["gateway_id"],node_id=item["node_id"],direction="RX",protocol="MODBUS",function_code=item["function_code"],address=item["address"],quantity=item["quantity"])
        finally:
            self.active.discard(item["id"])
            if self.tasks.get(item["id"]) is asyncio.current_task():
                self.tasks.pop(item["id"], None)

    async def read_once(self, item):
        if item["id"] in self.active:
            return
        self.active.add(item["id"])
        return await self.poll(item)
