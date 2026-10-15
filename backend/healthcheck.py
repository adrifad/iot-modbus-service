"""Read-only Modbus liveness checks for otherwise quiet gateway sessions."""

import asyncio
import os
import time

from .store import utc_now
from .node_lifecycle import NodeOperationError


def _int_env(name, default, minimum, maximum):
    try:
        return min(maximum, max(minimum, int(os.getenv(name, str(default)))))
    except (TypeError, ValueError):
        return default


class GatewayHealthMonitor:
    """Probe gateways only when no validated Modbus traffic is recent."""

    def __init__(self, store, sessions, output_driver):
        self.store = store
        self.sessions = sessions
        self.output = output_driver
        self.running = False
        self.task = None
        self.due: dict[str, float] = {}
        self.failures: dict[tuple[str, int], int] = {}
        self.in_flight: set[tuple[str, int]] = set()

    @property
    def enabled(self):
        return os.getenv("GATEWAY_HEALTHCHECK_ENABLED", "true").lower() not in {"0", "false", "no"}

    @property
    def interval(self):
        return _int_env("GATEWAY_HEALTHCHECK_INTERVAL_SECONDS", 30, 5, 3600)

    @property
    def timeout_ms(self):
        return _int_env("GATEWAY_HEALTHCHECK_TIMEOUT_MS", 5000, 250, 30000)

    @property
    def failure_threshold(self):
        return _int_env("GATEWAY_HEALTHCHECK_FAILURE_THRESHOLD", 3, 1, 20)

    def start(self):
        if self.enabled and (self.task is None or self.task.done()):
            self.running = True
            self.task = asyncio.create_task(self.loop(), name="gateway-health-monitor")

    async def stop(self):
        self.running = False
        if self.task:
            self.task.cancel()
            await asyncio.gather(self.task, return_exceptions=True)
            self.task = None

    async def loop(self):
        while self.running:
            now = time.monotonic()
            gateways = self.store.rows("SELECT id FROM gateways ORDER BY id")
            valid = {row["id"] for row in gateways}
            self.due = {key: value for key, value in self.due.items() if key in valid}
            self.failures = {key: value for key, value in self.failures.items() if key[0] in valid}
            for row in gateways:
                gateway_id = row["id"]
                if now < self.due.get(gateway_id, 0):
                    continue
                self.due[gateway_id] = now + self.interval
                await self.check_gateway(gateway_id)
            await asyncio.sleep(1)

    async def check_gateway(self, gateway_id):
        session = self.sessions.for_gateway(gateway_id)
        if not session:
            return
        key = (gateway_id, session.generation)
        for old_key in [item for item in self.failures if item[0] == gateway_id and item != key]:
            self.failures.pop(old_key, None)
        if key in self.in_flight:
            return
        # Normal polling or pipeline I/O is already stronger evidence than an
        # extra probe. This avoids competing with useful Modbus traffic.
        if session.last_request_at and time.perf_counter() - session.last_request_at < self.interval:
            return
        nodes = self.store.rows("SELECT * FROM nodes WHERE gateway_id=? AND polling_enabled=1 AND archived_at IS NULL ORDER BY id", (gateway_id,))
        if not nodes:
            return
        self.in_flight.add(key)
        try:
            previous_failures = self.failures.get(key, 0)
            for node in nodes:
                if self.sessions.for_gateway(gateway_id) is not session:
                    return
                if node["node_type"] == "ANALOG_INPUT":
                    if session.transport_mode != "MODBUS_TCP":
                        raise ConnectionError("Input ME31 health check requires MODBUS_TCP framing")
                    async with self.output.operations.operation(node["id"]):
                        values, frame, request, latency = await session.read_registers(node["slave_id"], 4, 0x0064, 6, self.timeout_ms)
                    detail = f"Read-only FC04 AI1-AI6 health check returned {len(values)} registers"
                    self.store.event("GATEWAY_HEALTHCHECK_RECOVERED" if previous_failures else "GATEWAY_HEALTHCHECK_OK", detail, gateway_id=gateway_id, node_id=node["id"], protocol=session.transport_mode,
                                     function_code=4, address=0x0064, quantity=6, latency_ms=latency, request_hex=request.hex(" "), response_hex=frame.hex(" "),session_id=session.session_id,session_generation=session.generation,identity_method=getattr(session,"commissioning_method",None),peer=getattr(session,"peer",None))
                elif node["node_type"] == "ANALOG_OUTPUT":
                    if session.transport_mode != "TRANSPARENT_SERIAL":
                        raise ConnectionError("Output ME31 health check requires transparent RTU-over-TCP framing")
                    snapshot = await self.output.inspect(node["id"], range_timeout_ms=self.timeout_ms, readback_timeout_ms=self.timeout_ms)
                    detail = "Read-only FC03 AO range and setpoint health check succeeded; reported setpoints are not physical current measurements"
                    self.store.event("GATEWAY_HEALTHCHECK_RECOVERED" if previous_failures else "GATEWAY_HEALTHCHECK_OK", detail, gateway_id=gateway_id, node_id=node["id"], protocol=session.transport_mode,
                                     function_code=3, address=0x0064, quantity=6, latency_ms=max(snapshot["latency_ms"]),session_id=session.session_id,session_generation=session.generation,identity_method=getattr(session,"commissioning_method",None),peer=getattr(session,"peer",None))
            self.failures[key] = 0
        except asyncio.CancelledError:
            raise
        except NodeOperationError:
            return
        except Exception as exc:
            count = self.failures.get(key, 0) + 1
            self.failures[key] = count
            self.store.event("GATEWAY_HEALTHCHECK_FAILED", f"Read-only Modbus health check {count}/{self.failure_threshold} failed: {exc}", gateway_id=gateway_id, protocol=session.transport_mode,session_id=session.session_id,session_generation=session.generation,identity_method=getattr(session,"commissioning_method",None),peer=getattr(session,"peer",None))
            if count >= self.failure_threshold and self.sessions.for_gateway(gateway_id) is session:
                previous = self.store.one("SELECT connection_state FROM gateways WHERE id=?", (gateway_id,))
                self.store.run("UPDATE gateways SET connection_state='DEGRADED',last_disconnect_reason=?,updated_at=? WHERE id=? AND last_session_id=?", (str(exc),utc_now(),gateway_id,session.session_id))
                if previous and previous["connection_state"] != "DEGRADED":
                    self.store.event("GATEWAY_DEGRADED", "TCP session remains present, but Modbus health checks exceeded the failure threshold", gateway_id=gateway_id, protocol=session.transport_mode)
        finally:
            self.in_flight.discard(key)
