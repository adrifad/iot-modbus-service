"""Allowlisted EBYTE ME31-XXXA0006 analog output operations."""

import asyncio
import math

from .store import utc_now


class OutputError(ValueError):
    pass


class OutputDriver:
    READBACK_TOLERANCE_MA = 0.05

    def __init__(self, store, sessions):
        self.store = store
        self.sessions = sessions
        self._locks: dict[tuple[str, int], asyncio.Lock] = {}

    def lock_for(self, node_id: str, channel: int) -> asyncio.Lock:
        return self._locks.setdefault((node_id, channel), asyncio.Lock())

    def _node(self, node_id: str):
        node = self.store.one("SELECT n.*,g.transport_mode FROM nodes n LEFT JOIN gateways g ON g.id=n.gateway_id WHERE n.id=?", (node_id,))
        if not node:
            raise OutputError("Output node not found")
        if node["node_type"] != "ANALOG_OUTPUT" or node["profile_id"] != "ebyte-me31-xxxa0006-v1.1":
            raise OutputError("Output command requires a registered EBYTE ME31-XXXA0006 node")
        if node["hardware_model"] != "EBYTE ME31-XXXA0006":
            raise OutputError("Output node model does not match the ME31 output profile")
        if not node["gateway_id"]:
            raise OutputError("Output node has no assigned gateway")
        if node["transport_mode"] != "TRANSPARENT_SERIAL":
            raise OutputError("ME31-XXXA0006 requires transparent Modbus RTU-over-TCP framing")
        return node

    def _session(self, node):
        session = self.sessions.for_gateway(node["gateway_id"])
        if not session:
            raise ConnectionError("Output gateway is not connected and commissioned")
        if session.transport_mode != "TRANSPARENT_SERIAL":
            raise ConnectionError("Commissioned gateway framing does not match the ME31 output profile")
        return session

    async def inspect(self, node_id: str):
        node = self._node(node_id)
        session = self._session(node)
        try:
            ranges, range_frame, range_request, range_latency = await session.read_registers(node["slave_id"], 3, 0x0514, 6, 1500)
            setpoints, setpoint_frame, setpoint_request, setpoint_latency = await session.read_registers(node["slave_id"], 3, 0x0064, 6, 1500)
        except Exception as exc:
            self.store.event("OUTPUT_READ_FAILED", str(exc), gateway_id=node["gateway_id"], node_id=node_id,
                             direction="RX", protocol=session.transport_mode, function_code=3)
            raise
        self.store.event("OUTPUT_RANGE_READ", "Read ME31 output range configuration", gateway_id=node["gateway_id"], node_id=node_id,
                         direction="RX", protocol=session.transport_mode, function_code=3, address=0x0514, quantity=6,
                         latency_ms=range_latency, request_hex=range_request.hex(" "), response_hex=range_frame.hex(" "))
        self.store.event("OUTPUT_SETPOINT_READ", "Read ME31 reported output setpoints; these are not physical current measurements", gateway_id=node["gateway_id"], node_id=node_id,
                         direction="RX", protocol=session.transport_mode, function_code=3, address=0x0064, quantity=6,
                         latency_ms=setpoint_latency, request_hex=setpoint_request.hex(" "), response_hex=setpoint_frame.hex(" "))
        result = []
        for index, raw_range in enumerate(ranges, start=1):
            known = raw_range in (0, 1)
            result.append({
                "channel": index,
                "label": f"AO{index}",
                "range_code": raw_range,
                "range_known": known,
                "output_min_ma": 0.0 if raw_range == 0 else 4.0 if raw_range == 1 else None,
                "output_max_ma": 20.0 if raw_range in (0, 1) else None,
                "setpoint_ma": setpoints[index - 1] / 1000.0,
                "quality": "GOOD" if known else "UNKNOWN_RANGE",
            })
        return {
            "node_id": node_id,
            "gateway_id": node["gateway_id"],
            "gateway_connected": True,
            "channels": result,
            "request_hex": [range_request.hex(" "), setpoint_request.hex(" ")],
            "response_hex": [range_frame.hex(" "), setpoint_frame.hex(" ")],
            "latency_ms": [range_latency, setpoint_latency],
        }

    async def set_output(self, node_id: str, channel: int, target_ma: float, *, source: str, timeout_ms: int = 2000, pipeline_id: str | None = None):
        if channel not in range(1, 7):
            raise OutputError("Output channel must be AO1–AO6")
        if isinstance(target_ma, bool) or not isinstance(target_ma, (int, float)) or not math.isfinite(target_ma):
            raise OutputError("Target current must be a finite number")
        node = self._node(node_id)
        if not node["polling_enabled"]:
            raise OutputError("Output node is disabled")
        channel_row = self.store.one("SELECT enabled FROM channels WHERE node_id=? AND channel_index=? AND channel_type='ANALOG_OUTPUT'", (node_id, channel))
        if not channel_row or not channel_row["enabled"]:
            raise OutputError("Output channel is missing or disabled")
        requested = float(target_ma)
        request_hex = response_hex = None
        readback = None
        session = None
        write_started = False
        now = utc_now()
        async with self.lock_for(node_id, channel):
            try:
                owners = self.store.rows("SELECT id FROM mappings WHERE destination_node_id=? AND destination_channel=? AND enabled=1 AND armed=1", (node_id, channel))
                if pipeline_id is None and owners:
                    raise OutputError("Manual control is locked while an armed pipeline owns this output channel")
                if pipeline_id is not None and [owner["id"] for owner in owners] != [pipeline_id]:
                    raise OutputError("Pipeline no longer owns this output channel; command was cancelled")
                session = self._session(node)
                ranges, range_frame, range_request, range_latency = await session.read_registers(node["slave_id"], 3, 0x0514, 6, timeout_ms)
                self.store.event("OUTPUT_RANGE_CHECK", "Verify configured AO range before output write", gateway_id=node["gateway_id"], node_id=node_id,
                                 direction="RX", protocol=session.transport_mode, function_code=3, address=0x0514, quantity=6,
                                 latency_ms=range_latency, request_hex=range_request.hex(" "), response_hex=range_frame.hex(" "))
                configured = ranges[channel - 1]
                if configured not in (0, 1):
                    raise OutputError(f"AO{channel} output range is unknown (register value {configured})")
                minimum, maximum = (0.0, 20.0) if configured == 0 else (4.0, 20.0)
                if not minimum <= requested <= maximum:
                    raise OutputError(f"Target {requested:g} mA is outside AO{channel}'s configured {minimum:g}–{maximum:g} mA range")
                write_started = True
                result = await session.write_me31_output_and_readback(node["slave_id"], channel, requested, timeout_ms)
                request_hex = f"{result['write_request'].hex(' ')} | {result['readback_request'].hex(' ')}"
                response_hex = f"{result['acknowledgement'].hex(' ')} | {result['readback_frame'].hex(' ')}"
                readback = result["readback_ma"]
                if abs(readback - requested) > self.READBACK_TOLERANCE_MA:
                    raise OutputError(f"AO{channel} readback mismatch: requested {requested:.3f} mA, device reports {readback:.3f} mA")
                detail = f"FC16 acknowledged; FC03 setpoint readback {readback:.3f} mA"
                status = "ACKNOWLEDGED"
            except Exception as exc:
                trace = getattr(exc, "trace", {})
                if trace:
                    requests=[trace["write_request"].hex(" ")]
                    if trace.get("readback_request"):
                        requests.append(trace["readback_request"].hex(" "))
                    request_hex=" | ".join(requests)
                    responses=[frame.hex(" ") for frame in (trace.get("acknowledgement"),trace.get("readback_frame")) if frame]
                    response_hex=" | ".join(responses) or None
                detail = str(exc)
                acknowledged = bool(trace and trace.get("acknowledged"))
                status = "READBACK_FAILED" if acknowledged else "FAILED"
                self._record(node_id, channel, source, requested, readback, status, detail, request_hex, response_hex)
                result_code = "OUTPUT_READBACK_FAILED" if acknowledged else "OUTPUT_WRITE_FAILED" if write_started else "OUTPUT_COMMAND_REJECTED"
                self.store.event(result_code, detail, gateway_id=node["gateway_id"], node_id=node_id,
                                 direction="TX" if write_started else "VALIDATION", protocol="TRANSPARENT_SERIAL",
                                 function_code=16 if write_started else None, address=(channel - 1) * 2 if write_started else None,
                                 quantity=2 if write_started else None, latency_ms=trace.get("write_latency_ms"), request_hex=request_hex, response_hex=response_hex)
                raise
            self._record(node_id, channel, source, requested, readback, status, detail, request_hex, response_hex)
            self.store.event("OUTPUT_WRITE_ACKNOWLEDGED", detail, gateway_id=node["gateway_id"], node_id=node_id,
                             direction="TX", protocol="TRANSPARENT_SERIAL", function_code=16,
                             address=(channel - 1) * 2, quantity=2, latency_ms=result["write_latency_ms"],
                             request_hex=result["write_request"].hex(" "), response_hex=result["acknowledgement"].hex(" "))
            self.store.event("OUTPUT_READBACK", f"AO{channel} setpoint readback {readback:.3f} mA", gateway_id=node["gateway_id"], node_id=node_id,
                             direction="RX", protocol="TRANSPARENT_SERIAL", function_code=3, address=0x0064, quantity=6,
                             latency_ms=result["readback_latency_ms"], request_hex=result["readback_request"].hex(" "), response_hex=result["readback_frame"].hex(" "))
            return {"channel": channel, "requested_ma": requested, "acknowledged_ma": readback,
                    "status": status, "updated_at": now, "request_hex": request_hex, "response_hex": response_hex}

    def _record(self, node_id, channel, source, target, readback, result, detail, request_hex, response_hex):
        self.store.run("""INSERT INTO output_commands(at,node_id,channel,source,target_ma,readback_ma,result,detail,request_hex,response_hex)
            VALUES(?,?,?,?,?,?,?,?,?,?)""", (utc_now(), node_id, channel, source, target, readback, result, detail, request_hex, response_hex))

