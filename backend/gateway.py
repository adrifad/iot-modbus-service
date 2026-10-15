"""TCP listener sessions for explicitly commissioned Modbus gateways."""

import asyncio
import os
import secrets
import socket
import time
from dataclasses import dataclass, field

from .modbus import (ModbusError, StreamFrames, build_me31_ao_write_request,
                     build_request, decode_response, decode_write_ack)


class TransportDesynchronized(ConnectionError):
    """An RTU reply timed out or was cancelled; late replies cannot be correlated."""


class OutputReadbackError(ConnectionError):
    """The FC16 acknowledgement was valid, but setpoint readback did not complete."""

    def __init__(self, message, trace):
        super().__init__(message)
        self.trace = trace


@dataclass
class GatewaySession:
    session_id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    peer: str
    peer_ip: str | None = None
    gateway_id: str | None = None
    generation: int = 0
    trusted_ingress_id: str | None = None
    transport_mode: str | None = None
    identity_verified: bool = False
    commissioning_method: str = "NONE"
    frames: asyncio.Queue = field(default_factory=asyncio.Queue)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: bool = False
    state: str = "CONNECTING"
    last_request_at: float = 0.0
    last_activity: float = field(default_factory=time.time)
    request_count: int = 0
    identity_checked: bool = False
    identity_buffer: bytearray = field(default_factory=bytearray)
    pending_data: bytearray = field(default_factory=bytearray)
    parser: StreamFrames | None = None
    desynchronized: bool = False
    is_tls: bool = False
    on_identity: object = None
    on_invalid_frame: object = None
    on_unsolicited_frame: object = None
    on_modbus_success: object = None

    def __post_init__(self):
        self.is_tls = self.writer.get_extra_info("ssl_object") is not None

    @property
    def connected(self):
        return not self.closed and self.state in {"UNCOMMISSIONED", "COMMISSIONED", "DESYNCHRONIZED", "RETIRED"}

    @property
    def commissioned(self):
        return self.connected and self.state == "COMMISSIONED" and self.gateway_id is not None

    async def run_reader(self, on_close, on_invalid_frame, on_identity=None, on_unsolicited_frame=None):
        if not self.commissioned:
            self.state = "UNCOMMISSIONED"
        self.on_invalid_frame = on_invalid_frame
        self.on_identity = on_identity
        self.on_unsolicited_frame = on_unsolicited_frame
        failure = None
        try:
            while not self.closed:
                try:
                    data = await asyncio.wait_for(self.reader.read(4096), timeout=self._read_timeout())
                except asyncio.TimeoutError:
                    # A TCP idle timeout detects dead LTE/NAT sessions without
                    # treating a remote IP or source port as gateway identity.
                    if self._read_timeout() is not None:
                        raise TimeoutError("gateway TCP idle timeout")
                    continue
                if not data:
                    break
                self.last_activity = time.time()
                data = await self._consume_identity(data)
                if data and self.commissioned:
                    await self._parse(data)
                elif data:
                    self.pending_data.extend(data)
                    if len(self.pending_data) > 8192:
                        await self._invalid("uncommissioned session buffer exceeded 8192 bytes")
                        self.pending_data.clear()
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ValueError, TimeoutError) as exc:
            failure = exc
            if isinstance(exc, TimeoutError) and self.gateway_id:
                await self._invalid(str(exc))
        finally:
            self.closed = True
            if self.state not in {"RETIRED", "DESYNCHRONIZED"}:
                self.state = "DISCONNECTED"
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            await on_close(self, failure)

    def _read_timeout(self):
        # Deprecated compatibility hook. A receive-side idle timeout is unsafe
        # for transparent gateways because a stable setpoint can be silent.
        # Modbus health checks and TCP keepalive now determine liveness.
        return None

    async def _consume_identity(self, data: bytes) -> bytes:
        if self.identity_checked or self.gateway_id:
            return data
        marker = b"FIELDLINE/1 "
        self.identity_buffer.extend(data)
        current = bytes(self.identity_buffer)
        if len(current) <= len(marker) and marker.startswith(current):
            return b""
        if not current.startswith(marker):
            self.identity_checked = True
            self.pending_data.extend(current)
            self.identity_buffer.clear()
            return b""
        newline = current.find(b"\n")
        if newline < 0:
            if len(current) > 256:
                await self._invalid("gateway registration packet exceeds 256 bytes")
                self.identity_checked = True
                self.writer.close()
            return b""
        identity = current[:newline + 1].rstrip(b"\r\n")
        remainder = current[newline + 1:]
        self.identity_checked = True
        self.identity_buffer.clear()
        if self.on_identity is None:
            await self._invalid("gateway registration packet received but identity commissioning is unavailable")
            self.writer.close()
            return b""
        accepted = await self.on_identity(self, identity)
        return remainder if accepted and self.commissioned else b""

    async def commission(self, gateway_id: str, transport_mode: str, *, identity_verified=False, commissioning_method="OPERATOR_ASSIGNMENT"):
        if self.closed:
            raise ConnectionError("cannot commission a closed TCP session")
        if transport_mode not in {"MODBUS_TCP", "MODBUS_RTU", "TRANSPARENT_SERIAL"}:
            raise ValueError("unsupported gateway transport mode")
        self.gateway_id = gateway_id
        self.transport_mode = transport_mode
        self.identity_verified = identity_verified
        self.commissioning_method = commissioning_method
        self.identity_checked = True
        self.parser = StreamFrames.create(transport_mode)
        self.state = "COMMISSIONED"
        buffered = bytes(self.pending_data)
        self.pending_data.clear()
        if buffered:
            await self._parse(buffered)

    async def _parse(self, data):
        if not self.parser:
            return
        frames = self.parser.push(data)
        for error in self.parser.errors:
            await self._invalid(error)
        for frame in frames:
            await self.frames.put(frame)

    async def _invalid(self, detail):
        if self.on_invalid_frame:
            await self.on_invalid_frame(self, detail)

    async def _unsolicited(self, frame, detail):
        if self.on_unsolicited_frame:
            await self.on_unsolicited_frame(self, frame, detail)

    async def retire(self):
        if self.closed:
            return
        self.state = "RETIRED"
        self.closed = True
        self.writer.close()
        try:
            await self.writer.wait_closed()
        except (ConnectionError, OSError):
            pass

    async def _desynchronize_rtu(self, reason):
        if self.transport_mode != "MODBUS_TCP":
            self.desynchronized = True
            self.state = "DESYNCHRONIZED"
            self.writer.close()

    async def _wait_transaction_locked(self, unit_id, function_code, request, timeout_ms, decoder):
        if self.desynchronized:
            raise TransportDesynchronized("RTU stream is desynchronized; reconnect and recommission gateway")
        if self.closed or not self.commissioned:
            raise ConnectionError("gateway session is not commissioned or connected")
        minimum_gap = max(0, int(os.getenv("MODBUS_MIN_REQUEST_INTERVAL_MS", "50"))) / 1000
        delay = minimum_gap - (time.perf_counter() - self.last_request_at)
        if delay > 0:
            await asyncio.sleep(delay)
        while not self.frames.empty():
            stale = self.frames.get_nowait()
            await self._unsolicited(stale, "frame arrived outside the active Modbus request")
        transaction_id = int.from_bytes(request[:2], "big") if self.transport_mode == "MODBUS_TCP" else None
        started = time.perf_counter()
        try:
            self.writer.write(request)
            await self.writer.drain()
        except (ConnectionError, OSError) as exc:
            raise ConnectionError("gateway TCP write failed") from exc
        self.last_request_at = time.perf_counter()
        self.request_count += 1
        deadline = time.perf_counter() + timeout_ms / 1000
        try:
            while True:
                remaining = deadline - time.perf_counter()
                if remaining <= 0:
                    raise asyncio.TimeoutError
                frame = await asyncio.wait_for(self.frames.get(), timeout=remaining)
                if self.transport_mode == "MODBUS_TCP":
                    if len(frame) >= 2 and int.from_bytes(frame[:2], "big") == transaction_id:
                        break
                    await self._unsolicited(frame, "Modbus TCP transaction identifier did not match the active request")
                elif len(frame) >= 2 and frame[0] == unit_id and frame[1] in (function_code, function_code | 0x80):
                    break
                else:
                    await self._unsolicited(frame, "RTU unit/function did not match the active request")
            latency_ms = (time.perf_counter() - started) * 1000
            result = decoder(frame, transaction_id)
            if self.on_modbus_success:
                value = self.on_modbus_success(self, latency_ms)
                if asyncio.iscoroutine(value):
                    await value
            return result, frame, request, latency_ms
        except asyncio.TimeoutError as exc:
            latency_ms = (time.perf_counter() - started) * 1000
            await self._desynchronize_rtu("RTU response timed out; session closed to quarantine delayed replies")
            error = TimeoutError("Modbus response timed out")
            error.modbus_latency_ms = latency_ms
            raise error from exc
        except asyncio.CancelledError:
            await self._desynchronize_rtu("RTU transaction cancelled; session closed to quarantine delayed replies")
            raise
        except Exception as exc:
            try:
                exc.modbus_frame = frame
            except Exception:
                pass
            raise

    async def read_registers(self, unit_id, function_code, address, quantity, timeout_ms):
        if self.desynchronized:
            raise TransportDesynchronized("RTU stream is desynchronized; reconnect and recommission gateway")
        if self.closed or not self.commissioned:
            raise ConnectionError("gateway session is not commissioned or connected")
        async with self.lock:
            transaction_id = secrets.randbelow(65536)
            request = build_request(self.transport_mode, unit_id, function_code, address, quantity, transaction_id)
            decoder = lambda frame, tx: decode_response(
                self.transport_mode, frame, unit_id=unit_id, function_code=function_code,
                quantity=quantity, transaction_id=tx)
            return await self._wait_transaction_locked(unit_id, function_code, request, timeout_ms, decoder)

    async def write_me31_output_and_readback(self, unit_id, channel, value_ma, write_timeout_ms, readback_timeout_ms=None):
        """Serialize the allowlisted FC16 output write and FC03 setpoint readback."""
        if self.desynchronized:
            raise TransportDesynchronized("RTU stream is desynchronized; reconnect and recommission gateway")
        if self.closed or not self.commissioned:
            raise ConnectionError("gateway session is not commissioned or connected")
        async with self.lock:
            transaction_id = secrets.randbelow(65536)
            address = (channel - 1) * 2 if channel in range(1, 7) else -1
            request = build_me31_ao_write_request(self.transport_mode, unit_id, channel, value_ma, transaction_id)
            decoder = lambda frame, tx: decode_write_ack(
                self.transport_mode, frame, unit_id=unit_id, address=address, quantity=2, transaction_id=tx)
            try:
                _, ack, write_request, write_latency = await self._wait_transaction_locked(
                    unit_id, 16, request, write_timeout_ms, decoder)
            except Exception as exc:
                try:
                    exc.trace = {"write_request": request, "acknowledgement": getattr(exc, "modbus_frame", None),
                                 "write_latency_ms": getattr(exc, "modbus_latency_ms", None), "acknowledged": False}
                except Exception:
                    pass
                raise
            read_tx = secrets.randbelow(65536)
            read_request = build_request(self.transport_mode, unit_id, 3, 0x0064, 6, read_tx)
            read_decoder = lambda frame, tx: decode_response(
                self.transport_mode, frame, unit_id=unit_id, function_code=3, quantity=6, transaction_id=tx)
            try:
                values, readback_frame, _, read_latency = await self._wait_transaction_locked(
                    unit_id, 3, read_request, readback_timeout_ms or write_timeout_ms, read_decoder)
            except Exception as exc:
                raise OutputReadbackError(str(exc), {"acknowledgement": ack, "write_request": write_request,
                    "write_latency_ms": write_latency, "readback_request": read_request,
                    "readback_frame": getattr(exc, "modbus_frame", None),
                    "readback_latency_ms": getattr(exc, "modbus_latency_ms", None), "acknowledged": True}) from exc
            return {
                "acknowledgement": ack,
                "write_request": write_request,
                "write_latency_ms": write_latency,
                "readback_frame": readback_frame,
                "readback_request": read_request,
                "readback_latency_ms": read_latency,
                "readback_ma": values[channel - 1] / 1000.0,
            }
