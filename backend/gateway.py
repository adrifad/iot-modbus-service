"""TCP listener and explicitly commissioned gateway sessions."""

import asyncio
import os
import secrets
import time
from dataclasses import dataclass, field

from .modbus import ModbusError, StreamFrames, build_request, decode_response


@dataclass
class GatewaySession:
    session_id: str
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    peer: str
    gateway_id: str | None = None
    transport_mode: str | None = None
    frames: asyncio.Queue = field(default_factory=asyncio.Queue)
    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    closed: bool = False
    last_request_at: float = 0.0

    async def run_reader(self, on_close, on_invalid_frame):
        try:
            while data := await self.reader.read(4096):
                self.last_activity = time.time()
                if self.transport_mode:
                    parser = getattr(self, "parser", None)
                    if parser is None:
                        parser = self.parser = StreamFrames.create(self.transport_mode)
                    try:
                        parsed = parser.push(data)
                    except ModbusError as exc:
                        await on_invalid_frame(self, str(exc))
                        return
                    for frame in parsed:
                        await self.frames.put(frame)
        except (asyncio.IncompleteReadError, ConnectionError, OSError, ValueError):
            pass
        finally:
            self.closed = True
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except (ConnectionError, OSError):
                pass
            await on_close(self)

    async def read_registers(self, unit_id, function_code, address, quantity, timeout_ms):
        if self.closed or not self.transport_mode:
            raise ConnectionError("gateway session is not commissioned or connected")
        async with self.lock:
            minimum_gap = int(os.getenv("MODBUS_MIN_REQUEST_INTERVAL_MS", "50")) / 1000
            delay = minimum_gap - (time.perf_counter() - self.last_request_at)
            if delay > 0:
                await asyncio.sleep(delay)
            while not self.frames.empty():
                self.frames.get_nowait()
            transaction_id = secrets.randbelow(65536)
            request = build_request(self.transport_mode, unit_id, function_code, address, quantity, transaction_id)
            started = time.perf_counter()
            self.writer.write(request)
            await self.writer.drain()
            self.last_request_at = time.perf_counter()
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
                    elif len(frame) >= 2 and frame[0] == unit_id and frame[1] in (function_code, function_code | 0x80):
                        break
                latency_ms = (time.perf_counter() - started) * 1000
                values = decode_response(self.transport_mode, frame, unit_id=unit_id,
                                          function_code=function_code, quantity=quantity,
                                          transaction_id=transaction_id if self.transport_mode == "MODBUS_TCP" else None)
                return values, frame, request, latency_ms
            except asyncio.TimeoutError as exc:
                raise TimeoutError("Modbus response timed out") from exc
