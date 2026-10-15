"""Small Modbus read codec for MBAP, RTU ADU, and transparent serial-over-TCP."""

from dataclasses import dataclass, field
import struct
import math


class ModbusError(Exception):
    pass


class ModbusExceptionResponse(ModbusError):
    def __init__(self, function_code: int, exception_code: int):
        super().__init__(f"Modbus exception {exception_code} for function {function_code}")
        self.function_code = function_code
        self.exception_code = exception_code


def crc16(data: bytes) -> int:
    crc = 0xFFFF
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0xA001 if crc & 1 else 0)
    return crc


def with_crc(data: bytes) -> bytes:
    crc = crc16(data)
    return data + bytes((crc & 0xFF, crc >> 8))


def build_request(mode: str, unit_id: int, function_code: int, address: int, quantity: int, transaction_id: int = 1) -> bytes:
    if mode not in {"MODBUS_TCP", "MODBUS_RTU", "TRANSPARENT_SERIAL"}:
        raise ValueError("unsupported gateway transport mode")
    if unit_id not in range(1, 248) or function_code not in (3, 4):
        raise ValueError("only valid unit IDs and read functions FC03/FC04 are supported")
    if address not in range(0, 65536) or quantity not in range(1, 126) or address + quantity > 65536:
        raise ValueError("register range is invalid")
    pdu = bytes((function_code, address >> 8, address & 0xFF, quantity >> 8, quantity & 0xFF))
    if mode == "MODBUS_TCP":
        return transaction_id.to_bytes(2, "big") + b"\x00\x00" + (len(pdu) + 1).to_bytes(2, "big") + bytes((unit_id,)) + pdu
    return with_crc(bytes((unit_id,)) + pdu)


def expected_response_length(mode: str, buffer: bytes) -> int | None:
    if mode == "MODBUS_TCP":
        if len(buffer) < 7:
            return None
        protocol = int.from_bytes(buffer[2:4], "big")
        length = int.from_bytes(buffer[4:6], "big")
        if protocol != 0 or not 2 <= length <= 254:
            raise ModbusError("invalid MBAP protocol ID or length")
        return 6 + length
    if len(buffer) < 2:
        return None
    function = buffer[1]
    if function & 0x80:
        return 5
    if function not in (3, 4):
        raise ModbusError(f"unexpected response function code {function}")
    if len(buffer) < 3:
        return None
    return 5 + buffer[2]


def decode_response(mode: str, frame: bytes, *, unit_id: int, function_code: int, quantity: int, transaction_id: int | None = None) -> list[int]:
    if mode == "MODBUS_TCP":
        if len(frame) < 9:
            raise ModbusError("truncated Modbus TCP response")
        tx = int.from_bytes(frame[0:2], "big")
        protocol = int.from_bytes(frame[2:4], "big")
        length = int.from_bytes(frame[4:6], "big")
        if transaction_id is not None and tx != transaction_id:
            raise ModbusError("transaction identifier mismatch")
        if protocol != 0 or length != len(frame) - 6:
            raise ModbusError("invalid Modbus TCP header length or protocol ID")
        if frame[6] != unit_id:
            raise ModbusError("unit identifier mismatch")
        function = frame[7]
        body = frame[8:]
    else:
        if len(frame) < 5:
            raise ModbusError("truncated Modbus RTU response")
        if frame[0] != unit_id:
            raise ModbusError("slave identifier mismatch")
        received_crc = int.from_bytes(frame[-2:], "little")
        if crc16(frame[:-2]) != received_crc:
            raise ModbusError("RTU CRC mismatch")
        function = frame[1]
        body = frame[2:-2]
    if function == (function_code | 0x80):
        if len(body) != 1:
            raise ModbusError("invalid Modbus exception response length")
        code = body[0]
        raise ModbusExceptionResponse(function_code, code)
    if function != function_code:
        raise ModbusError(f"function code mismatch: expected {function_code}, received {function}")
    expected_bytes = quantity * 2
    if not body or body[0] != expected_bytes or len(body) != expected_bytes + 1:
        raise ModbusError("response byte count does not match requested register quantity")
    return [int.from_bytes(body[offset:offset + 2], "big") for offset in range(1, len(body), 2)]


def decode_registers(registers: list[int], data_type: str, *, byte_order="big", word_order="BIG_WORD_FIRST"):
    if byte_order not in {"big", "little"}:
        raise ModbusError(f"unsupported byte order {byte_order}")
    if data_type in {"uint16", "uint16_array", "enum16_array", "raw"}:
        return registers[0] if data_type == "uint16" else registers
    if data_type == "int16":
        return registers[0] - 65536 if registers[0] & 0x8000 else registers[0]
    if data_type in {"float32", "float32_array"}:
        if len(registers) % 2:
            raise ModbusError("float32 decoding requires an even register count")
        if word_order not in {"BIG_WORD_FIRST", "LITTLE_WORD_FIRST"}:
            raise ModbusError("float32 word order is not verified")
        floats = []
        for offset in range(0, len(registers), 2):
            words = registers[offset:offset + 2]
            if word_order == "LITTLE_WORD_FIRST":
                words.reverse()
            raw = b"".join(word.to_bytes(2, "big") for word in words)
            if byte_order == "little":
                raw = bytes((raw[1], raw[0], raw[3], raw[2]))
            value = struct.unpack(">f", raw)[0]
            if not math.isfinite(value):
                raise ModbusError("float32 register value is not finite")
            floats.append(value)
        return floats[0] if data_type == "float32" else floats
    if data_type == "ascii":
        return b"".join(value.to_bytes(2, "big") for value in registers).decode("ascii", errors="replace").rstrip("\x00\ufffd ")
    raise ModbusError(f"unsupported register data type {data_type}")


@dataclass
class StreamFrames:
    mode: str
    buffer: bytearray
    errors: list[str] = field(default_factory=list)

    @classmethod
    def create(cls, mode: str):
        return cls(mode, bytearray())

    def push(self, data: bytes) -> list[bytes]:
        self.errors.clear()
        if self.mode not in {"MODBUS_TCP", "MODBUS_RTU", "TRANSPARENT_SERIAL"}:
            raise ModbusError("unsupported stream framing mode")
        self.buffer.extend(data)
        if len(self.buffer) > 8192:
            keep = 5 if self.mode == "MODBUS_TCP" else 2
            self.buffer[:] = self.buffer[-keep:]
            self.errors.append("stream buffer exceeded 8192 bytes; discarded unsynchronized data")
        frames = []
        while self.buffer:
            if self.mode == "MODBUS_TCP":
                if len(self.buffer) < 6:
                    break
                length = int.from_bytes(self.buffer[4:6], "big")
                if not 2 <= length <= 254 or int.from_bytes(self.buffer[2:4], "big") != 0:
                    next_header = next((offset for offset in range(1, max(1, len(self.buffer) - 5))
                                        if int.from_bytes(self.buffer[offset + 2:offset + 4], "big") == 0
                                        and 2 <= int.from_bytes(self.buffer[offset + 4:offset + 6], "big") <= 254), None)
                    if next_header is None:
                        discarded = max(0, len(self.buffer) - 5)
                        if discarded:
                            del self.buffer[:discarded]
                            self.errors.append(f"invalid MBAP stream data; discarded {discarded} byte(s)")
                        break
                    del self.buffer[:next_header]
                    self.errors.append(f"invalid MBAP header; resynchronized after {next_header} byte(s)")
                    continue
                total = 6 + length
            else:
                if len(self.buffer) < 2:
                    break
                if self.buffer[0] not in range(1, 248):
                    del self.buffer[0]
                    self.errors.append("discarded RTU byte outside slave address range")
                    continue
                function = self.buffer[1]
                if function in (0x83, 0x84):
                    total = 5
                elif function in (3, 4):
                    if len(self.buffer) < 3:
                        break
                    byte_count = self.buffer[2]
                    if byte_count == 0 or byte_count > 250 or byte_count % 2:
                        del self.buffer[0]
                        self.errors.append("invalid RTU register byte count; resynchronizing")
                        continue
                    total = 5 + byte_count
                else:
                    del self.buffer[0]
                    self.errors.append(f"unexpected RTU function code {function}; resynchronizing")
                    continue
            if len(self.buffer) < total:
                break
            candidate = bytes(self.buffer[:total])
            if self.mode != "MODBUS_TCP" and crc16(candidate[:-2]) != int.from_bytes(candidate[-2:], "little"):
                recovered = None
                for offset in range(1, len(self.buffer) - 4):
                    if self.buffer[offset] not in range(1, 248):
                        continue
                    function = self.buffer[offset + 1]
                    if function in (0x83, 0x84):
                        candidate_length = 5
                    elif function in (3, 4) and offset + 3 <= len(self.buffer):
                        byte_count = self.buffer[offset + 2]
                        if byte_count == 0 or byte_count > 250 or byte_count % 2:
                            continue
                        candidate_length = 5 + byte_count
                    else:
                        continue
                    end = offset + candidate_length
                    if end <= len(self.buffer) and crc16(self.buffer[offset:end - 2]) == int.from_bytes(self.buffer[end - 2:end], "little"):
                        recovered = offset
                        break
                if recovered is None:
                    del self.buffer[0]
                    self.errors.append("RTU CRC mismatch; resynchronizing")
                else:
                    del self.buffer[:recovered]
                    self.errors.append(f"RTU CRC mismatch; recovered after {recovered} byte(s)")
                continue
            frames.append(candidate)
            del self.buffer[:total]
        return frames
