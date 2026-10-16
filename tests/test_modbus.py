import unittest

from backend.modbus import (
    ModbusError, ModbusExceptionResponse, StreamFrames, build_request, decode_registers,
    build_me31_ao_range_request, build_me31_ao_write_request, crc16, decode_response,
    decode_write_ack, decode_write_single_ack, with_crc,
)


class ModbusCodecTests(unittest.TestCase):
    def test_build_modbus_tcp_fc04_request(self):
        self.assertEqual(build_request("MODBUS_TCP", 1, 4, 0x0064, 6, 0x1234),
                         bytes.fromhex("12 34 00 00 00 06 01 04 00 64 00 06"))

    def test_build_rtu_request_with_crc(self):
        request = build_request("MODBUS_RTU", 1, 4, 0x0064, 1)
        self.assertEqual(request[:6], bytes.fromhex("01 04 00 64 00 01"))
        self.assertEqual(int.from_bytes(request[-2:], "little"), crc16(request[:-2]))

    def test_tcp_stream_handles_fragmentation_and_concatenation(self):
        parser = StreamFrames.create("MODBUS_TCP")
        first = bytes.fromhex("00 01 00 00 00 05 01 04 02 03 e8")
        second = bytes.fromhex("00 02 00 00 00 05 01 04 02 07 d0")
        self.assertEqual(parser.push(first[:4]), [])
        self.assertEqual(parser.push(first[4:] + second), [first, second])

    def test_rtu_stream_handles_fragmentation_and_concatenation(self):
        parser = StreamFrames.create("TRANSPARENT_SERIAL")
        first = with_crc(bytes.fromhex("01 04 02 03 e8"))
        second = with_crc(bytes.fromhex("01 04 02 07 d0"))
        self.assertEqual(parser.push(first[:3]), [])
        self.assertEqual(parser.push(first[3:] + second), [first, second])

    def test_decode_tcp_register_response_and_check_transaction(self):
        frame = bytes.fromhex("12 34 00 00 00 07 01 04 04 00 00 61 a8")
        self.assertEqual(decode_response("MODBUS_TCP", frame, unit_id=1, function_code=4,
                                         quantity=2, transaction_id=0x1234), [0, 25000])
        with self.assertRaisesRegex(ModbusError, "transaction"):
            decode_response("MODBUS_TCP", frame, unit_id=1, function_code=4,
                            quantity=2, transaction_id=0x9999)

    def test_decode_rtu_register_response_and_crc(self):
        frame = with_crc(bytes.fromhex("01 04 02 61 a8"))
        self.assertEqual(decode_response("MODBUS_RTU", frame, unit_id=1,
                                         function_code=4, quantity=1), [25000])
        broken = frame[:-1] + bytes((frame[-1] ^ 1,))
        with self.assertRaisesRegex(ModbusError, "CRC"):
            decode_response("MODBUS_RTU", broken, unit_id=1, function_code=4, quantity=1)

    def test_modbus_exception_is_not_accepted_as_measurement(self):
        frame = bytes.fromhex("00 01 00 00 00 03 01 84 02")
        with self.assertRaises(ModbusExceptionResponse) as caught:
            decode_response("MODBUS_TCP", frame, unit_id=1, function_code=4,
                            quantity=1, transaction_id=1)
        self.assertEqual(caught.exception.exception_code, 2)

    def test_wrong_unit_and_byte_count_are_rejected(self):
        with self.assertRaisesRegex(ModbusError, "unit"):
            decode_response("MODBUS_TCP", bytes.fromhex("00 01 00 00 00 05 02 04 02 00 01"),
                            unit_id=1, function_code=4, quantity=1, transaction_id=1)

    def test_float32_word_order_is_explicit(self):
        self.assertEqual(decode_registers([0x4148, 0x0000], "float32"), 12.5)
        self.assertEqual(decode_registers([0x0000, 0x4148], "float32", word_order="LITTLE_WORD_FIRST"), 12.5)
        self.assertEqual(decode_registers([0x4841, 0x0000], "float32", byte_order="little"), 12.5)
        self.assertEqual(decode_registers([0x0000, 0x4841], "float32", byte_order="little",
                                          word_order="LITTLE_WORD_FIRST"), 12.5)
        self.assertEqual(decode_registers([0x4148, 0x0000] * 6, "float32_array"), [12.5] * 6)
        with self.assertRaisesRegex(ModbusError, "not verified"):
            decode_registers([0x4148, 0x0000], "float32", word_order="unverified")
        with self.assertRaisesRegex(ModbusError, "not finite"):
            decode_registers([0x7FC0, 0x0000], "float32")
        with self.assertRaisesRegex(ModbusError, "not finite"):
            decode_registers([0x7F80, 0x0000], "float32")
        with self.assertRaisesRegex(ModbusError, "byte count"):
            decode_response("MODBUS_TCP", bytes.fromhex("00 01 00 00 00 04 01 04 01 00"),
                            unit_id=1, function_code=4, quantity=1, transaction_id=1)

    def test_tcp_parser_recovers_after_invalid_mbap_header(self):
        parser = StreamFrames.create("MODBUS_TCP")
        good = bytes.fromhex("00 01 00 00 00 05 01 04 02 03 e8")
        self.assertEqual(parser.push(bytes.fromhex("FF FF 00 01 00 00 00 00") + good), [good])
        self.assertTrue(parser.errors)

    def test_rtu_parser_discards_bad_crc_and_recovers(self):
        parser = StreamFrames.create("TRANSPARENT_SERIAL")
        good = with_crc(bytes.fromhex("01 04 02 03 e8"))
        bad = good[:-1] + bytes((good[-1] ^ 0x01,))
        self.assertEqual(parser.push(bad + good), [good])
        self.assertTrue(any("CRC" in error for error in parser.errors))

    def test_me31_ao_fc16_float_encoding_and_rtu_crc(self):
        request = build_me31_ao_write_request("TRANSPARENT_SERIAL", 1, 2, 12.5)
        self.assertEqual(request[:7], bytes.fromhex("01 10 00 02 00 02 04"))
        self.assertEqual(request[7:11], bytes.fromhex("41 48 00 00"))
        self.assertEqual(crc16(request[:-2]), int.from_bytes(request[-2:], "little"))

    def test_me31_ao_fc16_tcp_frame_and_acknowledgement_validation(self):
        request = build_me31_ao_write_request("MODBUS_TCP", 1, 2, 12.5, transaction_id=0x1234)
        self.assertEqual(request, bytes.fromhex("12 34 00 00 00 0B 01 10 00 02 00 02 04 41 48 00 00"))
        response = bytes.fromhex("12 34 00 00 00 06 01 10 00 02 00 02")
        decode_write_ack("MODBUS_TCP", response, unit_id=1, address=2, transaction_id=0x1234)
        with self.assertRaises(ModbusError):
            decode_write_ack("MODBUS_TCP", response, unit_id=1, address=4, transaction_id=0x1234)
        with self.assertRaises(ModbusError):
            decode_write_ack("MODBUS_TCP", response, unit_id=1, address=2, transaction_id=0x9999)

    def test_me31_ao_fc16_exception_and_allowlist_validation(self):
        exception = with_crc(bytes.fromhex("01 90 02"))
        with self.assertRaises(ModbusExceptionResponse):
            decode_write_ack("TRANSPARENT_SERIAL", exception, unit_id=1, address=0)
        for channel, value in ((0, 10), (7, 10), (1, float("nan")), (1, 25.1)):
            with self.assertRaises(ValueError):
                build_me31_ao_write_request("TRANSPARENT_SERIAL", 1, channel, value)

    def test_rtu_parser_recognizes_fc16_acknowledgement(self):
        parser = StreamFrames.create("TRANSPARENT_SERIAL")
        ack = with_crc(bytes.fromhex("01 10 00 02 00 02"))
        self.assertEqual(parser.push(ack[:3]), [])
        self.assertEqual(parser.push(ack[3:]), [ack])

    def test_me31_ao_fc06_range_request_is_single_register_allowlisted(self):
        ao1 = build_me31_ao_range_request("TRANSPARENT_SERIAL", 1, 1, 1)
        ao6 = build_me31_ao_range_request("TRANSPARENT_SERIAL", 1, 6, 0)
        self.assertEqual(ao1[:6], bytes.fromhex("01 06 05 14 00 01"))
        self.assertEqual(ao6[:6], bytes.fromhex("01 06 05 19 00 00"))
        self.assertEqual(crc16(ao1[:-2]), int.from_bytes(ao1[-2:], "little"))
        with self.assertRaises(ValueError):
            build_me31_ao_range_request("TRANSPARENT_SERIAL", 1, 1, 2)
        with self.assertRaises(ValueError):
            build_me31_ao_range_request("TRANSPARENT_SERIAL", 1, 7, 0)

    def test_me31_ao_fc06_ack_crc_echo_and_exception(self):
        ack = with_crc(bytes.fromhex("01 06 05 14 00 01"))
        decode_write_single_ack("TRANSPARENT_SERIAL", ack, unit_id=1, address=0x0514, value=1)
        with self.assertRaisesRegex(ModbusError, "echo"):
            decode_write_single_ack("TRANSPARENT_SERIAL", ack, unit_id=1, address=0x0515, value=1)
        bad = ack[:-1] + bytes((ack[-1] ^ 1,))
        with self.assertRaisesRegex(ModbusError, "CRC"):
            decode_write_single_ack("TRANSPARENT_SERIAL", bad, unit_id=1, address=0x0514, value=1)
        with self.assertRaises(ModbusExceptionResponse):
            decode_write_single_ack("TRANSPARENT_SERIAL", with_crc(bytes.fromhex("01 86 02")),
                                    unit_id=1, address=0x0514, value=1)

    def test_rtu_parser_handles_fragmented_fc06_ack_and_exception(self):
        parser = StreamFrames.create("TRANSPARENT_SERIAL")
        ack = with_crc(bytes.fromhex("01 06 05 19 00 01"))
        exception = with_crc(bytes.fromhex("01 86 02"))
        self.assertEqual(parser.push(ack[:4]), [])
        self.assertEqual(parser.push(ack[4:] + exception), [ack, exception])


if __name__ == "__main__":
    unittest.main()
