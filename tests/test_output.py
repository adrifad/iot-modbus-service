import asyncio
import struct
import unittest
from uuid import uuid4

from backend.gateway import GatewaySession, OutputReadbackError
from backend.modbus import with_crc
from backend.output import OutputDriver, OutputError, OutputOutcomeUnknown
from backend.store import Store, utc_now


class SessionLookup:
    def __init__(self, session):
        self.session = session

    def for_gateway(self, gateway_id):
        return self.session if self.session.gateway_id == gateway_id and self.session.commissioned else None


class OutputDriverIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def _device(self, ranges=None):
        self.ranges = ranges or [0] * 6
        self.setpoints = [0] * 6
        self.write_requests = []
        self.range_write_requests = []
        self.ignore_writes = False
        self.drop_write_ack = False
        self.ignore_range_writes = False
        self.drop_range_ack = False

        async def simulated_me31(reader, writer):
            try:
                while True:
                    prefix = await reader.readexactly(2)
                    function = prefix[1]
                    if function == 3:
                        rest = await reader.readexactly(6)
                        request = prefix + rest
                        address = int.from_bytes(request[2:4], "big")
                        quantity = int.from_bytes(request[4:6], "big")
                        values = self.ranges if address == 0x0514 else self.setpoints if address == 0x0064 else []
                        if quantity != 6 or len(values) != 6:
                            raise AssertionError(f"unexpected FC03 request {request.hex()}")
                        body = bytes((quantity * 2,)) + b"".join(value.to_bytes(2, "big") for value in values)
                        writer.write(with_crc(prefix[:1] + bytes((3,)) + body))
                    elif function == 16:
                        rest = await reader.readexactly(11)
                        request = prefix + rest
                        self.write_requests.append(request)
                        address = int.from_bytes(request[2:4], "big")
                        quantity = int.from_bytes(request[4:6], "big")
                        byte_count = request[6]
                        if quantity != 2 or byte_count != 4 or address % 2:
                            raise AssertionError(f"unexpected AO setpoint write {request.hex()}")
                        channel = address // 2
                        current = struct.unpack(">f", request[7:11])[0]
                        if not self.ignore_writes:
                            self.setpoints[channel] = round(current * 1000)
                        if not self.drop_write_ack:
                            writer.write(with_crc(prefix[:1] + bytes((16,)) + request[2:6]))
                    elif function == 6:
                        rest = await reader.readexactly(6)
                        request = prefix + rest
                        self.range_write_requests.append(request)
                        address = int.from_bytes(request[2:4], "big")
                        value = int.from_bytes(request[4:6], "big")
                        if address not in range(0x0514, 0x051A) or value not in (0, 1):
                            raise AssertionError(f"unexpected AO range write {request.hex()}")
                        if not self.ignore_range_writes:
                            self.ranges[address - 0x0514] = value
                        if not self.drop_range_ack:
                            writer.write(with_crc(prefix[:1] + bytes((6,)) + request[2:6]))
                    else:
                        raise AssertionError(f"unexpected function {function}")
                    await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        self.server = await asyncio.start_server(simulated_me31, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        self.session = GatewaySession(str(uuid4()), reader, writer, "127.0.0.1:0")
        await self.session.commission("output-gateway", "TRANSPARENT_SERIAL")

        async def close(*_):
            pass

        async def invalid(_, detail):
            self.fail(detail)

        self.reader_task = asyncio.create_task(self.session.run_reader(close, invalid))
        self.store = Store(":memory:")
        self.gateway_id, self.node_id = "output-gateway", "output-node"
        now = utc_now()
        self.store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)", (self.gateway_id, "Simulated W630", "TRANSPARENT_SERIAL", now, now))
        self.store.run("INSERT INTO nodes(id,name,node_type,hardware_model,gateway_id,slave_id,profile_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                       (self.node_id, "Simulated AO", "ANALOG_OUTPUT", "EBYTE ME31-XXXA0006", self.gateway_id, 1, "ebyte-me31-xxxa0006-v1.1", now, now))
        for channel in range(1, 7):
            self.store.run("INSERT INTO channels(id,node_id,channel_index,channel_type,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                           (str(uuid4()), self.node_id, channel, "ANALOG_OUTPUT", f"AO{channel}", now, now))
        self.driver = OutputDriver(self.store, SessionLookup(self.session))

    async def asyncTearDown(self):
        if hasattr(self, "session"):
            await self.session.retire()
        if hasattr(self, "reader_task"):
            await asyncio.gather(self.reader_task, return_exceptions=True)
        if hasattr(self, "server"):
            self.server.close()
            await self.server.wait_closed()
        if hasattr(self, "store"):
            self.store.close()

    async def test_fc03_ranges_and_fc16_ao2_float_write_readback(self):
        await self._device()
        result = await self.driver.set_output(self.node_id, 2, 12.5, source="MANUAL:test")
        self.assertEqual(result["status"], "ACKNOWLEDGED")
        self.assertAlmostEqual(result["acknowledged_ma"], 12.5)
        request = self.write_requests[0]
        self.assertEqual(request[:7], bytes.fromhex("01 10 00 02 00 02 04"))
        self.assertEqual(struct.unpack(">f", request[7:11])[0], 12.5)
        row = self.store.one("SELECT * FROM output_commands WHERE node_id=?", (self.node_id,))
        self.assertEqual(row["result"], "ACKNOWLEDGED")
        self.assertIn("01 10", row["request_hex"])
        self.assertEqual(self.store.one("SELECT result FROM events WHERE result='OUTPUT_WRITE_ACKNOWLEDGED'")["result"], "OUTPUT_WRITE_ACKNOWLEDGED")

    async def test_verified_4ma_and_20ma_setpoints(self):
        await self._device()
        low = await self.driver.set_output(self.node_id, 1, 4.0, source="MANUAL:test")
        high = await self.driver.set_output(self.node_id, 6, 20.0, source="MANUAL:test")
        self.assertAlmostEqual(low["acknowledged_ma"], 4.0)
        self.assertAlmostEqual(high["acknowledged_ma"], 20.0)

    async def test_range_rejection_sends_no_fc16(self):
        await self._device([1] * 6)
        with self.assertRaisesRegex(OutputError, "outside AO1's configured 4–20 mA range"):
            await self.driver.set_output(self.node_id, 1, 3.5, source="MANUAL:test")
        self.assertEqual(self.write_requests, [])

    async def test_fc16_ack_without_matching_setpoint_readback_is_failure(self):
        await self._device()
        self.ignore_writes = True
        with self.assertRaisesRegex(OutputError, "readback mismatch"):
            await self.driver.set_output(self.node_id, 1, 12.5, source="MANUAL:test")
        self.assertEqual(len(self.write_requests), 1)
        command=self.store.one("SELECT * FROM output_commands WHERE node_id=?",(self.node_id,))
        self.assertEqual(command["result"],"READBACK_MISMATCH")
        self.assertEqual(command["readback_ma"],0.0)

    async def test_fc16_timeout_is_unknown_and_never_blindly_retried(self):
        await self._device()
        self.store.set_setting("output_write_timeout_ms", "250")
        self.drop_write_ack = True
        with self.assertRaises(OutputOutcomeUnknown):
            await self.driver.set_output(self.node_id, 1, 12.5, source="MANUAL:test")
        self.assertEqual(len(self.write_requests),1)
        command=self.store.one("SELECT * FROM output_commands WHERE node_id=?",(self.node_id,))
        self.assertEqual(command["result"],"WRITE_OUTCOME_UNKNOWN")
        self.assertIn("01 10 00 00",command["request_hex"])
        self.assertTrue(self.session.desynchronized)

    async def test_output_timeouts_read_persistent_configuration_and_validate_bounds(self):
        await self._device()
        self.store.set_setting("output_range_timeout_ms","4200")
        self.assertEqual(self.driver.timeout_ms("output_range_timeout_ms"),4200)
        self.assertEqual(self.driver.timeout_ms("output_range_timeout_ms",3000),3000)
        with self.assertRaisesRegex(OutputError,"between 250 and 30000"):
            self.driver.timeout_ms("output_range_timeout_ms",100)

    async def test_manual_write_blocked_while_pipeline_owns_channel(self):
        await self._device()
        now = utc_now()
        self.store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)", ("source-gateway", "Input gateway", "MODBUS_TCP", now, now))
        self.store.run("INSERT INTO nodes(id,name,node_type,hardware_model,gateway_id,slave_id,profile_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                       ("source-node", "AI", "ANALOG_INPUT", "EBYTE ME31-XAXX0600", "source-gateway", 1, "ebyte-me31-xaxx0600-v1.1", now, now))
        self.store.run("INSERT INTO mappings(id,name,source_node_id,source_channel,destination_node_id,destination_channel,source_value_type,config_json,enabled,status,armed,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,1,'RUNNING',1,?,?)",
                       ("pipeline-1", "AI1 to AO1", "source-node", 1, self.node_id, 1, "CALIBRATED_MA", "{}", now, now))
        with self.assertRaisesRegex(OutputError, "armed pipeline owns"):
            await self.driver.set_output(self.node_id, 1, 10, source="MANUAL:test")
        self.assertEqual(self.write_requests, [])

    async def test_range_fc06_ao1_and_ao6_each_confirmed_by_fc03(self):
        await self._device()
        self.setpoints[0] = 12000
        self.setpoints[5] = 12000
        first = await self.driver.set_output_range(self.node_id, 1, 1, confirm=True)
        sixth = await self.driver.set_output_range(self.node_id, 6, 1, confirm=True)
        self.assertEqual(first["status"], "ACKNOWLEDGED")
        self.assertEqual(sixth["status"], "ACKNOWLEDGED")
        self.assertEqual(self.range_write_requests[0][:6], bytes.fromhex("01 06 05 14 00 01"))
        self.assertEqual(self.range_write_requests[1][:6], bytes.fromhex("01 06 05 19 00 01"))
        self.assertEqual(self.ranges[0], 1)
        self.assertEqual(self.ranges[5], 1)
        self.assertEqual(self.store.one("SELECT COUNT(*) count FROM output_range_changes WHERE result='ACKNOWLEDGED'")["count"], 2)
        self.assertEqual(self.store.one("SELECT result FROM events WHERE result='OUTPUT_RANGE_CHANGE_ACKNOWLEDGED' ORDER BY id DESC LIMIT 1")["result"], "OUTPUT_RANGE_CHANGE_ACKNOWLEDGED")

    async def test_range_change_rejects_incompatible_setpoint_without_fc06(self):
        await self._device()
        self.setpoints[0] = 3000
        with self.assertRaisesRegex(ValueError, "incompatible"):
            await self.driver.set_output_range(self.node_id, 1, 1, confirm=True)
        self.assertEqual(self.range_write_requests, [])
        self.assertEqual(self.ranges[0], 0)

    async def test_range_change_requires_confirmation_and_rejects_armed_pipeline(self):
        await self._device()
        with self.assertRaisesRegex(OutputError, "confirmation"):
            await self.driver.set_output_range(self.node_id, 1, 0, confirm=False)
        now = utc_now()
        self.store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)", ("source-gateway", "Input gateway", "MODBUS_TCP", now, now))
        self.store.run("INSERT INTO nodes(id,name,node_type,hardware_model,gateway_id,slave_id,profile_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
                       ("source-node", "AI", "ANALOG_INPUT", "EBYTE ME31-XAXX0600", "source-gateway", 1, "ebyte-me31-xaxx0600-v1.1", now, now))
        self.store.run("INSERT INTO mappings(id,name,source_node_id,source_channel,destination_node_id,destination_channel,source_value_type,config_json,enabled,status,armed,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,1,'RUNNING',1,?,?)",
                       ("pipeline-range-lock", "AI1 to AO1", "source-node", 1, self.node_id, 1, "CALIBRATED_MA", "{}", now, now))
        with self.assertRaisesRegex(OutputError, "armed pipeline"):
            await self.driver.set_output_range(self.node_id, 1, 1, confirm=True)
        self.assertEqual(self.range_write_requests, [])

    async def test_range_fc06_timeout_is_unknown_and_never_blindly_retried(self):
        await self._device()
        self.setpoints[0] = 12000
        self.store.set_setting("output_range_timeout_ms", "250")
        self.drop_range_ack = True
        with self.assertRaises(TimeoutError):
            await self.driver.set_output_range(self.node_id, 1, 1, confirm=True)
        self.assertEqual(len(self.range_write_requests), 1)
        self.assertTrue(self.session.desynchronized)
        row = self.store.one("SELECT result FROM output_range_changes WHERE node_id=?", (self.node_id,))
        self.assertEqual(row["result"], "RANGE_WRITE_OUTCOME_UNKNOWN")

    async def test_fc06_ack_without_range_readback_is_not_reported_as_success(self):
        await self._device()
        self.setpoints[0] = 12000
        self.ignore_range_writes = True
        with self.assertRaisesRegex(OutputReadbackError, "mismatch"):
            await self.driver.set_output_range(self.node_id, 1, 1, confirm=True)
        self.assertEqual(len(self.range_write_requests), 1)
        row = self.store.one("SELECT result FROM output_range_changes WHERE node_id=?", (self.node_id,))
        self.assertEqual(row["result"], "RANGE_READBACK_MISMATCH")


if __name__ == "__main__":
    unittest.main()
