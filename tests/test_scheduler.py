import asyncio
import json
import unittest
from uuid import uuid4

from backend.gateway import GatewaySession
from backend.scheduler import PollScheduler
from backend.store import Store, utc_now


class Sessions:
    def __init__(self, session): self.session = session
    def for_gateway(self, gateway_id): return self.session if self.session.gateway_id == gateway_id else None


class SchedulerIntegrationTests(unittest.IsolatedAsyncioTestCase):
    async def test_simulated_modbus_tcp_read_persists_raw_and_calibrated_values(self):
        async def simulator(reader, writer):
            try:
                request = await reader.readexactly(12)
                transaction = request[:2]
                unit, function = request[6], request[7]
                writer.write(transaction + bytes.fromhex("00 00 00 05") + bytes((unit, function, 2, 0x2E, 0xE0)))
                await writer.drain()
                await reader.read()
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(simulator, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        store = Store(":memory:")
        gateway_id, node_id, item_id = str(uuid4()), str(uuid4()), str(uuid4())
        now = utc_now()
        store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)", (gateway_id,"Test gateway","MODBUS_TCP",now,now))
        store.run("INSERT INTO nodes(id,name,node_type,gateway_id,slave_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)", (node_id,"Test input","ANALOG_INPUT",gateway_id,1,now,now))
        store.run("INSERT INTO polling_items(id,node_id,profile_id,register_name,function_code,address,quantity,data_type,interval_ms,timeout_ms,retries,scale,unit,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (item_id,node_id,"ebyte-me31-xaxx0600-v1.1","AI1 current",4,0x0064,1,"uint16",1000,500,0,0.001,"mA",1,now,now))
        store.run("INSERT INTO calibrations(node_id,channel,revision,gain,offset_ma,enabled,method,modified_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)", (node_id,1,1,1.1,0.1,1,"GAIN_OFFSET","test",now))
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        session = GatewaySession(str(uuid4()), reader, writer, "127.0.0.1")
        session.gateway_id = gateway_id
        session.transport_mode = "MODBUS_TCP"
        async def closed(_): pass
        async def invalid(_, detail): raise AssertionError(detail)
        reader_task = asyncio.create_task(session.run_reader(closed, invalid))
        scheduler = PollScheduler(store, Sessions(session))
        item = store.one("SELECT p.*,n.gateway_id,n.slave_id,n.polling_enabled FROM polling_items p JOIN nodes n ON n.id=p.node_id WHERE p.id=?", (item_id,))
        try:
            await scheduler.poll(item)
            measured = store.one("SELECT * FROM measurements WHERE node_id=? AND channel=1", (node_id,))
            self.assertIsNotNone(measured)
            self.assertEqual(measured["register_value"], 12000)
            self.assertEqual(json.loads(measured["raw_registers_json"]), [12000])
            self.assertAlmostEqual(measured["raw_ma"], 12.0)
            self.assertAlmostEqual(measured["calibrated_ma"], 13.3)
            self.assertEqual(store.one("SELECT last_status FROM polling_items WHERE id=?", (item_id,))["last_status"], "GOOD")
            self.assertEqual(scheduler.success, 1)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except ConnectionError: pass
            await asyncio.wait_for(reader_task, 1)
            server.close(); await server.wait_closed(); store.db.close()


if __name__ == "__main__":
    unittest.main()
