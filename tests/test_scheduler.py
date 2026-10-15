import asyncio
import json
import unittest
from uuid import uuid4

from backend.gateway import GatewaySession
from backend.modbus import StreamFrames
from backend.scheduler import PollScheduler
from backend.profiles import ME31
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
        store.run("INSERT INTO channels(id,node_id,channel_index,channel_type,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",(str(uuid4()),node_id,1,"ANALOG_INPUT","AI1",now,now))
        store.run("INSERT INTO polling_items(id,node_id,profile_id,register_name,function_code,address,quantity,data_type,interval_ms,timeout_ms,retries,scale,unit,measurement_metadata_json,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                  (item_id,node_id,"ebyte-me31-xaxx0600-v1.1","AI1 current",4,0x0064,1,"uint16",1000,500,0,0.001,"mA",json.dumps({"type":"analog_current","channel_indices":[1],"conversion":"register_scale"}),1,now,now))
        store.run("INSERT INTO calibrations(node_id,channel,revision,gain,offset_ma,enabled,method,modified_by,created_at) VALUES(?,?,?,?,?,?,?,?,?)", (node_id,1,1,1.1,0.1,1,"GAIN_OFFSET","test",now))
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        session = GatewaySession(str(uuid4()), reader, writer, "127.0.0.1")
        session.gateway_id = gateway_id
        session.transport_mode = "MODBUS_TCP"
        session.state = "COMMISSIONED"
        session.parser = StreamFrames.create("MODBUS_TCP")
        async def closed(*_): pass
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
            server.close(); await server.wait_closed(); store.close()

    async def test_named_me31_register_reads_do_not_infer_channels_from_names(self):
        async def simulator(reader, writer):
            try:
                while request := await reader.readexactly(12):
                    transaction, unit, function = request[:2], request[6], request[7]
                    address = int.from_bytes(request[8:10], "big")
                    quantity = int.from_bytes(request[10:12], "big")
                    if 0x64 <= address < 0x6A:
                        values = [12000 + address - 0x64 + offset for offset in range(quantity)]
                    elif address == 0x7D0:
                        values = [0x4542 + offset for offset in range(quantity)]
                    else:
                        values = [offset + 1 for offset in range(quantity)]
                    data = bytes((quantity * 2,)) + b"".join(value.to_bytes(2, "big") for value in values)
                    writer.write(transaction + bytes.fromhex("00 00") + (2 + len(data)).to_bytes(2, "big") + bytes((unit, function)) + data)
                    await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(simulator, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        store = Store(":memory:")
        gateway_id, node_id = str(uuid4()), str(uuid4())
        now = utc_now()
        store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)", (gateway_id,"Profile gateway","MODBUS_TCP",now,now))
        store.run("INSERT INTO nodes(id,name,node_type,gateway_id,slave_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?)", (node_id,"Profile input","ANALOG_INPUT",gateway_id,1,now,now))
        for channel in range(1,7):
            store.run("INSERT INTO channels(id,node_id,channel_index,channel_type,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",(str(uuid4()),node_id,channel,"ANALOG_INPUT",f"AI{channel}",now,now))
        definitions = [definition for definition in ME31["registers"] if definition["name"] in {
            "AI1 current","AI2 current","AI3 current","AI4 current","AI5 current","AI6 current",
            "AI current block","AI filtering","AI sampling range","AI trigger high thresholds",
            "AI trigger low thresholds","AI trigger mode","Firmware version","Device model"}]
        definitions.append({"name":"Custom holding register","address":0x0100,"function_code":3,"quantity":1,"data_type":"uint16","scale":1,"unit":"","measurement":{}})
        item_ids=[]
        for definition in definitions:
            item_id=str(uuid4());item_ids.append((item_id,definition["name"]))
            store.run("INSERT INTO polling_items(id,node_id,profile_id,register_name,function_code,address,quantity,data_type,byte_order,word_order,interval_ms,timeout_ms,retries,scale,unit,measurement_metadata_json,enabled,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                      (item_id,node_id,"ebyte-me31-xaxx0600-v1.1" if "measurement" in definition else None,definition["name"],definition["function_code"],definition["address"],definition["quantity"],definition["data_type"],definition.get("byte_order","big"),"BIG_WORD_FIRST",1000,500,0,definition.get("scale",1),definition.get("unit",""),json.dumps(definition.get("measurement",{})),1,now,now))
        reader, writer = await asyncio.open_connection("127.0.0.1",port)
        session=GatewaySession(str(uuid4()),reader,writer,"127.0.0.1:0");session.gateway_id=gateway_id
        session.transport_mode="MODBUS_TCP";session.state="COMMISSIONED";session.parser=StreamFrames.create("MODBUS_TCP")
        async def closed(*_): pass
        async def invalid(_, detail): raise AssertionError(detail)
        reader_task=asyncio.create_task(session.run_reader(closed,invalid))
        scheduler=PollScheduler(store,Sessions(session))
        try:
            for item_id,_ in item_ids:
                item=store.one("SELECT p.*,n.gateway_id,n.slave_id,n.polling_enabled FROM polling_items p JOIN nodes n ON n.id=p.node_id WHERE p.id=?",(item_id,))
                await scheduler.poll(item)
            states={item["register_name"]:item["last_status"] for item in store.rows("SELECT register_name,last_status FROM polling_items")}
            self.assertTrue(all(state=="GOOD" for state in states.values()),states)
            self.assertEqual(store.one("SELECT COUNT(*) count FROM measurements WHERE node_id=?",(node_id,))["count"],12)
            self.assertEqual(store.one("SELECT COUNT(*) count FROM events WHERE result='SUCCESS'",())["count"],len(definitions))
            self.assertEqual(store.one("SELECT COUNT(*) count FROM measurements WHERE channel=1 AND node_id=?",(node_id,))["count"],2)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except ConnectionError: pass
            await asyncio.wait_for(reader_task,1)
            server.close();await server.wait_closed();store.close()


class SchedulerBatchTests(unittest.TestCase):
    def test_adjacent_me31_measurement_items_form_one_read_group(self):
        scheduler = PollScheduler(Store(":memory:"), None)
        items = []
        for channel in range(1, 7):
            items.append({
                "id": str(channel), "node_id": "node", "gateway_id": "gateway", "slave_id": 1,
                "function_code": 4, "address": 0x0063 + channel, "quantity": 1,
                "data_type": "uint16", "interval_ms": 1000, "timeout_ms": 1500,
                "retries": 1, "scale": 0.001, "profile_id": ME31["id"],
                "measurement_metadata_json": json.dumps({"type": "analog_current", "conversion": "register_scale", "channel_indices": [channel]}),
            })
        groups = scheduler._batch_due_items(items)
        self.assertEqual(len(groups), 1)
        self.assertEqual(len(groups[0]), 6)
        self.assertEqual(sum(item["quantity"] for item in groups[0]), 6)
        scheduler.store.close()


if __name__ == "__main__":
    unittest.main()
