import asyncio
import json
import struct
import unittest
from datetime import datetime, timedelta, timezone
from uuid import uuid4
from unittest.mock import patch

from backend.gateway import GatewaySession
from backend.output import OutputDriver
from backend.pipelines import PipelineError, PipelineService
from backend.store import Store, utc_now
from backend.modbus import with_crc
from backend import app as service_app


class SessionMap:
    def __init__(self):
        self.items = {}

    def for_gateway(self, gateway_id):
        session = self.items.get(gateway_id)
        return session if session and session.commissioned else None


class ConnectedStub:
    commissioned = True


class PipelineExecutionTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.ranges = [0] * 6
        self.setpoints = [0] * 6
        self.fc16 = []

        async def device(reader, writer):
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
                        body = bytes((quantity * 2,)) + b"".join(value.to_bytes(2, "big") for value in values)
                        writer.write(with_crc(prefix[:1] + bytes((3,)) + body))
                    elif function == 16:
                        rest = await reader.readexactly(11)
                        request = prefix + rest
                        self.fc16.append(request)
                        channel = int.from_bytes(request[2:4], "big") // 2
                        target = struct.unpack(">f", request[7:11])[0]
                        self.setpoints[channel] = round(target * 1000)
                        writer.write(with_crc(prefix[:1] + bytes((16,)) + request[2:6]))
                    else:
                        raise AssertionError(function)
                    await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        self.server = await asyncio.start_server(device, "127.0.0.1", 0)
        port = self.server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        self.output_session = GatewaySession(str(uuid4()), reader, writer, "127.0.0.1")
        await self.output_session.commission("gw-output", "TRANSPARENT_SERIAL")

        async def close(*_): pass
        async def invalid(_, detail): self.fail(detail)
        self.reader_task = asyncio.create_task(self.output_session.run_reader(close, invalid))
        self.sessions = SessionMap()
        self.sessions.items["gw-input"] = ConnectedStub()
        self.sessions.items["gw-output"] = self.output_session
        self.store = Store(":memory:")
        self.input_id, self.output_id = "ai-node", "ao-node"
        self.pipeline_id = "ai1-to-ao2"
        now = utc_now()
        self.store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)", ("gw-input", "Input G781", "MODBUS_TCP", now, now))
        self.store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)", ("gw-output", "Output W630", "TRANSPARENT_SERIAL", now, now))
        self.store.run("INSERT INTO nodes(id,name,node_type,hardware_model,gateway_id,slave_id,profile_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (self.input_id,"ME31 AI","ANALOG_INPUT","EBYTE ME31-XAXX0600","gw-input",1,"ebyte-me31-xaxx0600-v1.1",now,now))
        self.store.run("INSERT INTO nodes(id,name,node_type,hardware_model,gateway_id,slave_id,profile_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
            (self.output_id,"ME31 AO","ANALOG_OUTPUT","EBYTE ME31-XXXA0006","gw-output",1,"ebyte-me31-xxxa0006-v1.1",now,now))
        for index in range(1, 7):
            for node_id, kind, prefix in ((self.input_id,"ANALOG_INPUT","AI"),(self.output_id,"ANALOG_OUTPUT","AO")):
                self.store.run("INSERT INTO channels(id,node_id,channel_index,channel_type,label,created_at,updated_at) VALUES(?,?,?,?,?,?,?)",
                    (str(uuid4()),node_id,index,kind,f"{prefix}{index}",now,now))
        self.measure_at = now
        self.store.run("INSERT INTO polling_items(id,node_id,profile_id,register_name,function_code,address,quantity,data_type,interval_ms,timeout_ms,retries,scale,unit,measurement_metadata_json,enabled,last_value,last_success_at,last_status,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
            ("poll-ai-block",self.input_id,"ebyte-me31-xaxx0600-v1.1","AI current block",4,0x0064,6,"uint16_array",1000,1000,0,0.001,"mA",json.dumps({"type":"analog_current","channel_indices":[1,2,3,4,5,6]}),1,"[12.2,0,0,0,0,0]",now,"GOOD",now,now))
        self.store.run("INSERT INTO measurements(node_id,channel,register_value,raw_registers_json,raw_ma,calibration_revision,calibrated_ma,engineering_value,engineering_unit,quality,measured_at) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
            (self.input_id,1,12200,"[12200]",12.2,1,12.5,50.0,"bar","GOOD",now))
        self.output = OutputDriver(self.store, self.sessions)
        self.service = PipelineService(self.store, self.sessions, self.output)

    async def asyncTearDown(self):
        await self.output_session.retire()
        await asyncio.gather(self.reader_task, return_exceptions=True)
        self.server.close()
        await self.server.wait_closed()
        self.store.close()

    def definition(self, *, value_type="CALIBRATED_MA", config=None, destination_channel=2, pipeline_id=None):
        return {"id":pipeline_id, "name":"AI1 to AO2", "source_node_id":self.input_id, "source_channel":1,
                "destination_node_id":self.output_id, "destination_channel":destination_channel,
                "source_value_type":value_type, "config":config or {"transformation":"DIRECT","update_interval_ms":1000,
                    "deadband_ma":0.05,"stale_timeout_ms":5000,"timeout_ms":1000,"retries":1,"fail_safe_policy":"HOLD_LAST"}}

    async def _create_and_arm(self, definition=None):
        pipeline = self.service.create(definition or self.definition())
        with self.assertRaises(PipelineError):
            await self.service.enable(pipeline["id"], False)
        await self.service.enable(pipeline["id"], True)
        await self.service.arm(pipeline["id"], True)
        return pipeline["id"]

    async def test_ai1_calibrated_current_to_ao2_fc16_ack_and_readback(self):
        pipeline_id = await self._create_and_arm()
        await self.service.execute(pipeline_id)
        state = self.service.get(pipeline_id)
        self.assertEqual(len(self.fc16), 1)
        self.assertEqual(self.fc16[0][:7], bytes.fromhex("01 10 00 02 00 02 04"))
        self.assertAlmostEqual(struct.unpack(">f", self.fc16[0][7:11])[0], 12.5)
        self.assertEqual(state["status"], "RUNNING")
        self.assertAlmostEqual(state["last_source_value"], 12.5)
        self.assertAlmostEqual(state["last_requested_value"], 12.5)
        self.assertAlmostEqual(state["last_acknowledged_value"], 12.5)
        command = self.store.one("SELECT * FROM output_commands WHERE source=?", (f"PIPELINE:{pipeline_id}",))
        self.assertEqual(command["result"], "ACKNOWLEDGED")

    async def test_engineering_source_requires_and_applies_explicit_two_point_transform(self):
        config={"transformation":"TWO_POINT","input_min":0,"input_max":100,"output_min_ma":4,"output_max_ma":20,
                "update_interval_ms":1000,"deadband_ma":0,"stale_timeout_ms":5000,"timeout_ms":1000,"retries":0,"fail_safe_policy":"HOLD_LAST"}
        pipeline_id=await self._create_and_arm(self.definition(value_type="ENGINEERING",config=config))
        await self.service.execute(pipeline_id)
        self.assertAlmostEqual(struct.unpack(">f",self.fc16[0][7:11])[0],12.0)

    async def test_stale_input_sends_no_output_and_safe_setpoint_is_bounded(self):
        config={"transformation":"DIRECT","update_interval_ms":1000,"deadband_ma":0,"stale_timeout_ms":500,"timeout_ms":1000,"retries":0,
                "fail_safe_policy":"SAFE_SETPOINT","safe_setpoint_ma":4}
        pipeline_id=await self._create_and_arm(self.definition(config=config))
        old=(datetime.now(timezone.utc)-timedelta(seconds=10)).isoformat()
        self.store.run("UPDATE measurements SET measured_at=? WHERE node_id=?",(old,self.input_id))
        await self.service.execute(pipeline_id)
        self.assertEqual(len(self.fc16),1)
        self.assertAlmostEqual(struct.unpack(">f",self.fc16[0][7:11])[0],4.0)
        row=self.service.get(pipeline_id)
        self.assertEqual(row["status"],"FAILSAFE_ACKNOWLEDGED")
        self.assertTrue(row["failsafe_attempted"])

    async def test_source_disconnect_callback_attempts_safe_setpoint_once(self):
        config={"transformation":"DIRECT","update_interval_ms":1000,"deadband_ma":0,"stale_timeout_ms":5000,
                "timeout_ms":1000,"retries":0,"fail_safe_policy":"SAFE_SETPOINT","safe_setpoint_ma":4}
        pipeline_id=await self._create_and_arm(self.definition(config=config))
        callback_session=type("Session",(),{"gateway_id":"gw-input","session_id":"input-session",
            "desynchronized":False,"transport_mode":"MODBUS_TCP"})()
        registry=type("Registry",(),{"by_gateway":{"gw-input":"input-session"},"shutting_down":False,"sessions":{},"tasks":{}})()
        with patch.object(service_app,"store",self.store), patch.object(service_app,"sessions",registry), \
             patch.object(service_app,"pipelines",self.service):
            await service_app.on_disconnect(callback_session, ConnectionError("LTE link dropped"))
            await service_app.on_disconnect(callback_session, ConnectionError("duplicate close callback"))
        commands=self.store.rows("SELECT * FROM output_commands WHERE pipeline_id=?",(pipeline_id,))
        self.assertEqual(len(commands),1)
        self.assertEqual(commands[0]["classification"],"FAILSAFE")
        self.assertEqual(commands[0]["result"],"ACKNOWLEDGED")
        self.assertEqual(self.service.get(pipeline_id)["status"],"FAILSAFE_ACKNOWLEDGED")
        self.assertAlmostEqual(commands[0]["target_ma"],4.0)

    async def test_source_disconnect_with_output_down_is_failsafe_unconfirmed(self):
        config={"transformation":"DIRECT","update_interval_ms":1000,"deadband_ma":0,"stale_timeout_ms":5000,
                "timeout_ms":1000,"retries":0,"fail_safe_policy":"SAFE_SETPOINT","safe_setpoint_ma":4}
        pipeline_id=await self._create_and_arm(self.definition(config=config))
        self.sessions.items.pop("gw-output")
        callback_session=type("Session",(),{"gateway_id":"gw-input","session_id":"input-session",
            "desynchronized":False,"transport_mode":"MODBUS_TCP"})()
        registry=type("Registry",(),{"by_gateway":{"gw-input":"input-session"},"shutting_down":False,"sessions":{},"tasks":{}})()
        with patch.object(service_app,"store",self.store), patch.object(service_app,"sessions",registry), \
             patch.object(service_app,"pipelines",self.service):
            await service_app.on_disconnect(callback_session, ConnectionError("LTE link dropped"))
        self.assertEqual(self.fc16,[])
        row=self.service.get(pipeline_id)
        self.assertEqual(row["status"],"FAILSAFE_UNCONFIRMED")
        self.assertEqual(row["failsafe_status"],"UNCONFIRMED")
        self.assertFalse(row["armed"])

    async def test_backend_shutdown_disconnect_does_not_issue_failsafe_write(self):
        config={"transformation":"DIRECT","update_interval_ms":1000,"deadband_ma":0,"stale_timeout_ms":5000,
                "timeout_ms":1000,"retries":0,"fail_safe_policy":"SAFE_SETPOINT","safe_setpoint_ma":4}
        pipeline_id=await self._create_and_arm(self.definition(config=config))
        callback_session=type("Session",(),{"gateway_id":"gw-input","session_id":"input-session",
            "desynchronized":False,"transport_mode":"MODBUS_TCP"})()
        registry=type("Registry",(),{"by_gateway":{"gw-input":"input-session"},"shutting_down":True,"sessions":{},"tasks":{}})()
        with patch.object(service_app,"store",self.store), patch.object(service_app,"sessions",registry), \
             patch.object(service_app,"pipelines",self.service):
            await service_app.on_disconnect(callback_session, ConnectionError("server shutdown"))
        self.assertEqual(self.fc16,[])
        self.assertTrue(self.service.get(pipeline_id)["armed"])

    async def test_consecutive_write_failures_enter_manual_fault_lockout(self):
        pipeline_id=await self._create_and_arm(self.definition())
        row=self.service.get(pipeline_id)
        measurement=self.store.one("SELECT * FROM measurements WHERE node_id=?",(self.input_id,))
        for attempt in range(3):
            self.service._record_write_failure(row,12.5,"OUTPUT_WRITE_FAILED",f"simulated failure {attempt+1}",measurement,unknown=False)
        state=self.service.get(pipeline_id)
        self.assertEqual(state["status"],"OUTPUT_FAULT_LOCKOUT")
        self.assertFalse(state["armed"])
        self.assertEqual(state["consecutive_write_failures"],3)
        self.assertIsNotNone(state["faulted_at"])

    async def test_pipeline_loop_runs_independent_work_concurrently_with_bound(self):
        first=await self._create_and_arm(self.definition(destination_channel=2))
        other=self.definition(destination_channel=3,pipeline_id="parallel-pipeline")
        other["name"]="AI1 to AO3"
        second=await self._create_and_arm(other)
        started=asyncio.Event()
        release=asyncio.Event()
        completed=[]
        original=self.service.execute
        async def controlled(pipeline_id):
            if pipeline_id==first:
                started.set()
                await release.wait()
            else:
                completed.append(pipeline_id)
        self.service.execute=controlled
        self.service.MAX_CONCURRENT=2
        self.service.running=True
        loop=asyncio.create_task(self.service.loop())
        try:
            await asyncio.wait_for(started.wait(),1)
            await asyncio.wait_for(asyncio.sleep(0.5),1)
            self.assertIn(second,completed)
            self.assertLessEqual(len(self.service._workers),2)
            self.assertNotIn(first,completed)
        finally:
            release.set()
            self.service.running=False
            loop.cancel()
            await asyncio.gather(loop,return_exceptions=True)
            workers=list(self.service._workers.values())
            if workers: await asyncio.gather(*workers,return_exceptions=True)
            self.service.execute=original
            self.service._workers.clear()

    async def test_stale_input_with_hold_last_sends_no_fc16(self):
        pipeline_id=await self._create_and_arm()
        old=(datetime.now(timezone.utc)-timedelta(seconds=10)).isoformat()
        self.store.run("UPDATE measurements SET measured_at=? WHERE node_id=?",(old,self.input_id))
        await self.service.execute(pipeline_id)
        self.assertEqual(self.fc16,[])
        self.assertEqual(self.service.get(pipeline_id)["status"],"SOURCE_STALE")

    async def test_missing_engineering_source_uses_central_fail_safe_transition(self):
        config={"transformation":"TWO_POINT","input_min":0,"input_max":100,"output_min_ma":4,"output_max_ma":20,
                "update_interval_ms":1000,"deadband_ma":0,"stale_timeout_ms":5000,"timeout_ms":1000,"retries":0,
                "fail_safe_policy":"SAFE_SETPOINT","safe_setpoint_ma":4}
        pipeline_id=await self._create_and_arm(self.definition(value_type="ENGINEERING",config=config))
        self.store.run("UPDATE measurements SET engineering_value=NULL WHERE node_id=?",(self.input_id,))
        await self.service.execute(pipeline_id)
        self.assertEqual(len(self.fc16),1)
        self.assertAlmostEqual(struct.unpack(">f",self.fc16[0][7:11])[0],4.0)
        self.assertEqual(self.service.get(pipeline_id)["status"],"FAILSAFE_ACKNOWLEDGED")

    async def test_destination_owner_is_unique_and_restart_disarms(self):
        first=await self._create_and_arm()
        second=self.service.create(self.definition(pipeline_id="second",destination_channel=2))
        await self.service.enable(second["id"],True)
        with self.assertRaisesRegex(PipelineError,"Another enabled pipeline owns"):
            await self.service.arm(second["id"],True)
        self.assertFalse(self.service.get(second["id"])["armed"])
        self.service.start()
        self.assertFalse(self.service.get(first)["armed"])
        self.assertEqual(self.service.get(first)["status"],"DISARMED")
        await self.service.stop()

    async def test_one_input_can_fan_out_to_multiple_ao_channels(self):
        first=await self._create_and_arm(self.definition(destination_channel=2))
        second_definition=self.definition(destination_channel=3,pipeline_id="ai1-to-ao3")
        second_definition["name"]="AI1 to AO3"
        second=await self._create_and_arm(second_definition)
        await self.service.execute(first)
        await self.service.execute(second)
        self.assertEqual(len(self.fc16),2)
        self.assertEqual([int.from_bytes(request[2:4],"big") for request in self.fc16],[2,4])
        self.assertAlmostEqual(self.service.get(first)["last_acknowledged_value"],12.5)
        self.assertAlmostEqual(self.service.get(second)["last_acknowledged_value"],12.5)

    async def test_destination_disconnect_disarms_without_claiming_success(self):
        pipeline_id=await self._create_and_arm()
        self.sessions.items.pop("gw-output")
        await self.service.execute(pipeline_id)
        state=self.service.get(pipeline_id)
        self.assertFalse(state["armed"])
        self.assertEqual(state["status"],"OUTPUT_DISCONNECTED")
        self.assertIsNone(state["last_success_at"])
        self.assertEqual(self.fc16,[])

    async def test_direct_pipeline_does_not_apply_calibration_twice(self):
        definition=self.definition(value_type="RAW_MA",destination_channel=1)
        pipeline_id=await self._create_and_arm(definition)
        await self.service.execute(pipeline_id)
        self.assertAlmostEqual(struct.unpack(">f",self.fc16[0][7:11])[0],12.2,places=6)


if __name__ == "__main__":
    unittest.main()
