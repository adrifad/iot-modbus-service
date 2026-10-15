import asyncio
import time
import unittest
from uuid import uuid4
from unittest.mock import patch

from backend import app as service_app
from backend.app import SessionRegistry
from backend.gateway import GatewaySession
from backend.healthcheck import GatewayHealthMonitor
from backend.store import Store, utc_now


class StubWriter:
    def __init__(self):
        self.closed = False

    def get_extra_info(self, name):
        return None

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


class GatewayReconnectTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = Store(":memory:")
        self.registry = SessionRegistry(self.store)
        self.gateway_id = "gw-vpn-test"
        now = utc_now()
        self.store.run("""INSERT INTO gateways(id,name,model,transport_mode,created_at,updated_at,
            auto_reconnect_enabled,trusted_ingress_id,trusted_peer_ip,listener_port,identity_method,connection_state,commissioning_state)
            VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)""", (self.gateway_id,"VPN test gateway","USR-G781","MODBUS_TCP",now,now,1,"wg-test","127.0.0.1",0,"WIREGUARD_PEER_INGRESS","WAITING_FOR_RECONNECT","WAITING_FOR_RECONNECT"))

    async def asyncTearDown(self):
        if hasattr(self, "server"):
            self.server.close()
            await self.server.wait_closed()
        if hasattr(self, "client_writer"):
            self.client_writer.close()
            try:
                await self.client_writer.wait_closed()
            except ConnectionError:
                pass
        for session in list(self.registry.sessions.values()):
            await session.retire()
        self.store.close()

    async def test_new_tcp_sessions_auto_commission_by_persisted_vpn_binding(self):
        async def pipeline_disconnect(*_args, **_kwargs):
            return None

        class Pipelines:
            gateway_disconnected = staticmethod(pipeline_disconnect)

        self.store.run("UPDATE gateways SET listener_port=5101 WHERE id=?",(self.gateway_id,))
        binding = {"gateway_id":self.gateway_id,"ingress_id":"wg-test","trusted_peer_ip":"127.0.0.1","listener_port":5101}
        with patch.object(service_app,"store",self.store), patch.object(service_app,"sessions",self.registry), patch.object(service_app,"pipelines",Pipelines()):
            self.server = await asyncio.start_server(lambda r,w: service_app._handle_tcp_client(r,w,binding),"127.0.0.1",0)
            port = self.server.sockets[0].getsockname()[1]
            for expected_generation in (1, 2):
                self.client_reader, self.client_writer = await asyncio.open_connection("127.0.0.1",port)
                for _ in range(50):
                    active = self.registry.for_gateway(self.gateway_id)
                    if active and active.generation == expected_generation:
                        break
                    await asyncio.sleep(0.01)
                self.assertIsNotNone(active)
                self.assertTrue(active.identity_verified)
                self.assertEqual(active.commissioning_method,"WIREGUARD_INGRESS")
                self.assertEqual(active.generation,expected_generation)
                self.client_writer.close()
                await self.client_writer.wait_closed()
                await asyncio.sleep(0.02)
            persisted = self.store.one("SELECT auto_reconnect_enabled,session_generation,reconnection_count FROM gateways WHERE id=?",(self.gateway_id,))
            self.assertEqual(persisted["auto_reconnect_enabled"],1)
            self.assertEqual(persisted["session_generation"],2)
            self.assertEqual(persisted["reconnection_count"],1)

    async def test_untrusted_peer_cannot_use_a_gateway_ingress_binding(self):
        binding = {"gateway_id":self.gateway_id,"ingress_id":"wg-test","trusted_peer_ip":"192.0.2.44","listener_port":5101}
        async def callback(reader, writer):
            await service_app._handle_tcp_client(reader,writer,binding)
        with patch.object(service_app,"store",self.store), patch.object(service_app,"sessions",self.registry):
            self.server = await asyncio.start_server(callback,"127.0.0.1",0)
            port = self.server.sockets[0].getsockname()[1]
            reader, writer = await asyncio.open_connection("127.0.0.1",port)
            await asyncio.wait_for(reader.read(),1)
            writer.close()
            await writer.wait_closed()
        self.assertIsNone(self.registry.for_gateway(self.gateway_id))
        self.assertEqual(self.store.one("SELECT COUNT(*) count FROM events WHERE result='GATEWAY_IDENTITY_FAILED'")["count"],1)

    async def test_late_disconnect_from_old_generation_does_not_mark_new_session_offline(self):
        old = GatewaySession("old",asyncio.StreamReader(),StubWriter(),"old peer")
        current = GatewaySession("new",asyncio.StreamReader(),StubWriter(),"new peer")
        await current.commission(self.gateway_id,"MODBUS_TCP",identity_verified=True,commissioning_method="WIREGUARD_INGRESS")
        current.generation = 2
        self.registry.sessions = {old.session_id:old,current.session_id:current}
        self.registry.by_gateway[self.gateway_id] = current.session_id
        self.store.run("UPDATE gateways SET last_session_id='new',session_generation=2,connection_state='CONNECTED' WHERE id=?",(self.gateway_id,))
        with patch.object(service_app,"store",self.store), patch.object(service_app,"sessions",self.registry):
            await service_app.on_disconnect(old,ConnectionError("late old close"))
        row = self.store.one("SELECT connection_state,last_session_id FROM gateways WHERE id=?",(self.gateway_id,))
        self.assertEqual(row["connection_state"],"CONNECTED")
        self.assertEqual(row["last_session_id"],"new")
        self.assertIs(self.registry.for_gateway(self.gateway_id),current)

    async def test_late_modbus_response_from_retired_session_cannot_mark_new_generation_healthy(self):
        self.store.run("UPDATE gateways SET last_session_id='new',session_generation=2,connection_state='MODBUS_VERIFYING' WHERE id=?",(self.gateway_id,))
        old = GatewaySession("old",asyncio.StreamReader(),StubWriter(),"old peer")
        old.gateway_id = self.gateway_id
        old.generation = 1
        old.transport_mode = "MODBUS_TCP"
        with patch.object(service_app,"store",self.store):
            await service_app.on_gateway_modbus_success(old,2.0)
        row = self.store.one("SELECT connection_state,last_modbus_generation FROM gateways WHERE id=?",(self.gateway_id,))
        self.assertEqual(row["connection_state"],"MODBUS_VERIFYING")
        self.assertIsNone(row["last_modbus_generation"])

    async def test_simultaneous_verified_connections_receive_distinct_generations(self):
        first = GatewaySession("parallel-1",asyncio.StreamReader(),StubWriter(),"peer")
        second = GatewaySession("parallel-2",asyncio.StreamReader(),StubWriter(),"peer")
        self.registry.sessions = {first.session_id:first,second.session_id:second}
        gateway = self.store.one("SELECT * FROM gateways WHERE id=?",(self.gateway_id,))
        await asyncio.gather(
            self.registry.assign(first,gateway,identity_verified=True,commissioning_method="WIREGUARD_INGRESS"),
            self.registry.assign(second,gateway,identity_verified=True,commissioning_method="WIREGUARD_INGRESS"),
        )
        generations = {first.generation,second.generation}
        self.assertEqual(generations,{1,2})
        self.assertEqual(self.store.one("SELECT session_generation FROM gateways WHERE id=?",(self.gateway_id,))["session_generation"],2)
        active = self.registry.for_gateway(self.gateway_id)
        self.assertIn(active.session_id,{first.session_id,second.session_id})
        self.assertEqual(sum(session.commissioned for session in self.registry.sessions.values()),1)

    async def test_receive_idle_timeout_is_ignored_even_when_legacy_setting_is_set(self):
        session = GatewaySession("quiet",asyncio.StreamReader(),StubWriter(),"quiet peer")
        with patch.dict("os.environ",{"TCP_IDLE_TIMEOUT_SECONDS":"180"}):
            self.assertIsNone(session._read_timeout())


class HealthCheckTests(unittest.IsolatedAsyncioTestCase):
    async def test_quiet_w630_gets_only_read_only_modbus_probe(self):
        store = Store(":memory:")
        now = utc_now()
        store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)",("gw","Output","TRANSPARENT_SERIAL",now,now))
        store.run("INSERT INTO nodes(id,name,node_type,hardware_model,gateway_id,profile_id,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?)",("ao","Output node","ANALOG_OUTPUT","EBYTE ME31-XXXA0006","gw","ebyte-me31-xxxa0006-v1.1",now,now))

        class QuietSession:
            commissioned = True
            closed = False
            generation = 9
            session_id = "w630-session"
            transport_mode = "TRANSPARENT_SERIAL"
            last_request_at = time.perf_counter() - 181

            async def read_registers(self,*args):
                raise AssertionError("AO monitor must use the output read-only inspection interface")

        class Sessions:
            session = QuietSession()
            def for_gateway(self,gateway_id): return self.session

        class Output:
            calls = 0
            async def inspect(self,node_id,**kwargs):
                self.calls += 1
                self.args = (node_id,kwargs)
                return {"channels":[],"latency_ms":[12,14]}

        sessions, output = Sessions(), Output()
        monitor = GatewayHealthMonitor(store,sessions,output)
        with patch.dict("os.environ",{"TCP_IDLE_TIMEOUT_SECONDS":"180","GATEWAY_HEALTHCHECK_TIMEOUT_MS":"5000"}):
            await monitor.check_gateway("gw")
        self.assertEqual(output.calls,1)
        self.assertEqual(output.args[1],{"range_timeout_ms":5000,"readback_timeout_ms":5000})
        self.assertLess(time.perf_counter() - sessions.session.last_request_at, 182)
        self.assertTrue(sessions.session.commissioned)
        store.close()


if __name__ == "__main__":
    unittest.main()
