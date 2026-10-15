import asyncio
import tempfile
import unittest
from uuid import uuid4

from backend.app import SessionRegistry
from backend.gateway import GatewaySession, TransportDesynchronized
from backend.modbus import with_crc
from backend.store import Store, utc_now


class FakeWriter:
    def __init__(self):
        self.closed = False

    def get_extra_info(self, _name):
        return None

    def close(self):
        self.closed = True

    async def wait_closed(self):
        return None


class GatewayLifecycleTests(unittest.IsolatedAsyncioTestCase):
    async def test_explicit_recommission_replaces_session_without_ip_identity(self):
        store = Store(":memory:")
        gateway_id = str(uuid4())
        now = utc_now()
        store.run("INSERT INTO gateways(id,name,transport_mode,created_at,updated_at) VALUES(?,?,?,?,?)",
                  (gateway_id, "LTE gateway", "TRANSPARENT_SERIAL", now, now))
        registry = SessionRegistry(store)
        old = GatewaySession("old", asyncio.StreamReader(), FakeWriter(), "198.51.100.3:1400")
        old.state = "UNCOMMISSIONED"
        new = GatewaySession("new", asyncio.StreamReader(), FakeWriter(), "203.0.113.9:51000")
        new.state = "UNCOMMISSIONED"
        registry.sessions = {old.session_id: old, new.session_id: new}

        await registry.assign(old, store.one("SELECT * FROM gateways WHERE id=?", (gateway_id,)))
        await registry.assign(new, store.one("SELECT * FROM gateways WHERE id=?", (gateway_id,)))

        self.assertEqual(registry.for_gateway(gateway_id), new)
        self.assertEqual(old.state, "RETIRED")
        self.assertEqual(new.state, "COMMISSIONED")
        self.assertEqual(new.peer, "203.0.113.9:51000")
        self.assertEqual(store.one("SELECT reconnection_count FROM gateways WHERE id=?", (gateway_id,))["reconnection_count"], 1)
        self.assertEqual(store.one("SELECT COUNT(*) count FROM events WHERE result='SESSION_REPLACED'")["count"], 1)
        store.close()

    async def test_rtu_requests_for_multiple_slaves_are_serialized_per_gateway(self):
        active = 0
        maximum_active = 0

        async def simulated_gateway(reader, writer):
            nonlocal active, maximum_active
            try:
                while request := await reader.readexactly(8):
                    active += 1
                    maximum_active = max(maximum_active, active)
                    unit, function = request[0], request[1]
                    address = int.from_bytes(request[2:4], "big")
                    await asyncio.sleep(0.02)
                    writer.write(with_crc(bytes((unit, function, 2)) + address.to_bytes(2, "big")))
                    await writer.drain()
                    active -= 1
            except (asyncio.IncompleteReadError, ConnectionError, OSError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(simulated_gateway, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        session = GatewaySession(str(uuid4()), reader, writer, "127.0.0.1")
        await session.commission(str(uuid4()), "TRANSPARENT_SERIAL")

        async def closed(*_): return None
        async def invalid(*_): return None
        reader_task = asyncio.create_task(session.run_reader(closed, invalid))
        try:
            first, second = await asyncio.gather(
                session.read_registers(1, 4, 0x0064, 1, 500),
                session.read_registers(2, 4, 0x0100, 1, 500),
            )
            self.assertEqual(first[0], [0x0064])
            self.assertEqual(second[0], [0x0100])
            self.assertEqual(maximum_active, 1)
        finally:
            writer.close()
            try: await writer.wait_closed()
            except ConnectionError: pass
            await asyncio.wait_for(reader_task, 1)
            server.close()
            await server.wait_closed()

    async def test_rtu_timeout_quarantines_late_response_and_requires_reconnect(self):
        response = with_crc(bytes.fromhex("01 04 02 2E E0"))

        async def silent_then_late(reader, writer):
            try:
                await reader.readexactly(8)
                await asyncio.sleep(0.08)
                writer.write(response)
                await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError, OSError):
                pass
            finally:
                writer.close()
                await writer.wait_closed()

        server = await asyncio.start_server(silent_then_late, "127.0.0.1", 0)
        port = server.sockets[0].getsockname()[1]
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        session = GatewaySession(str(uuid4()), reader, writer, "127.0.0.1:dynamic")
        await session.commission(str(uuid4()), "TRANSPARENT_SERIAL")

        async def closed(*_):
            return None

        async def invalid(*_):
            return None

        reader_task = asyncio.create_task(session.run_reader(closed, invalid))
        try:
            with self.assertRaises(TimeoutError):
                await session.read_registers(1, 4, 0x0064, 1, 20)
            self.assertTrue(session.desynchronized)
            with self.assertRaises(TransportDesynchronized):
                await session.read_registers(1, 4, 0x0064, 1, 100)
        finally:
            writer.close()
            try:
                await writer.wait_closed()
            except ConnectionError:
                pass
            await asyncio.wait_for(reader_task, 1)
            server.close()
            await server.wait_closed()


if __name__ == "__main__":
    unittest.main()
