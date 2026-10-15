import asyncio
import unittest

from backend.node_lifecycle import NodeBusyError, NodeOperationError, NodeOperationGate
from backend.store import Store, utc_now


class NodeOperationGateTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store = Store(":memory:")
        now = utc_now()
        self.store.run("INSERT INTO nodes(id,name,node_type,created_at,updated_at) VALUES('node-1','Node 1','ANALOG_INPUT',?,?)", (now,now))
        self.gate = NodeOperationGate(self.store)

    async def asyncTearDown(self):
        self.store.close()

    async def test_archive_barrier_blocks_new_work_and_waits_for_inflight_operation(self):
        entered = asyncio.Event()
        release = asyncio.Event()

        async def work():
            async with self.gate.operation("node-1"):
                entered.set()
                await release.wait()

        active = asyncio.create_task(work())
        await entered.wait()
        archive = asyncio.create_task(self.gate.begin_archive("node-1", 1.0))
        await asyncio.sleep(0)
        with self.assertRaises(NodeOperationError):
            async with self.gate.operation("node-1"):
                self.fail("new operation crossed archive barrier")
        self.assertFalse(archive.done())
        release.set()
        await active
        await archive
        now = utc_now()
        self.store.run("UPDATE nodes SET archived_at=?,polling_enabled=0 WHERE id='node-1'", (now,))
        await self.gate.finish_archive("node-1", archived=True)
        with self.assertRaises(NodeOperationError):
            async with self.gate.operation("node-1"):
                self.fail("archived node started work")

    async def test_archive_timeout_releases_barrier_without_claiming_success(self):
        entered = asyncio.Event()
        release = asyncio.Event()

        async def work():
            async with self.gate.operation("node-1"):
                entered.set()
                await release.wait()

        active = asyncio.create_task(work())
        await entered.wait()
        with self.assertRaises(NodeBusyError):
            await self.gate.begin_archive("node-1", 0.01)
        release.set()
        await active
        async with self.gate.operation("node-1"):
            pass


if __name__ == "__main__":
    unittest.main()
