"""Single-process operation barriers for safely archiving nodes."""

import asyncio
import contextvars
from contextlib import asynccontextmanager


class NodeOperationError(RuntimeError):
    pass


class NodeBusyError(NodeOperationError):
    pass


class NodeOperationGate:
    """Fence new operations and drain in-flight work before archival.

    SQLite triggers provide the persistent race barrier for new pipeline
    definitions. This in-process gate coordinates Modbus operations in the
    application's supported single-process deployment.
    """

    def __init__(self, store):
        self.store = store
        self._condition = asyncio.Condition()
        self._active: dict[str, int] = {}
        self._blocked: set[str] = set()
        self._held = contextvars.ContextVar(f"node_operation_gate_{id(self)}", default=frozenset())

    def is_archived(self, node_id: str) -> bool:
        row = self.store.one("SELECT archived_at FROM nodes WHERE id=?", (node_id,))
        return row is None or row["archived_at"] is not None

    @asynccontextmanager
    async def operation(self, *node_ids: str):
        requested = frozenset(node_ids)
        inherited = self._held.get()
        additional = requested - inherited
        token = None
        if additional:
            async with self._condition:
                unavailable = [node_id for node_id in additional
                               if node_id in self._blocked or self.is_archived(node_id)]
                if unavailable:
                    raise NodeOperationError(f"Node {unavailable[0]} is archived or being archived")
                for node_id in additional:
                    self._active[node_id] = self._active.get(node_id, 0) + 1
            token = self._held.set(inherited | requested)
        try:
            yield
        finally:
            if token is not None:
                self._held.reset(token)
                async with self._condition:
                    for node_id in additional:
                        remaining = self._active.get(node_id, 0) - 1
                        if remaining > 0:
                            self._active[node_id] = remaining
                        else:
                            self._active.pop(node_id, None)
                    self._condition.notify_all()

    async def begin_archive(self, node_id: str, timeout: float):
        async with self._condition:
            if node_id in self._blocked:
                raise NodeBusyError("Node archival is already in progress")
            self._blocked.add(node_id)
            try:
                async with asyncio.timeout(timeout):
                    await self._wait_idle(node_id)
            except asyncio.TimeoutError as exc:
                self._blocked.discard(node_id)
                self._condition.notify_all()
                raise NodeBusyError(
                    f"Node still has {self._active.get(node_id, 0)} in-flight operation(s); "
                    "wait for Modbus work to finish and retry"
                ) from exc

    async def _wait_idle(self, node_id: str):
        while self._active.get(node_id, 0):
            await self._condition.wait()

    async def finish_archive(self, node_id: str, *, archived: bool):
        if archived:
            return
        async with self._condition:
            self._blocked.discard(node_id)
            self._condition.notify_all()
