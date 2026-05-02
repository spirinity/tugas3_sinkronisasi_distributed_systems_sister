"""
Distributed Lock Manager with shared/exclusive locks,
deadlock detection via wait-for graph, and lease-based expiry.
Built on top of Raft consensus for replication.
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Dict, List, Optional, Set

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig
from src.consensus.raft import RaftConsensus, RaftState
from src.communication.message_passing import Message, MessageType

logger = logging.getLogger(__name__)


class LockType(str, Enum):
    SHARED = "shared"
    EXCLUSIVE = "exclusive"


class LockState:
    def __init__(self, resource: str):
        self.resource = resource
        self.lock_type: Optional[LockType] = None
        self.holders: Set[str] = set()
        self.waiting: List[dict] = []  # [{"client_id", "lock_type", "timestamp"}]
        self.fencing_token: int = 0
        self.lease_expiry: float = 0.0

    def to_dict(self):
        return {
            "resource": self.resource,
            "lock_type": self.lock_type.value if self.lock_type else None,
            "holders": list(self.holders),
            "waiting": self.waiting,
            "fencing_token": self.fencing_token,
            "lease_expiry": self.lease_expiry,
        }


class DeadlockDetector:
    """Detects deadlocks using wait-for graph with DFS cycle detection."""

    def __init__(self):
        self.wait_for: Dict[str, Set[str]] = {}  # client -> set of clients it waits for

    def add_wait(self, waiter: str, holders: Set[str]):
        if waiter not in self.wait_for:
            self.wait_for[waiter] = set()
        self.wait_for[waiter].update(holders - {waiter})

    def remove_client(self, client_id: str):
        self.wait_for.pop(client_id, None)
        for s in self.wait_for.values():
            s.discard(client_id)

    def detect_cycles(self) -> List[List[str]]:
        """Detect all cycles in the wait-for graph using DFS."""
        cycles = []
        visited = set()
        rec_stack = set()

        def dfs(node, path):
            visited.add(node)
            rec_stack.add(node)
            path.append(node)
            for neighbor in self.wait_for.get(node, set()):
                if neighbor not in visited:
                    dfs(neighbor, path)
                elif neighbor in rec_stack:
                    idx = path.index(neighbor)
                    cycles.append(path[idx:].copy())
            path.pop()
            rec_stack.discard(node)

        for node in list(self.wait_for.keys()):
            if node not in visited:
                dfs(node, [])
        return cycles


class LockManagerNode(BaseNode):
    """Distributed Lock Manager node with Raft consensus."""

    def __init__(self, config: NodeConfig):
        super().__init__(config)
        self.raft = RaftConsensus(
            node_id=self.node_id,
            peers=self.peers,
            messenger=self.messenger,
            election_timeout_min=config.raft_election_timeout_min,
            election_timeout_max=config.raft_election_timeout_max,
            heartbeat_interval=config.raft_heartbeat_interval,
        )
        self.locks: Dict[str, LockState] = {}
        self.deadlock_detector = DeadlockDetector()
        self.lease_timeout = config.lock_lease_timeout
        self.raft.on_commit(self._apply_lock_command)
        self._setup_lock_routes()

    def _setup_lock_routes(self):
        self.app.router.add_post("/lock/acquire", self._handle_acquire)
        self.app.router.add_post("/lock/release", self._handle_release)
        self.app.router.add_get("/lock/status", self._handle_lock_status)
        self.app.router.add_get("/lock/deadlocks", self._handle_deadlocks)
        self.app.router.add_get("/raft/status", self._handle_raft_status)

    async def start(self):
        await super().start()
        await self.raft.start()
        asyncio.create_task(self._lease_expiry_loop())
        logger.info(f"[{self.node_id}] Lock Manager started")

    async def stop(self):
        await self.raft.stop()
        await super().stop()

    async def handle_message(self, message: Message):
        raft_types = {MessageType.VOTE_REQUEST, MessageType.VOTE_RESPONSE,
                      MessageType.APPEND_ENTRIES, MessageType.APPEND_ENTRIES_RESPONSE}
        if message.msg_type in raft_types:
            return await self.raft.handle_message(message)
        return await super().handle_message(message)

    def _apply_lock_command(self, entry):
        """Apply a committed lock command to the state machine."""
        cmd = entry.command
        action = cmd.get("action")
        resource = cmd.get("resource")
        client_id = cmd.get("client_id")

        if action == "acquire":
            lock_type = LockType(cmd.get("lock_type", "exclusive"))
            self._do_acquire(resource, client_id, lock_type, cmd.get("lease_timeout", self.lease_timeout))
        elif action == "release":
            self._do_release(resource, client_id)

    def _do_acquire(self, resource, client_id, lock_type, lease_timeout):
        if resource not in self.locks:
            self.locks[resource] = LockState(resource)
        lock = self.locks[resource]

        if not lock.holders:
            lock.lock_type = lock_type
            lock.holders.add(client_id)
            lock.fencing_token += 1
            lock.lease_expiry = time.time() + lease_timeout
            self.deadlock_detector.remove_client(client_id)
            self.metrics.counter_inc("lock_acquired_total", labels={"type": lock_type.value})
        elif lock_type == LockType.SHARED and lock.lock_type == LockType.SHARED:
            lock.holders.add(client_id)
            lock.lease_expiry = max(lock.lease_expiry, time.time() + lease_timeout)
            self.deadlock_detector.remove_client(client_id)
            self.metrics.counter_inc("lock_acquired_total", labels={"type": "shared"})
        else:
            lock.waiting.append({
                "client_id": client_id, "lock_type": lock_type.value,
                "timestamp": time.time()
            })
            self.deadlock_detector.add_wait(client_id, lock.holders)
            self.metrics.counter_inc("lock_contention_total")

    def _do_release(self, resource, client_id):
        if resource not in self.locks:
            return
        lock = self.locks[resource]
        lock.holders.discard(client_id)
        self.deadlock_detector.remove_client(client_id)
        self.metrics.counter_inc("lock_released_total")

        if not lock.holders and lock.waiting:
            next_req = lock.waiting.pop(0)
            nlt = LockType(next_req["lock_type"])
            lock.lock_type = nlt
            lock.holders.add(next_req["client_id"])
            lock.fencing_token += 1
            lock.lease_expiry = time.time() + self.lease_timeout
            self.deadlock_detector.remove_client(next_req["client_id"])
            if nlt == LockType.SHARED:
                while lock.waiting and lock.waiting[0]["lock_type"] == "shared":
                    sw = lock.waiting.pop(0)
                    lock.holders.add(sw["client_id"])
                    self.deadlock_detector.remove_client(sw["client_id"])

    async def _lease_expiry_loop(self):
        while self._running:
            try:
                now = time.time()
                for resource in list(self.locks.keys()):
                    lock = self.locks[resource]
                    if lock.holders and lock.lease_expiry > 0 and now > lock.lease_expiry:
                        logger.info(f"[{self.node_id}] Lease expired for {resource}")
                        for holder in list(lock.holders):
                            self._do_release(resource, holder)
                await asyncio.sleep(1.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Lease expiry error: {e}")
                await asyncio.sleep(1)

    # --- HTTP Handlers ---

    async def _handle_acquire(self, request: web.Request) -> web.Response:
        data = await request.json()
        if not self.raft.is_leader:
            return web.json_response(
                {"error": "not_leader", "leader_id": self.raft.leader_id}, status=307)
        resource = data.get("resource")
        client_id = data.get("client_id", "anonymous")
        lock_type = data.get("lock_type", "exclusive")
        with self.metrics.timer("lock_acquire_latency"):
            ok = await self.raft.propose({
                "action": "acquire", "resource": resource,
                "client_id": client_id, "lock_type": lock_type,
                "lease_timeout": data.get("timeout", self.lease_timeout),
            })
        if ok:
            lock = self.locks.get(resource)
            return web.json_response({
                "status": "acquired" if lock and client_id in lock.holders else "waiting",
                "fencing_token": lock.fencing_token if lock else 0,
            })
        return web.json_response({"error": "commit_failed"}, status=500)

    async def _handle_release(self, request: web.Request) -> web.Response:
        data = await request.json()
        if not self.raft.is_leader:
            return web.json_response(
                {"error": "not_leader", "leader_id": self.raft.leader_id}, status=307)
        ok = await self.raft.propose({
            "action": "release",
            "resource": data.get("resource"),
            "client_id": data.get("client_id", "anonymous"),
        })
        return web.json_response({"status": "released" if ok else "failed"})

    async def _handle_lock_status(self, request: web.Request) -> web.Response:
        locks = {r: l.to_dict() for r, l in self.locks.items()}
        return web.json_response({"locks": locks})

    async def _handle_deadlocks(self, request: web.Request) -> web.Response:
        cycles = self.deadlock_detector.detect_cycles()
        return web.json_response({"deadlocks": cycles, "wait_for": {
            k: list(v) for k, v in self.deadlock_detector.wait_for.items()
        }})

    async def _handle_raft_status(self, request: web.Request) -> web.Response:
        return web.json_response(self.raft.get_state())
