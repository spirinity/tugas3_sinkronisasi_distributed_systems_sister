"""
Async data replicator with eventual consistency using vector clocks
for conflict detection and last-writer-wins resolution.
"""

import asyncio
import json
import logging
import time
from enum import Enum
from typing import Any, Dict, List, Optional

from src.communication.message_passing import MessagePassing, Message, MessageType
from src.geo.region_manager import RegionManager

logger = logging.getLogger(__name__)


class ReplicationStatus(str, Enum):
    PENDING = "pending"
    REPLICATED = "replicated"
    CONFLICT = "conflict"


class VectorClock:
    """Vector clock for tracking causal ordering of events across nodes."""

    def __init__(self, clock: Dict[str, int] = None):
        self._clock: Dict[str, int] = clock or {}

    def increment(self, node_id: str):
        self._clock[node_id] = self._clock.get(node_id, 0) + 1

    def merge(self, other: "VectorClock"):
        for nid, ts in other._clock.items():
            self._clock[nid] = max(self._clock.get(nid, 0), ts)

    def is_concurrent(self, other: "VectorClock") -> bool:
        """Check if two vector clocks are concurrent (neither dominates)."""
        self_gte = True
        other_gte = True
        all_keys = set(self._clock.keys()) | set(other._clock.keys())
        for k in all_keys:
            sv = self._clock.get(k, 0)
            ov = other._clock.get(k, 0)
            if sv < ov:
                self_gte = False
            if ov < sv:
                other_gte = False
        return not self_gte and not other_gte

    def dominates(self, other: "VectorClock") -> bool:
        """Check if self dominates (happened-after) other."""
        dominated = False
        for k in set(self._clock.keys()) | set(other._clock.keys()):
            sv = self._clock.get(k, 0)
            ov = other._clock.get(k, 0)
            if sv < ov:
                return False
            if sv > ov:
                dominated = True
        return dominated

    def to_dict(self):
        return dict(self._clock)

    @classmethod
    def from_dict(cls, d):
        return cls(clock=dict(d) if d else {})


class ReplicatedEntry:
    def __init__(self, key: str, value: Any, vector_clock: VectorClock,
                 origin_node: str, timestamp: float = None,
                 status: ReplicationStatus = ReplicationStatus.PENDING):
        self.key = key
        self.value = value
        self.vector_clock = vector_clock
        self.origin_node = origin_node
        self.timestamp = timestamp or time.time()
        self.status = status

    def to_dict(self):
        return {
            "key": self.key, "value": self.value,
            "vector_clock": self.vector_clock.to_dict(),
            "origin_node": self.origin_node,
            "timestamp": self.timestamp, "status": self.status.value,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(key=d["key"], value=d["value"],
                   vector_clock=VectorClock.from_dict(d.get("vector_clock", {})),
                   origin_node=d.get("origin_node", ""),
                   timestamp=d.get("timestamp", time.time()),
                   status=ReplicationStatus(d.get("status", "pending")))


class Replicator:
    """Handles async data replication across regions with eventual consistency."""

    def __init__(self, node_id: str, region_manager: RegionManager,
                 messenger: MessagePassing):
        self.node_id = node_id
        self.region_manager = region_manager
        self.messenger = messenger
        self.data_store: Dict[str, ReplicatedEntry] = {}
        self.vector_clock = VectorClock()
        self._pending_replications: List[ReplicatedEntry] = []
        self._running = False
        self._replication_task = None

    async def start(self):
        self._running = True
        self._replication_task = asyncio.create_task(self._replication_loop())
        logger.info(f"[{self.node_id}] Replicator started")

    async def stop(self):
        self._running = False
        if self._replication_task:
            self._replication_task.cancel()
            try:
                await self._replication_task
            except asyncio.CancelledError:
                pass

    async def write(self, key: str, value: Any, peers: List[dict]):
        """Write a key-value pair and schedule replication."""
        self.vector_clock.increment(self.node_id)
        entry = ReplicatedEntry(
            key=key, value=value,
            vector_clock=VectorClock.from_dict(self.vector_clock.to_dict()),
            origin_node=self.node_id)
        self.data_store[key] = entry
        # Schedule async replication
        self._pending_replications.append(entry)
        # Also immediately try to replicate
        await self._replicate_entry(entry, peers)

    async def handle_replicate(self, data: dict) -> dict:
        """Handle incoming replication from another node."""
        entry = ReplicatedEntry.from_dict(data)
        existing = self.data_store.get(entry.key)
        if existing:
            if entry.vector_clock.is_concurrent(existing.vector_clock):
                # Conflict — resolve with LWW
                if entry.timestamp > existing.timestamp:
                    self.data_store[entry.key] = entry
                    entry.status = ReplicationStatus.CONFLICT
                    logger.info(f"[{self.node_id}] Conflict resolved (LWW) for key={entry.key}")
                else:
                    entry.status = ReplicationStatus.CONFLICT
            elif entry.vector_clock.dominates(existing.vector_clock):
                self.data_store[entry.key] = entry
                entry.status = ReplicationStatus.REPLICATED
            # else: existing dominates, ignore
        else:
            self.data_store[entry.key] = entry
            entry.status = ReplicationStatus.REPLICATED

        self.vector_clock.merge(entry.vector_clock)
        return {"status": entry.status.value, "key": entry.key}

    async def _replicate_entry(self, entry: ReplicatedEntry, peers: List[dict]):
        """Replicate a single entry to all peers with simulated latency."""
        msg = Message(msg_type=MessageType.GEO_REPLICATE, sender_id=self.node_id,
                      data=entry.to_dict())
        for peer in peers:
            asyncio.create_task(self._send_with_latency(peer, msg))

    async def _send_with_latency(self, peer: dict, msg: Message):
        """Send with simulated inter-region latency."""
        await self.region_manager.simulate_latency(peer["node_id"])
        resp = await self.messenger.send(peer["host"], peer["port"], msg, retry=True)
        return resp

    async def _replication_loop(self):
        """Background loop to retry pending replications."""
        while self._running:
            try:
                # Clear completed
                self._pending_replications = [
                    e for e in self._pending_replications
                    if e.status == ReplicationStatus.PENDING
                ]
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                break

    def get_replication_status(self) -> dict:
        statuses = {"pending": 0, "replicated": 0, "conflict": 0}
        for entry in self.data_store.values():
            statuses[entry.status.value] = statuses.get(entry.status.value, 0) + 1
        return {
            "total_keys": len(self.data_store),
            "statuses": statuses,
            "vector_clock": self.vector_clock.to_dict(),
            "pending_replications": len(self._pending_replications),
        }
