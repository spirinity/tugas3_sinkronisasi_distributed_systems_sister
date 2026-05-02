"""
Heartbeat-based failure detection for distributed nodes.
Tracks node status (ALIVE, SUSPECTED, DEAD) and triggers callbacks on state changes.
"""

import asyncio
import logging
import time
from enum import Enum
from typing import Callable, Dict, List, Optional

logger = logging.getLogger(__name__)


class NodeStatus(str, Enum):
    ALIVE = "alive"
    SUSPECTED = "suspected"
    DEAD = "dead"
    UNKNOWN = "unknown"


class FailureDetector:
    """
    Heartbeat-based failure detector.
    Monitors peer nodes and detects failures based on missed heartbeats.
    """

    def __init__(
        self,
        node_id: str,
        heartbeat_interval: float = 1.0,
        suspect_timeout: float = 3.0,
        dead_timeout: float = 10.0,
    ):
        self.node_id = node_id
        self.heartbeat_interval = heartbeat_interval
        self.suspect_timeout = suspect_timeout
        self.dead_timeout = dead_timeout

        # Track last heartbeat time for each peer
        self._last_heartbeat: Dict[str, float] = {}
        # Current status of each peer
        self._node_status: Dict[str, NodeStatus] = {}
        # Callbacks for status changes
        self._on_status_change: List[Callable] = []
        # Running flag
        self._running = False
        self._monitor_task: Optional[asyncio.Task] = None

    def register_peer(self, node_id: str):
        """Register a peer node to monitor."""
        self._last_heartbeat[node_id] = time.time()
        self._node_status[node_id] = NodeStatus.ALIVE

    def record_heartbeat(self, node_id: str):
        """Record that a heartbeat was received from a peer."""
        self._last_heartbeat[node_id] = time.time()
        old_status = self._node_status.get(node_id, NodeStatus.UNKNOWN)
        if old_status != NodeStatus.ALIVE:
            self._node_status[node_id] = NodeStatus.ALIVE
            self._notify_status_change(node_id, old_status, NodeStatus.ALIVE)
            logger.info(f"[{self.node_id}] Node {node_id} recovered: {old_status} -> ALIVE")

    def on_status_change(self, callback: Callable):
        """Register a callback for node status changes.
        Callback signature: callback(node_id: str, old_status: NodeStatus, new_status: NodeStatus)
        """
        self._on_status_change.append(callback)

    def get_status(self, node_id: str) -> NodeStatus:
        """Get the current status of a peer node."""
        return self._node_status.get(node_id, NodeStatus.UNKNOWN)

    def get_alive_nodes(self) -> List[str]:
        """Get list of nodes currently considered alive."""
        return [nid for nid, status in self._node_status.items() if status == NodeStatus.ALIVE]

    def get_all_statuses(self) -> Dict[str, str]:
        """Get status of all monitored nodes."""
        return {nid: status.value for nid, status in self._node_status.items()}

    async def start(self):
        """Start the failure detection monitor loop."""
        self._running = True
        self._monitor_task = asyncio.create_task(self._monitor_loop())
        logger.info(f"[{self.node_id}] Failure detector started")

    async def stop(self):
        """Stop the failure detection monitor loop."""
        self._running = False
        if self._monitor_task:
            self._monitor_task.cancel()
            try:
                await self._monitor_task
            except asyncio.CancelledError:
                pass
        logger.info(f"[{self.node_id}] Failure detector stopped")

    async def _monitor_loop(self):
        """Main monitoring loop — checks heartbeat timestamps periodically."""
        while self._running:
            try:
                now = time.time()
                for node_id, last_hb in self._last_heartbeat.items():
                    elapsed = now - last_hb
                    old_status = self._node_status.get(node_id, NodeStatus.UNKNOWN)

                    if elapsed > self.dead_timeout:
                        new_status = NodeStatus.DEAD
                    elif elapsed > self.suspect_timeout:
                        new_status = NodeStatus.SUSPECTED
                    else:
                        new_status = NodeStatus.ALIVE

                    if new_status != old_status:
                        self._node_status[node_id] = new_status
                        self._notify_status_change(node_id, old_status, new_status)
                        logger.info(
                            f"[{self.node_id}] Node {node_id}: {old_status} -> {new_status}"
                        )

                await asyncio.sleep(self.heartbeat_interval)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Failure detector error: {e}")
                await asyncio.sleep(1)

    def _notify_status_change(self, node_id: str, old_status: NodeStatus, new_status: NodeStatus):
        """Notify all registered callbacks about a status change."""
        for callback in self._on_status_change:
            try:
                callback(node_id, old_status, new_status)
            except Exception as e:
                logger.error(f"[{self.node_id}] Status change callback error: {e}")
