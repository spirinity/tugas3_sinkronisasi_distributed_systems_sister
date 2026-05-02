"""
Distributed Queue with consistent hashing, message persistence via Redis,
at-least-once delivery, and node failure handling.
"""

import asyncio
import hashlib
import json
import logging
import time
import uuid
from enum import Enum
from typing import Dict, List, Optional, Set

import redis.asyncio as aioredis
from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig
from src.communication.message_passing import Message, MessageType

logger = logging.getLogger(__name__)


class MessageState(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    DEAD_LETTER = "dead_letter"


class QueueMessage:
    def __init__(self, msg_id=None, payload=None, partition_key=None,
                 state=MessageState.PENDING, retries=0, max_retries=3,
                 created_at=None, assigned_node=None):
        self.msg_id = msg_id or str(uuid.uuid4())
        self.payload = payload or {}
        self.partition_key = partition_key or self.msg_id
        self.state = state
        self.retries = retries
        self.max_retries = max_retries
        self.created_at = created_at or time.time()
        self.assigned_node = assigned_node
        self.processing_started = None

    def to_dict(self):
        return {
            "msg_id": self.msg_id, "payload": self.payload,
            "partition_key": self.partition_key, "state": self.state.value,
            "retries": self.retries, "max_retries": self.max_retries,
            "created_at": self.created_at, "assigned_node": self.assigned_node,
            "processing_started": self.processing_started,
        }

    @classmethod
    def from_dict(cls, d):
        m = cls(msg_id=d["msg_id"], payload=d.get("payload", {}),
                partition_key=d.get("partition_key", d["msg_id"]),
                state=MessageState(d.get("state", "pending")),
                retries=d.get("retries", 0), max_retries=d.get("max_retries", 3),
                created_at=d.get("created_at", time.time()),
                assigned_node=d.get("assigned_node"))
        m.processing_started = d.get("processing_started")
        return m


class ConsistentHashRing:
    """Consistent hashing ring with virtual nodes for even distribution."""

    def __init__(self, replicas=150):
        self.replicas = replicas
        self._ring: Dict[int, str] = {}
        self._sorted_keys: List[int] = []
        self.nodes: Set[str] = set()

    def _hash(self, key: str) -> int:
        return int(hashlib.md5(key.encode()).hexdigest(), 16)

    def add_node(self, node_id: str):
        self.nodes.add(node_id)
        for i in range(self.replicas):
            h = self._hash(f"{node_id}:{i}")
            self._ring[h] = node_id
        self._sorted_keys = sorted(self._ring.keys())

    def remove_node(self, node_id: str):
        self.nodes.discard(node_id)
        for i in range(self.replicas):
            h = self._hash(f"{node_id}:{i}")
            self._ring.pop(h, None)
        self._sorted_keys = sorted(self._ring.keys())

    def get_node(self, key: str) -> Optional[str]:
        if not self._ring:
            return None
        h = self._hash(key)
        for k in self._sorted_keys:
            if k >= h:
                return self._ring[k]
        return self._ring[self._sorted_keys[0]]

    def get_nodes_for_key(self, key: str, count: int = 2) -> List[str]:
        """Get multiple nodes for replication."""
        if not self._ring:
            return []
        result = []
        h = self._hash(key)
        idx = 0
        for i, k in enumerate(self._sorted_keys):
            if k >= h:
                idx = i
                break
        seen = set()
        for i in range(len(self._sorted_keys)):
            node = self._ring[self._sorted_keys[(idx + i) % len(self._sorted_keys)]]
            if node not in seen:
                seen.add(node)
                result.append(node)
            if len(result) >= count:
                break
        return result

    def get_ring_info(self):
        return {"nodes": list(self.nodes), "total_vnodes": len(self._ring)}


class QueueNode(BaseNode):
    """Distributed Queue node with consistent hashing and Redis persistence."""

    def __init__(self, config: NodeConfig):
        super().__init__(config)
        self.hash_ring = ConsistentHashRing()
        self.messages: Dict[str, QueueMessage] = {}
        self.pending_queue: List[str] = []  # msg_ids in order
        self.redis_client = None
        self.replication_factor = config.queue_replication_factor
        self.processing_timeout = 30.0
        self._setup_queue_routes()

    def _setup_queue_routes(self):
        self.app.router.add_post("/queue/enqueue", self._handle_enqueue)
        self.app.router.add_post("/queue/dequeue", self._handle_dequeue)
        self.app.router.add_post("/queue/ack/{msg_id}", self._handle_ack)
        self.app.router.add_get("/queue/status", self._handle_queue_status)
        self.app.router.add_get("/queue/ring", self._handle_ring_status)

    async def start(self):
        await super().start()
        # Connect Redis
        self.redis_client = aioredis.Redis(
            host=self.config.redis_host, port=self.config.redis_port, decode_responses=True)
        try:
            await self.redis_client.ping()
            logger.info(f"[{self.node_id}] Connected to Redis")
        except Exception as e:
            logger.warning(f"[{self.node_id}] Redis not available: {e}")
            self.redis_client = None
        # Add self and peers to hash ring
        self.hash_ring.add_node(self.node_id)
        for peer in self.peers:
            self.hash_ring.add_node(peer["node_id"])
        # Recover messages from Redis
        await self._recover_from_redis()
        # Start timeout checker
        asyncio.create_task(self._processing_timeout_loop())
        logger.info(f"[{self.node_id}] Queue node started")

    async def stop(self):
        if self.redis_client:
            await self.redis_client.close()
        await super().stop()

    async def handle_message(self, message: Message):
        if message.msg_type == MessageType.QUEUE_ENQUEUE:
            return await self._replicate_enqueue(message)
        return await super().handle_message(message)

    async def _replicate_enqueue(self, message: Message):
        """Handle replicated enqueue from another node."""
        data = message.data
        msg = QueueMessage.from_dict(data)
        self.messages[msg.msg_id] = msg
        if msg.state == MessageState.PENDING:
            self.pending_queue.append(msg.msg_id)
        await self._persist_message(msg)
        return {"status": "replicated", "msg_id": msg.msg_id}

    async def _persist_message(self, msg: QueueMessage):
        if self.redis_client:
            try:
                key = f"queue:{self.node_id}:msg:{msg.msg_id}"
                await self.redis_client.set(key, json.dumps(msg.to_dict()), ex=3600)
            except Exception as e:
                logger.error(f"Redis persist error: {e}")

    async def _recover_from_redis(self):
        if not self.redis_client:
            return
        try:
            cursor = 0
            pattern = f"queue:{self.node_id}:msg:*"
            while True:
                cursor, keys = await self.redis_client.scan(cursor, match=pattern, count=100)
                for key in keys:
                    data = await self.redis_client.get(key)
                    if data:
                        msg = QueueMessage.from_dict(json.loads(data))
                        self.messages[msg.msg_id] = msg
                        if msg.state == MessageState.PENDING:
                            self.pending_queue.append(msg.msg_id)
                if cursor == 0:
                    break
            logger.info(f"[{self.node_id}] Recovered {len(self.messages)} messages from Redis")
        except Exception as e:
            logger.error(f"Recovery error: {e}")

    async def _processing_timeout_loop(self):
        while self._running:
            try:
                now = time.time()
                for msg in list(self.messages.values()):
                    if (msg.state == MessageState.PROCESSING and msg.processing_started
                            and now - msg.processing_started > self.processing_timeout):
                        msg.retries += 1
                        if msg.retries >= msg.max_retries:
                            msg.state = MessageState.DEAD_LETTER
                            self.metrics.counter_inc("queue_dead_letter_total")
                        else:
                            msg.state = MessageState.PENDING
                            msg.processing_started = None
                            self.pending_queue.append(msg.msg_id)
                            self.metrics.counter_inc("queue_retry_total")
                        await self._persist_message(msg)
                await asyncio.sleep(5.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"Timeout loop error: {e}")
                await asyncio.sleep(5)

    # --- HTTP Handlers ---

    async def _handle_enqueue(self, request: web.Request) -> web.Response:
        data = await request.json()
        msg = QueueMessage(
            payload=data.get("payload", {}),
            partition_key=data.get("partition_key", str(uuid.uuid4())),
        )
        target_node = self.hash_ring.get_node(msg.partition_key)
        msg.assigned_node = target_node
        self.messages[msg.msg_id] = msg
        self.pending_queue.append(msg.msg_id)
        await self._persist_message(msg)
        self.metrics.counter_inc("queue_enqueued_total")
        self.metrics.gauge_set("queue_depth", len(self.pending_queue))
        # Replicate
        rep_msg = Message(msg_type=MessageType.QUEUE_ENQUEUE, sender_id=self.node_id,
                          data=msg.to_dict())
        for peer in self.peers[:self.replication_factor - 1]:
            asyncio.create_task(
                self.messenger.send(peer["host"], peer["port"], rep_msg, retry=True))
        return web.json_response({"status": "enqueued", "msg_id": msg.msg_id,
                                   "assigned_node": target_node})

    async def _handle_dequeue(self, request: web.Request) -> web.Response:
        if not self.pending_queue:
            return web.json_response({"status": "empty"}, status=204)
        msg_id = self.pending_queue.pop(0)
        msg = self.messages.get(msg_id)
        if not msg:
            return web.json_response({"status": "empty"}, status=204)
        msg.state = MessageState.PROCESSING
        msg.processing_started = time.time()
        await self._persist_message(msg)
        self.metrics.counter_inc("queue_dequeued_total")
        self.metrics.gauge_set("queue_depth", len(self.pending_queue))
        return web.json_response({"status": "ok", "message": msg.to_dict()})

    async def _handle_ack(self, request: web.Request) -> web.Response:
        msg_id = request.match_info["msg_id"]
        msg = self.messages.get(msg_id)
        if not msg:
            return web.json_response({"error": "not_found"}, status=404)
        msg.state = MessageState.COMPLETED
        await self._persist_message(msg)
        self.metrics.counter_inc("queue_ack_total")
        return web.json_response({"status": "acknowledged"})

    async def _handle_queue_status(self, request: web.Request) -> web.Response:
        stats = {"pending": 0, "processing": 0, "completed": 0, "failed": 0, "dead_letter": 0}
        for msg in self.messages.values():
            stats[msg.state.value] = stats.get(msg.state.value, 0) + 1
        return web.json_response({"total": len(self.messages), "stats": stats,
                                   "pending_queue_size": len(self.pending_queue)})

    async def _handle_ring_status(self, request: web.Request) -> web.Response:
        return web.json_response(self.hash_ring.get_ring_info())
