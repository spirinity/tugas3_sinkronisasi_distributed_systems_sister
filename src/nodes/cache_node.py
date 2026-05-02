"""
Cache Coherence with MESI protocol, LRU replacement policy,
and performance monitoring for the distributed cache system.
"""

import asyncio
import logging
import time
from collections import OrderedDict
from enum import Enum
from typing import Any, Dict, Optional

from aiohttp import web

from src.nodes.base_node import BaseNode
from src.utils.config import NodeConfig
from src.communication.message_passing import Message, MessageType

logger = logging.getLogger(__name__)


class CacheLineState(str, Enum):
    MODIFIED = "M"
    EXCLUSIVE = "E"
    SHARED = "S"
    INVALID = "I"


class CacheLine:
    def __init__(self, key: str, value: Any = None, state: CacheLineState = CacheLineState.INVALID):
        self.key = key
        self.value = value
        self.state = state
        self.last_accessed = time.time()
        self.access_count = 0

    def to_dict(self):
        return {"key": self.key, "value": self.value, "state": self.state.value,
                "last_accessed": self.last_accessed, "access_count": self.access_count}


class LRUCache:
    """LRU cache using OrderedDict."""

    def __init__(self, max_size: int = 1000):
        self.max_size = max_size
        self._cache: OrderedDict[str, CacheLine] = OrderedDict()
        self._eviction_callbacks = []

    def on_eviction(self, callback):
        self._eviction_callbacks.append(callback)

    def get(self, key: str) -> Optional[CacheLine]:
        if key in self._cache:
            self._cache.move_to_end(key)
            line = self._cache[key]
            line.last_accessed = time.time()
            line.access_count += 1
            return line
        return None

    def put(self, key: str, line: CacheLine):
        if key in self._cache:
            self._cache.move_to_end(key)
        self._cache[key] = line
        while len(self._cache) > self.max_size:
            evicted_key, evicted_line = self._cache.popitem(last=False)
            for cb in self._eviction_callbacks:
                try:
                    cb(evicted_key, evicted_line)
                except Exception as e:
                    logger.error(f"Eviction callback error: {e}")

    def invalidate(self, key: str) -> Optional[CacheLine]:
        if key in self._cache:
            line = self._cache[key]
            line.state = CacheLineState.INVALID
            return line
        return None

    def remove(self, key: str):
        self._cache.pop(key, None)

    def items(self):
        return self._cache.items()

    def __len__(self):
        return len(self._cache)

    def __contains__(self, key):
        return key in self._cache


class CacheNode(BaseNode):
    """Distributed cache node implementing MESI coherence protocol."""

    def __init__(self, config: NodeConfig):
        super().__init__(config)
        self.cache = LRUCache(max_size=config.cache_max_size)
        self.cache.on_eviction(self._on_eviction)
        # Stats
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._invalidations = 0
        self._setup_cache_routes()

    def _setup_cache_routes(self):
        self.app.router.add_get("/cache/get/{key}", self._handle_get)
        self.app.router.add_post("/cache/put", self._handle_put)
        self.app.router.add_delete("/cache/invalidate/{key}", self._handle_invalidate)
        self.app.router.add_get("/cache/stats", self._handle_stats)
        self.app.router.add_get("/cache/state", self._handle_state)

    async def start(self):
        await super().start()
        logger.info(f"[{self.node_id}] Cache node started (MESI protocol, LRU max={self.cache.max_size})")

    async def handle_message(self, message: Message):
        if message.msg_type == MessageType.CACHE_INVALIDATE:
            return self._process_invalidate(message)
        elif message.msg_type == MessageType.CACHE_READ:
            return self._process_remote_read(message)
        elif message.msg_type == MessageType.CACHE_UPDATE:
            return self._process_update(message)
        return await super().handle_message(message)

    def _on_eviction(self, key: str, line: CacheLine):
        self._evictions += 1
        self.metrics.counter_inc("cache_evictions_total")
        if line.state == CacheLineState.MODIFIED:
            logger.info(f"[{self.node_id}] Write-back dirty cache line: {key}")

    # --- MESI Protocol ---

    async def cache_read(self, key: str) -> Optional[Any]:
        """Read a key with MESI protocol."""
        line = self.cache.get(key)
        if line and line.state != CacheLineState.INVALID:
            self._hits += 1
            self.metrics.counter_inc("cache_hits_total")
            return line.value

        self._misses += 1
        self.metrics.counter_inc("cache_misses_total")

        # Check other nodes
        read_msg = Message(msg_type=MessageType.CACHE_READ, sender_id=self.node_id,
                           data={"key": key})
        responses = await self.messenger.broadcast(self.peers, read_msg, retry=False)

        found_value = None
        other_has_copy = False
        for resp in responses.values():
            if resp and resp.get("found"):
                found_value = resp.get("value")
                other_has_copy = True

        if found_value is not None:
            # Other node(s) have it -> SHARED
            new_state = CacheLineState.SHARED
            new_line = CacheLine(key=key, value=found_value, state=new_state)
            self.cache.put(key, new_line)
            return found_value
        return None

    async def cache_write(self, key: str, value: Any):
        """Write a key with MESI protocol."""
        # Invalidate all other copies first
        inv_msg = Message(msg_type=MessageType.CACHE_INVALIDATE, sender_id=self.node_id,
                          data={"key": key})
        await self.messenger.broadcast(self.peers, inv_msg, retry=False)

        line = self.cache.get(key)
        if line:
            line.value = value
            line.state = CacheLineState.MODIFIED
        else:
            line = CacheLine(key=key, value=value, state=CacheLineState.MODIFIED)
        self.cache.put(key, line)
        self.metrics.counter_inc("cache_writes_total")

    def _process_invalidate(self, message: Message) -> dict:
        """Handle invalidation request from another node (MESI S/E -> I)."""
        key = message.data.get("key")
        line = self.cache.invalidate(key)
        self._invalidations += 1
        self.metrics.counter_inc("cache_invalidations_total")
        return {"status": "invalidated", "key": key,
                "had_copy": line is not None and line.state != CacheLineState.INVALID}

    def _process_remote_read(self, message: Message) -> dict:
        """Handle read request from another node (MESI M->S, E->S)."""
        key = message.data.get("key")
        line = self.cache.get(key)
        if line and line.state != CacheLineState.INVALID:
            # Transition: M->S or E->S
            if line.state in (CacheLineState.MODIFIED, CacheLineState.EXCLUSIVE):
                line.state = CacheLineState.SHARED
            return {"found": True, "value": line.value, "state": line.state.value}
        return {"found": False}

    def _process_update(self, message: Message) -> dict:
        key = message.data.get("key")
        value = message.data.get("value")
        line = self.cache.get(key)
        if line:
            line.value = value
            line.state = CacheLineState.SHARED
        return {"status": "updated"}

    # --- HTTP Handlers ---

    async def _handle_get(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        with self.metrics.timer("cache_read_latency"):
            value = await self.cache_read(key)
        if value is not None:
            return web.json_response({"key": key, "value": value, "status": "hit"})
        return web.json_response({"key": key, "status": "miss"}, status=404)

    async def _handle_put(self, request: web.Request) -> web.Response:
        data = await request.json()
        key = data.get("key")
        value = data.get("value")
        with self.metrics.timer("cache_write_latency"):
            await self.cache_write(key, value)
        return web.json_response({"status": "ok", "key": key})

    async def _handle_invalidate(self, request: web.Request) -> web.Response:
        key = request.match_info["key"]
        inv_msg = Message(msg_type=MessageType.CACHE_INVALIDATE, sender_id=self.node_id,
                          data={"key": key})
        await self.messenger.broadcast(self.peers, inv_msg, retry=False)
        self.cache.invalidate(key)
        return web.json_response({"status": "invalidated", "key": key})

    async def _handle_stats(self, request: web.Request) -> web.Response:
        total = self._hits + self._misses
        return web.json_response({
            "hits": self._hits, "misses": self._misses,
            "hit_rate": self._hits / total if total > 0 else 0,
            "evictions": self._evictions, "invalidations": self._invalidations,
            "cache_size": len(self.cache), "max_size": self.cache.max_size,
        })

    async def _handle_state(self, request: web.Request) -> web.Response:
        state = {k: line.to_dict() for k, line in self.cache.items()}
        return web.json_response({"cache_lines": state, "total": len(self.cache)})
