"""
Async message passing layer for inter-node communication.
Uses aiohttp for HTTP-based RPC between distributed nodes.
Supports retry logic with exponential backoff.
"""

import asyncio
import json
import logging
import time
from enum import Enum
from typing import Any, Dict, Optional

import aiohttp

logger = logging.getLogger(__name__)


class MessageType(str, Enum):
    """Types of messages exchanged between nodes."""
    # Raft consensus
    VOTE_REQUEST = "vote_request"
    VOTE_RESPONSE = "vote_response"
    APPEND_ENTRIES = "append_entries"
    APPEND_ENTRIES_RESPONSE = "append_entries_response"
    HEARTBEAT = "heartbeat"

    # Lock manager
    LOCK_REQUEST = "lock_request"
    LOCK_RELEASE = "lock_release"
    LOCK_RESPONSE = "lock_response"

    # Queue operations
    QUEUE_ENQUEUE = "queue_enqueue"
    QUEUE_DEQUEUE = "queue_dequeue"
    QUEUE_ACK = "queue_ack"
    QUEUE_RESPONSE = "queue_response"

    # Cache coherence
    CACHE_INVALIDATE = "cache_invalidate"
    CACHE_UPDATE = "cache_update"
    CACHE_READ = "cache_read"
    CACHE_READ_RESPONSE = "cache_read_response"

    # Geo replication
    GEO_REPLICATE = "geo_replicate"
    GEO_REPLICATE_ACK = "geo_replicate_ack"

    # General
    PING = "ping"
    PONG = "pong"


class Message:
    """A message to be sent between nodes."""

    def __init__(
        self,
        msg_type: MessageType,
        sender_id: str,
        data: Dict[str, Any] = None,
        term: int = 0,
        timestamp: float = None,
    ):
        self.msg_type = msg_type
        self.sender_id = sender_id
        self.data = data or {}
        self.term = term
        self.timestamp = timestamp or time.time()

    def to_dict(self) -> dict:
        return {
            "msg_type": self.msg_type.value,
            "sender_id": self.sender_id,
            "data": self.data,
            "term": self.term,
            "timestamp": self.timestamp,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "Message":
        return cls(
            msg_type=MessageType(d["msg_type"]),
            sender_id=d["sender_id"],
            data=d.get("data", {}),
            term=d.get("term", 0),
            timestamp=d.get("timestamp", time.time()),
        )


class MessagePassing:
    """
    Handles async message passing between nodes via HTTP.
    Provides send with retry + exponential backoff.
    """

    def __init__(self, node_id: str, timeout: float = 5.0, max_retries: int = 3):
        self.node_id = node_id
        self.timeout = timeout
        self.max_retries = max_retries
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self):
        """Initialize the HTTP client session."""
        connector = aiohttp.TCPConnector(limit=100, limit_per_host=10)
        self._session = aiohttp.ClientSession(
            connector=connector,
            timeout=aiohttp.ClientTimeout(total=self.timeout),
        )

    async def stop(self):
        """Close the HTTP client session."""
        if self._session:
            await self._session.close()
            self._session = None

    async def send(
        self,
        target_host: str,
        target_port: int,
        message: Message,
        retry: bool = True,
    ) -> Optional[dict]:
        """
        Send a message to a target node.

        Args:
            target_host: Target node hostname
            target_port: Target node port
            message: Message to send
            retry: Whether to retry on failure

        Returns:
            Response dict or None on failure
        """
        url = f"http://{target_host}:{target_port}/rpc"
        payload = message.to_dict()
        max_attempts = self.max_retries if retry else 1

        for attempt in range(max_attempts):
            try:
                async with self._session.post(url, json=payload) as resp:
                    if resp.status == 200:
                        return await resp.json()
                    else:
                        logger.warning(
                            f"[{self.node_id}] Send to {target_host}:{target_port} "
                            f"returned status {resp.status}"
                        )
            except asyncio.TimeoutError:
                logger.debug(
                    f"[{self.node_id}] Timeout sending to {target_host}:{target_port} "
                    f"(attempt {attempt + 1}/{max_attempts})"
                )
            except aiohttp.ClientError as e:
                logger.debug(
                    f"[{self.node_id}] Error sending to {target_host}:{target_port}: {e} "
                    f"(attempt {attempt + 1}/{max_attempts})"
                )
            except Exception as e:
                logger.error(f"[{self.node_id}] Unexpected error: {e}")
                break

            if attempt < max_attempts - 1:
                # Exponential backoff: 0.1s, 0.2s, 0.4s, ...
                delay = 0.1 * (2 ** attempt)
                await asyncio.sleep(delay)

        return None

    async def broadcast(
        self,
        peers: list,
        message: Message,
        retry: bool = False,
    ) -> Dict[str, Optional[dict]]:
        """
        Broadcast a message to all peer nodes.

        Args:
            peers: List of {"host": str, "port": int, "node_id": str}
            message: Message to broadcast
            retry: Whether to retry failed sends

        Returns:
            Dict mapping node_id to response (or None)
        """
        tasks = {}
        for peer in peers:
            task = asyncio.create_task(
                self.send(peer["host"], peer["port"], message, retry=retry)
            )
            tasks[peer["node_id"]] = task

        results = {}
        for node_id, task in tasks.items():
            try:
                results[node_id] = await task
            except Exception as e:
                logger.error(f"[{self.node_id}] Broadcast to {node_id} failed: {e}")
                results[node_id] = None

        return results
