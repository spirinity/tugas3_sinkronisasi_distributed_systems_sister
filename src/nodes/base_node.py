"""
Base node class for all distributed system nodes.
Provides HTTP server, lifecycle management, and health check endpoints.
"""

import asyncio
import logging
import json
from typing import Dict, Optional

from aiohttp import web

from src.utils.config import NodeConfig
from src.utils.metrics import MetricsCollector
from src.communication.message_passing import MessagePassing, Message, MessageType
from src.communication.failure_detector import FailureDetector

logger = logging.getLogger(__name__)


class BaseNode:
    """
    Abstract base class for distributed system nodes.
    Handles HTTP server setup, RPC handling, heartbeats, and lifecycle management.
    """

    def __init__(self, config: NodeConfig):
        self.config = config
        self.node_id = config.node_id
        self.host = config.node_host
        self.port = config.node_port

        # Core components
        self.messenger = MessagePassing(self.node_id)
        self.failure_detector = FailureDetector(
            self.node_id,
            heartbeat_interval=1.0,
            suspect_timeout=5.0,
            dead_timeout=15.0,
        )
        self.metrics = MetricsCollector(node_id=self.node_id)

        # HTTP server
        self.app = web.Application()
        self._setup_routes()
        self.runner: Optional[web.AppRunner] = None

        # Peers
        self.peers = config.get_peer_nodes()

        # State
        self._running = False
        self._heartbeat_task: Optional[asyncio.Task] = None

    def _setup_routes(self):
        """Setup base HTTP routes. Subclasses should call super and add their own."""
        self.app.router.add_post("/rpc", self._handle_rpc)
        self.app.router.add_get("/health", self._handle_health)
        self.app.router.add_get("/metrics", self._handle_metrics)
        self.app.router.add_get("/status", self._handle_status)

    async def start(self):
        """Start the node: HTTP server, messenger, failure detector, heartbeats."""
        logger.info(f"[{self.node_id}] Starting node on {self.host}:{self.port}")

        # Start messenger
        await self.messenger.start()

        # Register peers in failure detector
        for peer in self.peers:
            self.failure_detector.register_peer(peer["node_id"])
        await self.failure_detector.start()

        # Start HTTP server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.host, self.port)
        await site.start()

        self._running = True

        # Start heartbeat broadcast
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

        # Update metrics
        self.metrics.gauge_set("node_up", 1)
        self.metrics.counter_inc("node_starts_total")

        logger.info(f"[{self.node_id}] Node started successfully")

    async def stop(self):
        """Stop the node gracefully."""
        logger.info(f"[{self.node_id}] Stopping node...")
        self._running = False

        if self._heartbeat_task:
            self._heartbeat_task.cancel()
            try:
                await self._heartbeat_task
            except asyncio.CancelledError:
                pass

        await self.failure_detector.stop()
        await self.messenger.stop()

        if self.runner:
            await self.runner.cleanup()

        self.metrics.gauge_set("node_up", 0)
        logger.info(f"[{self.node_id}] Node stopped")

    async def _heartbeat_loop(self):
        """Periodically send heartbeats to all peers."""
        while self._running:
            try:
                heartbeat = Message(
                    msg_type=MessageType.HEARTBEAT,
                    sender_id=self.node_id,
                    data={"status": "alive"},
                )
                for peer in self.peers:
                    try:
                        resp = await self.messenger.send(
                            peer["host"], peer["port"], heartbeat, retry=False
                        )
                        if resp:
                            self.failure_detector.record_heartbeat(peer["node_id"])
                    except Exception:
                        pass

                await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Heartbeat error: {e}")
                await asyncio.sleep(2.0)

    async def _handle_rpc(self, request: web.Request) -> web.Response:
        """Handle incoming RPC messages from other nodes."""
        try:
            data = await request.json()
            message = Message.from_dict(data)

            # Record heartbeat from sender
            self.failure_detector.record_heartbeat(message.sender_id)
            self.metrics.counter_inc("rpc_received_total", labels={"type": message.msg_type.value})

            # Dispatch to handler
            response = await self.handle_message(message)
            return web.json_response(response or {"status": "ok"})
        except Exception as e:
            logger.error(f"[{self.node_id}] RPC error: {e}")
            return web.json_response({"error": str(e)}, status=500)

    async def handle_message(self, message: Message) -> Optional[dict]:
        """
        Handle an incoming message. Override in subclasses.
        Returns a response dict or None.
        """
        if message.msg_type == MessageType.PING:
            return {"msg_type": "pong", "sender_id": self.node_id}
        elif message.msg_type == MessageType.HEARTBEAT:
            return {"status": "alive", "node_id": self.node_id}
        return {"status": "unhandled", "msg_type": message.msg_type.value}

    async def _handle_health(self, request: web.Request) -> web.Response:
        """Health check endpoint."""
        return web.json_response({
            "node_id": self.node_id,
            "status": "healthy" if self._running else "unhealthy",
            "region": self.config.node_region,
            "peers": self.failure_detector.get_all_statuses(),
        })

    async def _handle_metrics(self, request: web.Request) -> web.Response:
        """Prometheus metrics endpoint."""
        output = self.metrics.get_prometheus_output()
        return web.Response(text=output, content_type="text/plain")

    async def _handle_status(self, request: web.Request) -> web.Response:
        """General status endpoint."""
        return web.json_response({
            "node_id": self.node_id,
            "running": self._running,
            "region": self.config.node_region,
            "peers": self.failure_detector.get_all_statuses(),
            "metrics": self.metrics.get_summary(),
        })
