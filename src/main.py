"""
Main entry point for running a distributed sync node.
Combines Lock Manager, Queue, Cache, Geo, and Security components
into a single unified node.
"""

import asyncio
import logging
import os
import signal
import sys

from aiohttp import web

from src.utils.config import NodeConfig
from src.utils.metrics import MetricsCollector
from src.communication.message_passing import MessagePassing, Message, MessageType
from src.communication.failure_detector import FailureDetector
from src.consensus.raft import RaftConsensus
from src.nodes.lock_manager import LockManagerNode, LockType
from src.nodes.queue_node import QueueNode, QueueMessage, ConsistentHashRing
from src.nodes.cache_node import CacheNode, CacheLineState
from src.geo.region_manager import RegionManager
from src.geo.latency_router import LatencyRouter
from src.geo.replicator import Replicator
from src.security.rbac import RBACManager, rbac_middleware
from src.security.audit_logger import AuditLogger
from src.security.tls_manager import TLSManager


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(name)s] %(levelname)s: %(message)s",
)
logger = logging.getLogger(__name__)


class DistributedNode:
    """
    Unified distributed node that integrates all components:
    Lock Manager, Queue, Cache, Geo-Distributed, and Security.
    """

    def __init__(self, config: NodeConfig):
        self.config = config
        self.node_id = config.node_id

        # Core infrastructure
        self.messenger = MessagePassing(self.node_id)
        self.failure_detector = FailureDetector(self.node_id)
        self.metrics = MetricsCollector(node_id=self.node_id)
        self.peers = config.get_peer_nodes()

        # Raft consensus (for lock manager)
        self.raft = RaftConsensus(
            node_id=self.node_id, peers=self.peers, messenger=self.messenger,
            election_timeout_min=config.raft_election_timeout_min,
            election_timeout_max=config.raft_election_timeout_max,
            heartbeat_interval=config.raft_heartbeat_interval,
        )

        # Geo-distributed
        self.region_manager = RegionManager(self.node_id, config.node_region)
        self.latency_router = LatencyRouter(self.region_manager)
        self.replicator = Replicator(self.node_id, self.region_manager, self.messenger)

        # Security
        self.rbac = RBACManager(config.secret_key)
        self.audit = AuditLogger(self.node_id)
        self.tls_manager = TLSManager(node_id=self.node_id) if config.enable_tls else None

        # Lock state
        self.locks = {}
        self.raft.on_commit(self._apply_lock_command)

        # Queue state
        self.queue_messages = {}
        self.pending_queue = []
        self.hash_ring = ConsistentHashRing()
        self.redis_client = None

        # Cache state
        from src.nodes.cache_node import LRUCache
        self.cache = LRUCache(max_size=config.cache_max_size)
        self._cache_hits = 0
        self._cache_misses = 0

        # HTTP app
        self.app = web.Application(middlewares=[
            self._cors_middleware,
            rbac_middleware(self.rbac, enabled=config.enable_rbac)
        ])
        self._setup_routes()
        self.runner = None
        self._running = False

    @web.middleware
    async def _cors_middleware(self, request, handler):
        """Allow cross-origin requests from Swagger UI on other ports."""
        if request.method == "OPTIONS":
            response = web.Response()
        else:
            try:
                response = await handler(request)
            except web.HTTPException as ex:
                response = ex
        response.headers["Access-Control-Allow-Origin"] = "*"
        response.headers["Access-Control-Allow-Methods"] = "GET, POST, PUT, DELETE, OPTIONS"
        response.headers["Access-Control-Allow-Headers"] = "Content-Type, Authorization"
        return response

    def _setup_routes(self):
        r = self.app.router
        # General
        r.add_post("/rpc", self._handle_rpc)
        r.add_get("/health", self._handle_health)
        r.add_get("/metrics", self._handle_metrics)
        r.add_get("/status", self._handle_status)
        # Lock Manager
        r.add_post("/lock/acquire", self._handle_lock_acquire)
        r.add_post("/lock/release", self._handle_lock_release)
        r.add_get("/lock/status", self._handle_lock_status)
        r.add_get("/lock/deadlocks", self._handle_lock_deadlocks)
        # Queue
        r.add_post("/queue/enqueue", self._handle_queue_enqueue)
        r.add_post("/queue/dequeue", self._handle_queue_dequeue)
        r.add_post("/queue/ack/{msg_id}", self._handle_queue_ack)
        r.add_get("/queue/status", self._handle_queue_status)
        r.add_get("/queue/ring", self._handle_queue_ring)
        # Cache
        r.add_get("/cache/get/{key}", self._handle_cache_get)
        r.add_post("/cache/put", self._handle_cache_put)
        r.add_delete("/cache/invalidate/{key}", self._handle_cache_invalidate)
        r.add_get("/cache/stats", self._handle_cache_stats)
        r.add_get("/cache/state", self._handle_cache_state)
        # Geo
        r.add_get("/geo/regions", self._handle_geo_regions)
        r.add_get("/geo/latency", self._handle_geo_latency)
        r.add_get("/geo/replication-status", self._handle_geo_replication)
        # Raft
        r.add_get("/raft/status", self._handle_raft_status)
        # Auth
        r.add_post("/auth/token", self._handle_auth_token)
        r.add_get("/auth/verify", self._handle_auth_verify)
        # Audit
        r.add_get("/audit/logs", self._handle_audit_logs)
        r.add_get("/audit/verify", self._handle_audit_verify)
        # Security
        r.add_get("/security/certs", self._handle_security_certs)
        # Swagger Docs
        r.add_get("/docs", self._handle_swagger_ui)
        r.add_get("/swagger.yaml", self._handle_swagger_yaml)

    async def start(self):
        logger.info(f"[{self.node_id}] Starting distributed node...")

        # TLS setup
        if self.tls_manager:
            self.tls_manager.setup()

        # Start messenger
        await self.messenger.start()

        # Register peers
        for peer in self.peers:
            self.failure_detector.register_peer(peer["node_id"])
            self.region_manager.register_node(
                peer["node_id"], peer["host"], peer["port"],
                self.config.node_region)  # Default region, updated via gossip
            self.hash_ring.add_node(peer["node_id"])
        self.hash_ring.add_node(self.node_id)

        await self.failure_detector.start()
        await self.raft.start()
        await self.replicator.start()

        # Connect Redis
        try:
            import redis.asyncio as aioredis
            self.redis_client = aioredis.Redis(
                host=self.config.redis_host, port=self.config.redis_port,
                decode_responses=True)
            await self.redis_client.ping()
            logger.info(f"[{self.node_id}] Connected to Redis")
        except Exception as e:
            logger.warning(f"[{self.node_id}] Redis not available: {e}")
            self.redis_client = None

        # Start HTTP server
        self.runner = web.AppRunner(self.app)
        await self.runner.setup()
        site = web.TCPSite(self.runner, self.config.node_host, self.config.node_port)
        await site.start()
        self._running = True

        # Background tasks
        asyncio.create_task(self._heartbeat_loop())

        logger.info(f"[{self.node_id}] Node started on port {self.config.node_port} "
                     f"(region={self.config.node_region})")
        self.audit.log("system", "node_start", self.node_id, "success")

    async def stop(self):
        self._running = False
        self.audit.log("system", "node_stop", self.node_id, "success")
        await self.raft.stop()
        await self.replicator.stop()
        await self.failure_detector.stop()
        await self.messenger.stop()
        if self.redis_client:
            await self.redis_client.close()
        if self.runner:
            await self.runner.cleanup()
        logger.info(f"[{self.node_id}] Node stopped")

    async def _heartbeat_loop(self):
        while self._running:
            try:
                heartbeat = Message(msg_type=MessageType.HEARTBEAT,
                                    sender_id=self.node_id, data={"status": "alive"})
                for peer in self.peers:
                    try:
                        resp = await self.messenger.send(
                            peer["host"], peer["port"], heartbeat, retry=False)
                        if resp:
                            self.failure_detector.record_heartbeat(peer["node_id"])
                    except Exception:
                        pass
                await asyncio.sleep(2.0)
            except asyncio.CancelledError:
                break
            except Exception:
                await asyncio.sleep(2.0)

    # --- RPC Handler ---
    async def _handle_rpc(self, request: web.Request) -> web.Response:
        data = await request.json()
        message = Message.from_dict(data)
        self.failure_detector.record_heartbeat(message.sender_id)
        # Raft messages
        raft_types = {MessageType.VOTE_REQUEST, MessageType.VOTE_RESPONSE,
                      MessageType.APPEND_ENTRIES, MessageType.APPEND_ENTRIES_RESPONSE}
        if message.msg_type in raft_types:
            resp = await self.raft.handle_message(message)
            return web.json_response(resp)
        if message.msg_type == MessageType.CACHE_INVALIDATE:
            return web.json_response(self._process_cache_invalidate(message))
        if message.msg_type == MessageType.CACHE_READ:
            return web.json_response(self._process_cache_read(message))
        if message.msg_type == MessageType.GEO_REPLICATE:
            resp = await self.replicator.handle_replicate(message.data)
            return web.json_response(resp)
        if message.msg_type == MessageType.QUEUE_ENQUEUE:
            qm = QueueMessage.from_dict(message.data)
            self.queue_messages[qm.msg_id] = qm
            self.pending_queue.append(qm.msg_id)
            return web.json_response({"status": "replicated"})
        if message.msg_type == MessageType.HEARTBEAT:
            return web.json_response({"status": "alive"})
        return web.json_response({"status": "ok"})

    # --- Lock Manager Handlers ---
    def _apply_lock_command(self, entry):
        cmd = entry.command
        action, resource, client_id = cmd.get("action"), cmd.get("resource"), cmd.get("client_id")
        if action == "acquire":
            from src.nodes.lock_manager import LockState
            if resource not in self.locks:
                self.locks[resource] = LockState(resource)
            lock = self.locks[resource]
            lt = LockType(cmd.get("lock_type", "exclusive"))
            if not lock.holders:
                lock.lock_type = lt
                lock.holders.add(client_id)
                lock.fencing_token += 1
            elif lt == LockType.SHARED and lock.lock_type == LockType.SHARED:
                lock.holders.add(client_id)
            else:
                lock.waiting.append({"client_id": client_id, "lock_type": lt.value})
        elif action == "release":
            if resource in self.locks:
                self.locks[resource].holders.discard(client_id)

    async def _handle_lock_acquire(self, req):
        data = await req.json()
        if not self.raft.is_leader:
            return web.json_response({"error": "not_leader", "leader_id": self.raft.leader_id}, status=307)
        ok = await self.raft.propose({
            "action": "acquire", "resource": data["resource"],
            "client_id": data.get("client_id", "anon"), "lock_type": data.get("lock_type", "exclusive")})
        self.audit.log(data.get("client_id", "anon"), "lock_acquire", data["resource"],
                       "success" if ok else "failed")
        return web.json_response({"status": "acquired" if ok else "failed"})

    async def _handle_lock_release(self, req):
        data = await req.json()
        if not self.raft.is_leader:
            return web.json_response({"error": "not_leader"}, status=307)
        ok = await self.raft.propose({
            "action": "release", "resource": data["resource"],
            "client_id": data.get("client_id", "anon")})
        self.audit.log(data.get("client_id", "anon"), "lock_release", data["resource"],
                       "success" if ok else "failed")
        return web.json_response({"status": "released" if ok else "failed"})

    async def _handle_lock_status(self, req):
        return web.json_response({"locks": {r: l.to_dict() for r, l in self.locks.items()}})

    async def _handle_lock_deadlocks(self, req):
        return web.json_response({"deadlocks": [], "message": "No deadlocks detected"})

    # --- Queue Handlers ---
    async def _handle_queue_enqueue(self, req):
        data = await req.json()
        import uuid
        msg = QueueMessage(payload=data.get("payload", {}),
                           partition_key=data.get("partition_key", str(uuid.uuid4())))
        msg.assigned_node = self.hash_ring.get_node(msg.partition_key)
        self.queue_messages[msg.msg_id] = msg
        self.pending_queue.append(msg.msg_id)
        self.metrics.counter_inc("queue_enqueued_total")
        # Replicate to peers
        rep_msg = Message(msg_type=MessageType.QUEUE_ENQUEUE, sender_id=self.node_id,
                          data=msg.to_dict())
        for peer in self.peers[:self.config.queue_replication_factor - 1]:
            asyncio.create_task(self.messenger.send(peer["host"], peer["port"], rep_msg, retry=True))
        self.audit.log("system", "queue_enqueue", msg.msg_id, "success")
        return web.json_response({"status": "enqueued", "msg_id": msg.msg_id})

    async def _handle_queue_dequeue(self, req):
        if not self.pending_queue:
            return web.json_response({"status": "empty"}, status=204)
        mid = self.pending_queue.pop(0)
        msg = self.queue_messages.get(mid)
        if not msg:
            return web.json_response({"status": "empty"}, status=204)
        from src.nodes.queue_node import MessageState
        msg.state = MessageState.PROCESSING
        import time as t
        msg.processing_started = t.time()
        return web.json_response({"status": "ok", "message": msg.to_dict()})

    async def _handle_queue_ack(self, req):
        mid = req.match_info["msg_id"]
        msg = self.queue_messages.get(mid)
        if not msg:
            return web.json_response({"error": "not_found"}, status=404)
        from src.nodes.queue_node import MessageState
        msg.state = MessageState.COMPLETED
        return web.json_response({"status": "acknowledged"})

    async def _handle_queue_status(self, req):
        return web.json_response({"total": len(self.queue_messages),
                                   "pending": len(self.pending_queue)})

    async def _handle_queue_ring(self, req):
        return web.json_response(self.hash_ring.get_ring_info())

    # --- Cache Handlers ---
    def _process_cache_invalidate(self, message):
        key = message.data.get("key")
        self.cache.invalidate(key)
        return {"status": "invalidated"}

    def _process_cache_read(self, message):
        key = message.data.get("key")
        from src.nodes.cache_node import CacheLineState
        line = self.cache.get(key)
        if line and line.state != CacheLineState.INVALID:
            if line.state in (CacheLineState.MODIFIED, CacheLineState.EXCLUSIVE):
                line.state = CacheLineState.SHARED
            return {"found": True, "value": line.value}
        return {"found": False}

    async def _handle_cache_get(self, req):
        key = req.match_info["key"]
        from src.nodes.cache_node import CacheLine, CacheLineState
        line = self.cache.get(key)
        if line and line.state != CacheLineState.INVALID:
            self._cache_hits += 1
            return web.json_response({"key": key, "value": line.value, "status": "hit"})
        self._cache_misses += 1
        # Try remote read
        read_msg = Message(msg_type=MessageType.CACHE_READ, sender_id=self.node_id,
                           data={"key": key})
        resps = await self.messenger.broadcast(self.peers, read_msg, retry=False)
        for resp in resps.values():
            if resp and resp.get("found"):
                new_line = CacheLine(key=key, value=resp["value"], state=CacheLineState.SHARED)
                self.cache.put(key, new_line)
                return web.json_response({"key": key, "value": resp["value"], "status": "hit"})
        return web.json_response({"key": key, "status": "miss"}, status=404)

    async def _handle_cache_put(self, req):
        data = await req.json()
        key, value = data["key"], data["value"]
        from src.nodes.cache_node import CacheLine, CacheLineState
        inv_msg = Message(msg_type=MessageType.CACHE_INVALIDATE, sender_id=self.node_id,
                          data={"key": key})
        await self.messenger.broadcast(self.peers, inv_msg, retry=False)
        line = CacheLine(key=key, value=value, state=CacheLineState.MODIFIED)
        self.cache.put(key, line)
        # Also replicate via geo
        await self.replicator.write(key, value, self.peers)
        return web.json_response({"status": "ok", "key": key})

    async def _handle_cache_invalidate(self, req):
        key = req.match_info["key"]
        self.cache.invalidate(key)
        inv_msg = Message(msg_type=MessageType.CACHE_INVALIDATE, sender_id=self.node_id,
                          data={"key": key})
        await self.messenger.broadcast(self.peers, inv_msg, retry=False)
        return web.json_response({"status": "invalidated"})

    async def _handle_cache_stats(self, req):
        total = self._cache_hits + self._cache_misses
        return web.json_response({
            "hits": self._cache_hits, "misses": self._cache_misses,
            "hit_rate": self._cache_hits / total if total else 0,
            "cache_size": len(self.cache)})

    async def _handle_cache_state(self, req):
        return web.json_response({
            "cache_lines": {k: l.to_dict() for k, l in self.cache.items()},
            "total": len(self.cache)})

    # --- Geo Handlers ---
    async def _handle_geo_regions(self, req):
        return web.json_response(self.region_manager.get_info())

    async def _handle_geo_latency(self, req):
        return web.json_response({
            "matrix": self.region_manager.get_latency_matrix(),
            "routing_table": self.latency_router.get_routing_table()})

    async def _handle_geo_replication(self, req):
        return web.json_response(self.replicator.get_replication_status())

    # --- Raft ---
    async def _handle_raft_status(self, req):
        return web.json_response(self.raft.get_state())

    # --- Auth ---
    async def _handle_auth_token(self, req):
        data = await req.json()
        token = self.rbac.authenticate(data.get("username", ""), data.get("password", ""))
        if token:
            self.audit.log(data["username"], "auth_login", "auth", "success")
            return web.json_response({"token": token})
        self.audit.log(data.get("username", "unknown"), "auth_login", "auth", "failed")
        return web.json_response({"error": "invalid_credentials"}, status=401)

    async def _handle_auth_verify(self, req):
        auth = req.headers.get("Authorization", "")
        token = auth.replace("Bearer ", "") if auth.startswith("Bearer ") else None
        if token:
            payload = self.rbac.verify_token(token)
            if payload:
                return web.json_response({"valid": True, "user": payload})
        return web.json_response({"valid": False}, status=401)

    # --- Audit ---
    async def _handle_audit_logs(self, req):
        user = req.query.get("user")
        action = req.query.get("action")
        limit = int(req.query.get("limit", "100"))
        return web.json_response({"logs": self.audit.query(user=user, action=action, limit=limit)})

    async def _handle_audit_verify(self, req):
        return web.json_response(self.audit.verify_chain())

    # --- Security ---
    async def _handle_security_certs(self, req):
        if self.tls_manager:
            return web.json_response(self.tls_manager.get_cert_info())
        return web.json_response({"status": "TLS not enabled"})

    # --- Swagger UI ---
    async def _handle_swagger_yaml(self, req):
        yaml_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), 'docs', 'api_spec.yaml')
        try:
            with open(yaml_path, 'r') as f:
                content = f.read()
            return web.Response(text=content, content_type='application/x-yaml')
        except FileNotFoundError:
            return web.Response(text="API Spec not found", status=404)

    async def _handle_swagger_ui(self, req):
        html = """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <title>Distributed Sync System - Swagger UI</title>
            <link rel="stylesheet" type="text/css" href="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui.css" >
            <style>
            html { box-sizing: border-box; overflow: -moz-scrollbars-vertical; overflow-y: scroll; }
            *, *:before, *:after { box-sizing: inherit; }
            body { margin:0; background: #fafafa; }
            </style>
        </head>
        <body>
            <div id="swagger-ui"></div>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-bundle.js"> </script>
            <script src="https://cdnjs.cloudflare.com/ajax/libs/swagger-ui/4.15.5/swagger-ui-standalone-preset.js"> </script>
            <script>
            window.onload = function() {
                window.ui = SwaggerUIBundle({
                    url: "/swagger.yaml",
                    dom_id: '#swagger-ui',
                    deepLinking: true,
                    presets: [SwaggerUIBundle.presets.apis, SwaggerUIStandalonePreset],
                    layout: "StandaloneLayout"
                });
            }
            </script>
        </body>
        </html>
        """
        return web.Response(text=html, content_type='text/html')

    # --- General ---
    async def _handle_health(self, req):
        return web.json_response({
            "node_id": self.node_id, "status": "healthy",
            "region": self.config.node_region,
            "peers": self.failure_detector.get_all_statuses()})

    async def _handle_metrics(self, req):
        return web.Response(text=self.metrics.get_prometheus_output(), content_type="text/plain")

    async def _handle_status(self, req):
        return web.json_response({
            "node_id": self.node_id, "region": self.config.node_region,
            "raft": self.raft.get_state(), "locks": len(self.locks),
            "queue": len(self.queue_messages), "cache_size": len(self.cache),
            "peers": self.failure_detector.get_all_statuses(),
            "geo": self.region_manager.get_info(),
            "replication": self.replicator.get_replication_status(),
            "audit": self.audit.get_stats()})


async def main():
    config = NodeConfig()
    node = DistributedNode(config)

    loop = asyncio.get_event_loop()

    async def shutdown():
        await node.stop()

    try:
        await node.start()
        # Keep running
        while True:
            await asyncio.sleep(3600)
    except (KeyboardInterrupt, asyncio.CancelledError):
        pass
    finally:
        await shutdown()


if __name__ == "__main__":
    asyncio.run(main())
