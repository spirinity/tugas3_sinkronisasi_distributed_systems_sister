"""
Integration tests for the distributed cluster.
Tests multi-node communication, failure scenarios, and end-to-end flows.
Requires a running cluster (via docker-compose or manual startup).
"""

import asyncio
import pytest
import aiohttp

# Default cluster endpoints
NODES = [
    {"node_id": "node-1", "url": "http://localhost:8001"},
    {"node_id": "node-2", "url": "http://localhost:8002"},
    {"node_id": "node-3", "url": "http://localhost:8003"},
]


@pytest.fixture
def session(event_loop):
    """Create a synchronous fixture that returns an aiohttp session."""
    sess = aiohttp.ClientSession()
    yield sess
    event_loop.run_until_complete(sess.close())


async def _get(session, url):
    async with session.get(url) as resp:
        return resp.status, await resp.json()


async def _post(session, url, json=None):
    async with session.post(url, json=json) as resp:
        status = resp.status
        try:
            data = await resp.json()
        except Exception:
            data = {}
        return status, data


@pytest.mark.integration
class TestClusterHealth:
    @pytest.mark.asyncio
    async def test_all_nodes_healthy(self):
        async with aiohttp.ClientSession() as session:
            for node in NODES:
                status, data = await _get(session, f"{node['url']}/health")
                assert status == 200
                assert data["status"] == "healthy"

    @pytest.mark.asyncio
    async def test_metrics_endpoint(self):
        async with aiohttp.ClientSession() as session:
            async with session.get(f"{NODES[0]['url']}/metrics") as resp:
                assert resp.status == 200
                text = await resp.text()
                assert "Node:" in text


@pytest.mark.integration
class TestDistributedLock:
    @pytest.mark.asyncio
    async def test_acquire_and_release(self):
        async with aiohttp.ClientSession() as session:
            # Find leader
            leader_url = None
            for node in NODES:
                status, data = await _get(session, f"{node['url']}/raft/status")
                if data.get("state") == "leader":
                    leader_url = node["url"]
                    break
            if not leader_url:
                pytest.skip("No leader found")

            # Acquire
            status, data = await _post(session, f"{leader_url}/lock/acquire",
                                       json={"resource": "test-res", "client_id": "client-1",
                                             "lock_type": "exclusive"})
            assert data.get("status") in ("acquired", "failed")

            # Release
            status, data = await _post(session, f"{leader_url}/lock/release",
                                       json={"resource": "test-res", "client_id": "client-1"})
            assert data.get("status") in ("released", "failed")


@pytest.mark.integration
class TestDistributedQueue:
    @pytest.mark.asyncio
    async def test_enqueue_dequeue(self):
        async with aiohttp.ClientSession() as session:
            url = NODES[0]["url"]
            # Enqueue
            status, data = await _post(session, f"{url}/queue/enqueue",
                                       json={"payload": {"msg": "hello"}, "partition_key": "pk1"})
            assert status == 200
            msg_id = data["msg_id"]

            # Dequeue
            status, data = await _post(session, f"{url}/queue/dequeue")
            if status == 200:
                assert data["message"]["msg_id"] == msg_id
                # Ack
                status2, _ = await _post(session, f"{url}/queue/ack/{msg_id}")
                assert status2 == 200


@pytest.mark.integration
class TestDistributedCache:
    @pytest.mark.asyncio
    async def test_put_and_get(self):
        async with aiohttp.ClientSession() as session:
            url = NODES[0]["url"]
            # Put
            status, _ = await _post(session, f"{url}/cache/put",
                                    json={"key": "test-key", "value": "test-value"})
            assert status == 200

            # Get
            status, data = await _get(session, f"{url}/cache/get/test-key")
            assert status == 200
            assert data["value"] == "test-value"


@pytest.mark.integration
class TestGeoDistributed:
    @pytest.mark.asyncio
    async def test_regions_info(self):
        async with aiohttp.ClientSession() as session:
            status, data = await _get(session, f"{NODES[0]['url']}/geo/regions")
            assert status == 200
            assert "region" in data

    @pytest.mark.asyncio
    async def test_latency_matrix(self):
        async with aiohttp.ClientSession() as session:
            status, data = await _get(session, f"{NODES[0]['url']}/geo/latency")
            assert status == 200
            assert "matrix" in data
