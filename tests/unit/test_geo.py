"""Tests for geo-distributed features."""

import asyncio
import pytest
from src.geo.region_manager import RegionManager, LATENCY_MATRIX
from src.geo.latency_router import LatencyRouter
from src.geo.replicator import VectorClock, ReplicatedEntry, ReplicationStatus


class TestRegionManager:
    def test_initial_region(self):
        rm = RegionManager("node-1", "us-east")
        assert rm.region == "us-east"
        assert rm.get_region("node-1") == "us-east"

    def test_register_node(self):
        rm = RegionManager("node-1", "us-east")
        rm.register_node("node-2", "node-2", 8002, "ap-southeast")
        assert rm.get_region("node-2") == "ap-southeast"

    def test_latency_same_region(self):
        rm = RegionManager("node-1", "us-east")
        lat = rm.get_latency("us-east", "us-east")
        assert lat == 1

    def test_latency_cross_region(self):
        rm = RegionManager("node-1", "us-east")
        lat = rm.get_latency("us-east", "ap-southeast")
        assert lat == 180

    def test_get_nodes_in_region(self):
        rm = RegionManager("node-1", "us-east")
        rm.register_node("node-2", "node-2", 8002, "us-east")
        rm.register_node("node-3", "node-3", 8003, "eu-west")
        nodes = rm.get_nodes_in_region("us-east")
        assert "node-1" in nodes
        assert "node-2" in nodes
        assert "node-3" not in nodes

    def test_get_all_regions(self):
        rm = RegionManager("node-1", "us-east")
        rm.register_node("node-2", "node-2", 8002, "ap-southeast")
        regions = rm.get_all_regions()
        assert "us-east" in regions
        assert "ap-southeast" in regions

    @pytest.mark.asyncio
    async def test_simulate_latency_same_region(self):
        rm = RegionManager("node-1", "us-east")
        rm.register_node("node-2", "node-2", 8002, "us-east")
        # Same region should be ~1ms (very fast)
        import time
        start = time.monotonic()
        await rm.simulate_latency("node-2")
        elapsed = time.monotonic() - start
        assert elapsed < 0.1  # Should be nearly instant


class TestLatencyRouter:
    def test_get_nearest_node(self):
        rm = RegionManager("node-1", "us-east")
        rm.register_node("node-2", "node-2", 8002, "us-east")
        rm.register_node("node-3", "node-3", 8003, "eu-west")
        router = LatencyRouter(rm)
        candidates = [
            {"node_id": "node-2", "host": "node-2", "port": 8002},
            {"node_id": "node-3", "host": "node-3", "port": 8003},
        ]
        nearest = router.get_nearest_node(candidates)
        assert nearest["node_id"] == "node-2"  # Same region

    def test_record_latency(self):
        rm = RegionManager("node-1", "us-east")
        router = LatencyRouter(rm)
        router.record_latency("node-2", 50.0)
        router.record_latency("node-2", 60.0)
        assert router.get_avg_latency("node-2") == 55.0

    def test_local_first_sorting(self):
        rm = RegionManager("node-1", "us-east")
        rm.register_node("node-2", "node-2", 8002, "eu-west")
        rm.register_node("node-3", "node-3", 8003, "us-east")
        router = LatencyRouter(rm)
        candidates = [
            {"node_id": "node-2", "host": "node-2", "port": 8002},
            {"node_id": "node-3", "host": "node-3", "port": 8003},
        ]
        sorted_nodes = router.get_local_first(candidates)
        assert sorted_nodes[0]["node_id"] == "node-3"  # Same region first


class TestVectorClock:
    def test_increment(self):
        vc = VectorClock()
        vc.increment("node-1")
        vc.increment("node-1")
        assert vc._clock["node-1"] == 2

    def test_merge(self):
        vc1 = VectorClock({"node-1": 2, "node-2": 1})
        vc2 = VectorClock({"node-1": 1, "node-2": 3})
        vc1.merge(vc2)
        assert vc1._clock["node-1"] == 2
        assert vc1._clock["node-2"] == 3

    def test_dominates(self):
        vc1 = VectorClock({"node-1": 2, "node-2": 3})
        vc2 = VectorClock({"node-1": 1, "node-2": 2})
        assert vc1.dominates(vc2)
        assert not vc2.dominates(vc1)

    def test_concurrent(self):
        vc1 = VectorClock({"node-1": 2, "node-2": 1})
        vc2 = VectorClock({"node-1": 1, "node-2": 2})
        assert vc1.is_concurrent(vc2)

    def test_serialization(self):
        vc = VectorClock({"node-1": 3, "node-2": 5})
        d = vc.to_dict()
        restored = VectorClock.from_dict(d)
        assert restored._clock == {"node-1": 3, "node-2": 5}
