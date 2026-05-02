"""Tests for distributed queue with consistent hashing."""

import pytest
from src.nodes.queue_node import ConsistentHashRing, QueueMessage, MessageState


class TestConsistentHashRing:
    def test_add_node(self):
        ring = ConsistentHashRing(replicas=10)
        ring.add_node("node-1")
        assert "node-1" in ring.nodes
        assert len(ring._sorted_keys) == 10

    def test_get_node(self):
        ring = ConsistentHashRing(replicas=100)
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")
        node = ring.get_node("test-key")
        assert node in {"node-1", "node-2", "node-3"}

    def test_consistent_mapping(self):
        ring = ConsistentHashRing(replicas=100)
        ring.add_node("node-1")
        ring.add_node("node-2")
        n1 = ring.get_node("key-A")
        n2 = ring.get_node("key-A")
        assert n1 == n2  # Same key always maps to same node

    def test_distribution(self):
        ring = ConsistentHashRing(replicas=150)
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")
        counts = {"node-1": 0, "node-2": 0, "node-3": 0}
        for i in range(300):
            node = ring.get_node(f"key-{i}")
            counts[node] += 1
        # Each node should get roughly 100 keys (±50%)
        for count in counts.values():
            assert count > 30, f"Poor distribution: {counts}"

    def test_remove_node(self):
        ring = ConsistentHashRing(replicas=10)
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.remove_node("node-2")
        assert "node-2" not in ring.nodes
        node = ring.get_node("any-key")
        assert node == "node-1"

    def test_get_nodes_for_key(self):
        ring = ConsistentHashRing(replicas=100)
        ring.add_node("node-1")
        ring.add_node("node-2")
        ring.add_node("node-3")
        nodes = ring.get_nodes_for_key("test", count=2)
        assert len(nodes) == 2
        assert len(set(nodes)) == 2  # No duplicates

    def test_ring_info(self):
        ring = ConsistentHashRing(replicas=10)
        ring.add_node("node-1")
        info = ring.get_ring_info()
        assert "node-1" in info["nodes"]
        assert info["total_vnodes"] == 10


class TestQueueMessage:
    def test_create_message(self):
        msg = QueueMessage(payload={"data": "test"})
        assert msg.payload == {"data": "test"}
        assert msg.state == MessageState.PENDING
        assert msg.retries == 0

    def test_serialization(self):
        msg = QueueMessage(payload={"key": "value"}, partition_key="pk-1")
        d = msg.to_dict()
        restored = QueueMessage.from_dict(d)
        assert restored.msg_id == msg.msg_id
        assert restored.payload == {"key": "value"}
        assert restored.partition_key == "pk-1"

    def test_message_states(self):
        assert MessageState.PENDING.value == "pending"
        assert MessageState.PROCESSING.value == "processing"
        assert MessageState.COMPLETED.value == "completed"
        assert MessageState.DEAD_LETTER.value == "dead_letter"
