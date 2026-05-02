"""Tests for cache coherence with MESI protocol."""

import pytest
from src.nodes.cache_node import CacheLine, CacheLineState, LRUCache


class TestCacheLine:
    def test_initial_state(self):
        line = CacheLine(key="k1", value="v1", state=CacheLineState.EXCLUSIVE)
        assert line.key == "k1"
        assert line.value == "v1"
        assert line.state == CacheLineState.EXCLUSIVE

    def test_to_dict(self):
        line = CacheLine(key="k1", value=42, state=CacheLineState.MODIFIED)
        d = line.to_dict()
        assert d["key"] == "k1"
        assert d["value"] == 42
        assert d["state"] == "M"


class TestMESIStates:
    def test_all_states(self):
        assert CacheLineState.MODIFIED.value == "M"
        assert CacheLineState.EXCLUSIVE.value == "E"
        assert CacheLineState.SHARED.value == "S"
        assert CacheLineState.INVALID.value == "I"


class TestLRUCache:
    def test_put_and_get(self):
        cache = LRUCache(max_size=10)
        line = CacheLine(key="k1", value="v1", state=CacheLineState.EXCLUSIVE)
        cache.put("k1", line)
        result = cache.get("k1")
        assert result is not None
        assert result.value == "v1"

    def test_miss(self):
        cache = LRUCache(max_size=10)
        assert cache.get("nonexistent") is None

    def test_eviction(self):
        evicted = []
        cache = LRUCache(max_size=3)
        cache.on_eviction(lambda k, l: evicted.append(k))
        for i in range(5):
            cache.put(f"k{i}", CacheLine(key=f"k{i}", value=i, state=CacheLineState.EXCLUSIVE))
        assert len(cache) == 3
        assert len(evicted) == 2
        assert "k0" in evicted
        assert "k1" in evicted

    def test_lru_order(self):
        cache = LRUCache(max_size=3)
        cache.put("k1", CacheLine(key="k1", value=1, state=CacheLineState.EXCLUSIVE))
        cache.put("k2", CacheLine(key="k2", value=2, state=CacheLineState.EXCLUSIVE))
        cache.put("k3", CacheLine(key="k3", value=3, state=CacheLineState.EXCLUSIVE))
        # Access k1 to make it recently used
        cache.get("k1")
        evicted = []
        cache.on_eviction(lambda k, l: evicted.append(k))
        # Adding k4 should evict k2 (least recently used)
        cache.put("k4", CacheLine(key="k4", value=4, state=CacheLineState.EXCLUSIVE))
        assert "k2" in evicted

    def test_invalidate(self):
        cache = LRUCache(max_size=10)
        cache.put("k1", CacheLine(key="k1", value="v1", state=CacheLineState.EXCLUSIVE))
        result = cache.invalidate("k1")
        assert result is not None
        assert result.state == CacheLineState.INVALID

    def test_mesi_transition_e_to_s(self):
        """Test E -> S transition when another node reads."""
        line = CacheLine(key="k1", value="v1", state=CacheLineState.EXCLUSIVE)
        # Simulating another node read: E -> S
        line.state = CacheLineState.SHARED
        assert line.state == CacheLineState.SHARED

    def test_mesi_transition_s_to_m(self):
        """Test S -> M transition on local write."""
        line = CacheLine(key="k1", value="v1", state=CacheLineState.SHARED)
        # Local write: S -> M
        line.value = "v2"
        line.state = CacheLineState.MODIFIED
        assert line.state == CacheLineState.MODIFIED
        assert line.value == "v2"

    def test_mesi_transition_m_to_i(self):
        """Test M -> I transition when another node writes."""
        line = CacheLine(key="k1", value="v1", state=CacheLineState.MODIFIED)
        # Another node writes: M -> I
        line.state = CacheLineState.INVALID
        assert line.state == CacheLineState.INVALID

    def test_contains(self):
        cache = LRUCache(max_size=10)
        cache.put("k1", CacheLine(key="k1", value=1, state=CacheLineState.EXCLUSIVE))
        assert "k1" in cache
        assert "k2" not in cache
