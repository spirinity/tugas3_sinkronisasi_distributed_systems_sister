"""Tests for distributed lock manager."""

import pytest
from src.nodes.lock_manager import LockState, LockType, DeadlockDetector


class TestLockState:
    def test_initial_state(self):
        lock = LockState("resource-1")
        assert lock.resource == "resource-1"
        assert lock.lock_type is None
        assert len(lock.holders) == 0
        assert lock.fencing_token == 0

    def test_to_dict(self):
        lock = LockState("r1")
        lock.lock_type = LockType.EXCLUSIVE
        lock.holders.add("client-1")
        d = lock.to_dict()
        assert d["resource"] == "r1"
        assert d["lock_type"] == "exclusive"
        assert "client-1" in d["holders"]


class TestDeadlockDetector:
    def test_no_deadlock(self):
        dd = DeadlockDetector()
        dd.add_wait("c1", {"c2"})
        dd.add_wait("c2", {"c3"})
        cycles = dd.detect_cycles()
        assert len(cycles) == 0

    def test_simple_deadlock(self):
        dd = DeadlockDetector()
        dd.add_wait("c1", {"c2"})
        dd.add_wait("c2", {"c1"})
        cycles = dd.detect_cycles()
        assert len(cycles) >= 1

    def test_three_way_deadlock(self):
        dd = DeadlockDetector()
        dd.add_wait("c1", {"c2"})
        dd.add_wait("c2", {"c3"})
        dd.add_wait("c3", {"c1"})
        cycles = dd.detect_cycles()
        assert len(cycles) >= 1

    def test_remove_client_breaks_cycle(self):
        dd = DeadlockDetector()
        dd.add_wait("c1", {"c2"})
        dd.add_wait("c2", {"c1"})
        dd.remove_client("c1")
        cycles = dd.detect_cycles()
        assert len(cycles) == 0


class TestLockTypes:
    def test_shared_lock(self):
        assert LockType.SHARED.value == "shared"

    def test_exclusive_lock(self):
        assert LockType.EXCLUSIVE.value == "exclusive"
