"""Tests for Raft consensus algorithm."""

import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.consensus.raft import RaftConsensus, RaftState, LogEntry


class MockMessenger:
    def __init__(self):
        self.sent_messages = []
        self.broadcast_responses = {}

    async def send(self, host, port, message, retry=True):
        self.sent_messages.append({"host": host, "port": port, "message": message})
        return {"status": "ok"}

    async def broadcast(self, peers, message, retry=False):
        return self.broadcast_responses


@pytest.fixture
def peers():
    return [
        {"node_id": "node-2", "host": "node-2", "port": 8002},
        {"node_id": "node-3", "host": "node-3", "port": 8003},
    ]


@pytest.fixture
def messenger():
    return MockMessenger()


@pytest.fixture
def raft(peers, messenger):
    return RaftConsensus(
        node_id="node-1", peers=peers, messenger=messenger,
        election_timeout_min=0.1, election_timeout_max=0.2,
        heartbeat_interval=0.05,
    )


class TestRaftState:
    def test_initial_state(self, raft):
        assert raft.state == RaftState.FOLLOWER
        assert raft.current_term == 0
        assert raft.voted_for is None
        assert raft.leader_id is None

    def test_quorum_size_3_nodes(self, raft):
        assert raft.quorum_size == 2

    def test_log_entry_serialization(self):
        entry = LogEntry(term=1, index=1, command={"action": "acquire", "resource": "r1"})
        d = entry.to_dict()
        restored = LogEntry.from_dict(d)
        assert restored.term == 1
        assert restored.index == 1
        assert restored.command == {"action": "acquire", "resource": "r1"}


class TestLeaderElection:
    @pytest.mark.asyncio
    async def test_start_election_wins(self, raft, messenger):
        messenger.broadcast_responses = {
            "node-2": {"vote_granted": True, "term": 1},
            "node-3": {"vote_granted": True, "term": 1},
        }
        await raft._start_election()
        assert raft.state == RaftState.LEADER
        assert raft.current_term == 1

    @pytest.mark.asyncio
    async def test_start_election_loses(self, raft, messenger):
        messenger.broadcast_responses = {
            "node-2": {"vote_granted": False, "term": 1},
            "node-3": {"vote_granted": False, "term": 1},
        }
        await raft._start_election()
        assert raft.state == RaftState.CANDIDATE

    @pytest.mark.asyncio
    async def test_become_leader_initializes_state(self, raft):
        await raft._become_leader()
        assert raft.state == RaftState.LEADER
        assert raft.leader_id == "node-1"
        assert "node-2" in raft.next_index
        assert "node-3" in raft.next_index


class TestVoteRequest:
    @pytest.mark.asyncio
    async def test_grant_vote(self, raft):
        from src.communication.message_passing import Message, MessageType
        msg = Message(msg_type=MessageType.VOTE_REQUEST, sender_id="node-2",
                      term=1, data={"last_log_index": 0, "last_log_term": 0})
        resp = await raft._handle_vote_request(msg)
        assert resp["vote_granted"] is True

    @pytest.mark.asyncio
    async def test_reject_vote_already_voted(self, raft):
        from src.communication.message_passing import Message, MessageType
        raft.voted_for = "node-3"
        msg = Message(msg_type=MessageType.VOTE_REQUEST, sender_id="node-2",
                      term=0, data={"last_log_index": 0, "last_log_term": 0})
        resp = await raft._handle_vote_request(msg)
        assert resp["vote_granted"] is False


class TestLogReplication:
    @pytest.mark.asyncio
    async def test_append_entries(self, raft):
        from src.communication.message_passing import Message, MessageType
        entries = [{"term": 1, "index": 1, "command": {"action": "test"}}]
        msg = Message(msg_type=MessageType.APPEND_ENTRIES, sender_id="node-2",
                      term=1, data={"prev_log_index": 0, "prev_log_term": 0,
                                     "entries": entries, "leader_commit": 0})
        resp = await raft._handle_append_entries(msg)
        assert resp["success"] is True
        assert len(raft.log) == 1

    @pytest.mark.asyncio
    async def test_reject_stale_term(self, raft):
        from src.communication.message_passing import Message, MessageType
        raft.current_term = 5
        msg = Message(msg_type=MessageType.APPEND_ENTRIES, sender_id="node-2",
                      term=3, data={"prev_log_index": 0, "prev_log_term": 0,
                                     "entries": [], "leader_commit": 0})
        resp = await raft._handle_append_entries(msg)
        assert resp["success"] is False

    def test_get_state(self, raft):
        state = raft.get_state()
        assert state["node_id"] == "node-1"
        assert state["state"] == "follower"
        assert state["current_term"] == 0
