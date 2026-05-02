"""
Raft Consensus Algorithm implementation.
Provides leader election, log replication, and safety guarantees.
"""

import asyncio
import logging
import random
import time
from enum import Enum
from typing import Any, Dict, List, Optional

from src.communication.message_passing import MessagePassing, Message, MessageType

logger = logging.getLogger(__name__)


class RaftState(str, Enum):
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"


class LogEntry:
    def __init__(self, term: int, index: int, command: dict):
        self.term = term
        self.index = index
        self.command = command

    def to_dict(self):
        return {"term": self.term, "index": self.index, "command": self.command}

    @classmethod
    def from_dict(cls, d):
        return cls(term=d["term"], index=d["index"], command=d["command"])


class RaftConsensus:
    def __init__(self, node_id, peers, messenger, election_timeout_min=0.15,
                 election_timeout_max=0.30, heartbeat_interval=0.05):
        self.node_id = node_id
        self.peers = peers
        self.messenger = messenger
        self.current_term = 0
        self.voted_for = None
        self.log: List[LogEntry] = []
        self.state = RaftState.FOLLOWER
        self.leader_id = None
        self.commit_index = 0
        self.last_applied = 0
        self.next_index: Dict[str, int] = {}
        self.match_index: Dict[str, int] = {}
        self.election_timeout_min = election_timeout_min
        self.election_timeout_max = election_timeout_max
        self.heartbeat_interval = heartbeat_interval
        self._last_heartbeat = time.time()
        self._running = False
        self._election_task = None
        self._heartbeat_task = None
        self._on_commit_callbacks = []
        self._pending_requests: Dict[int, asyncio.Future] = {}

    @property
    def is_leader(self):
        return self.state == RaftState.LEADER

    @property
    def quorum_size(self):
        return (len(self.peers) + 1) // 2 + 1

    def on_commit(self, callback):
        self._on_commit_callbacks.append(callback)

    async def start(self):
        self._running = True
        self._last_heartbeat = time.time()
        self._election_task = asyncio.create_task(self._election_timer_loop())
        logger.info(f"[{self.node_id}] Raft started as {self.state.value}")

    async def stop(self):
        self._running = False
        for task in [self._election_task, self._heartbeat_task]:
            if task:
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass

    async def propose(self, command: dict) -> bool:
        if not self.is_leader:
            return False
        entry = LogEntry(term=self.current_term, index=len(self.log) + 1, command=command)
        self.log.append(entry)
        future = asyncio.get_event_loop().create_future()
        self._pending_requests[entry.index] = future
        await self._replicate_entries()
        try:
            return await asyncio.wait_for(future, timeout=5.0)
        except asyncio.TimeoutError:
            self._pending_requests.pop(entry.index, None)
            return False

    async def handle_message(self, message: Message) -> dict:
        if message.term > self.current_term:
            self.current_term = message.term
            self.state = RaftState.FOLLOWER
            self.voted_for = None
            self.leader_id = None
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
        handlers = {
            MessageType.VOTE_REQUEST: self._handle_vote_request,
            MessageType.VOTE_RESPONSE: self._handle_vote_response,
            MessageType.APPEND_ENTRIES: self._handle_append_entries,
            MessageType.APPEND_ENTRIES_RESPONSE: self._handle_append_entries_response,
        }
        handler = handlers.get(message.msg_type)
        if handler:
            return await handler(message)
        return {"status": "unhandled"}

    async def _election_timer_loop(self):
        while self._running:
            try:
                timeout = random.uniform(self.election_timeout_min, self.election_timeout_max) * 20
                await asyncio.sleep(timeout)
                if self.state != RaftState.LEADER:
                    elapsed = time.time() - self._last_heartbeat
                    if elapsed > timeout:
                        await self._start_election()
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Election timer error: {e}")
                await asyncio.sleep(1)

    async def _start_election(self):
        self.current_term += 1
        self.state = RaftState.CANDIDATE
        self.voted_for = self.node_id
        self.leader_id = None
        votes = 1
        last_log_index = len(self.log)
        last_log_term = self.log[-1].term if self.log else 0
        msg = Message(msg_type=MessageType.VOTE_REQUEST, sender_id=self.node_id,
                      term=self.current_term,
                      data={"last_log_index": last_log_index, "last_log_term": last_log_term})
        responses = await self.messenger.broadcast(self.peers, msg, retry=False)
        for resp in responses.values():
            if resp and resp.get("vote_granted"):
                votes += 1
        if self.state == RaftState.CANDIDATE and votes >= self.quorum_size:
            await self._become_leader()

    async def _become_leader(self):
        self.state = RaftState.LEADER
        self.leader_id = self.node_id
        next_idx = len(self.log) + 1
        for peer in self.peers:
            self.next_index[peer["node_id"]] = next_idx
            self.match_index[peer["node_id"]] = 0
        logger.info(f"[{self.node_id}] Became LEADER for term {self.current_term}")
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop())

    async def _heartbeat_loop(self):
        while self._running and self.is_leader:
            try:
                await self._replicate_entries()
                await asyncio.sleep(self.heartbeat_interval * 20)
            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"[{self.node_id}] Heartbeat error: {e}")
                await asyncio.sleep(1)

    async def _handle_vote_request(self, message: Message) -> dict:
        data = message.data
        grant = False
        if message.term >= self.current_term:
            if self.voted_for is None or self.voted_for == message.sender_id:
                last_idx = len(self.log)
                last_term = self.log[-1].term if self.log else 0
                c_idx = data.get("last_log_index", 0)
                c_term = data.get("last_log_term", 0)
                if c_term > last_term or (c_term == last_term and c_idx >= last_idx):
                    grant = True
                    self.voted_for = message.sender_id
                    self.current_term = message.term
                    self._last_heartbeat = time.time()
        return {"term": self.current_term, "vote_granted": grant, "voter_id": self.node_id}

    async def _handle_vote_response(self, message):
        return {"status": "ok"}

    async def _replicate_entries(self):
        if not self.is_leader:
            return
        for peer in self.peers:
            pid = peer["node_id"]
            ni = self.next_index.get(pid, 1)
            entries = [e.to_dict() for e in self.log[ni - 1:]] if ni <= len(self.log) else []
            pli = ni - 1
            plt = self.log[pli - 1].term if 0 < pli <= len(self.log) else 0
            msg = Message(msg_type=MessageType.APPEND_ENTRIES, sender_id=self.node_id,
                          term=self.current_term,
                          data={"prev_log_index": pli, "prev_log_term": plt,
                                "entries": entries, "leader_commit": self.commit_index})
            asyncio.create_task(self._send_ae(peer, msg))

    async def _send_ae(self, peer, msg):
        resp = await self.messenger.send(peer["host"], peer["port"], msg, retry=False)
        if resp:
            await self._process_ae_resp(peer["node_id"], msg, resp)

    async def _process_ae_resp(self, pid, msg, resp):
        if resp.get("term", 0) > self.current_term:
            self.current_term = resp["term"]
            self.state = RaftState.FOLLOWER
            self.voted_for = None
            return
        if resp.get("success"):
            entries = msg.data.get("entries", [])
            if entries:
                self.next_index[pid] = msg.data["prev_log_index"] + len(entries) + 1
                self.match_index[pid] = msg.data["prev_log_index"] + len(entries)
            await self._try_advance_commit()
        else:
            self.next_index[pid] = max(1, self.next_index.get(pid, 1) - 1)

    async def _try_advance_commit(self):
        if not self.is_leader:
            return
        for n in range(self.commit_index + 1, len(self.log) + 1):
            if self.log[n - 1].term != self.current_term:
                continue
            replicated = 1
            for peer in self.peers:
                if self.match_index.get(peer["node_id"], 0) >= n:
                    replicated += 1
            if replicated >= self.quorum_size:
                self.commit_index = n
                await self._apply_committed()

    async def _apply_committed(self):
        while self.last_applied < self.commit_index:
            self.last_applied += 1
            entry = self.log[self.last_applied - 1]
            for cb in self._on_commit_callbacks:
                try:
                    cb(entry)
                except Exception as e:
                    logger.error(f"Commit callback error: {e}")
            future = self._pending_requests.pop(entry.index, None)
            if future and not future.done():
                future.set_result(True)

    async def _handle_append_entries(self, message: Message) -> dict:
        data = message.data
        success = False
        if message.term >= self.current_term:
            self.current_term = message.term
            self.state = RaftState.FOLLOWER
            self.leader_id = message.sender_id
            self.voted_for = None
            self._last_heartbeat = time.time()
            if self._heartbeat_task:
                self._heartbeat_task.cancel()
                self._heartbeat_task = None
            pli = data.get("prev_log_index", 0)
            plt = data.get("prev_log_term", 0)
            if pli == 0:
                success = True
            elif pli <= len(self.log) and self.log[pli - 1].term == plt:
                success = True
            if success:
                entries = data.get("entries", [])
                if entries:
                    self.log = self.log[:pli]
                    for ed in entries:
                        self.log.append(LogEntry.from_dict(ed))
                lc = data.get("leader_commit", 0)
                if lc > self.commit_index:
                    self.commit_index = min(lc, len(self.log))
                    await self._apply_committed()
        return {"term": self.current_term, "success": success, "node_id": self.node_id}

    async def _handle_append_entries_response(self, message):
        return {"status": "ok"}

    def get_state(self):
        return {
            "node_id": self.node_id, "state": self.state.value,
            "current_term": self.current_term, "leader_id": self.leader_id,
            "voted_for": self.voted_for, "log_length": len(self.log),
            "commit_index": self.commit_index, "last_applied": self.last_applied,
        }
