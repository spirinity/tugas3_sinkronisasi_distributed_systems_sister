"""
Tamper-proof audit logger using hash chains.
Each log entry contains the hash of the previous entry, forming
a blockchain-like chain that detects tampering.
"""

import hashlib
import json
import logging
import os
import time
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


class AuditEntry:
    def __init__(self, sequence: int, timestamp: float, node_id: str,
                 user: str, action: str, resource: str, result: str,
                 prev_hash: str = "", entry_hash: str = ""):
        self.sequence = sequence
        self.timestamp = timestamp
        self.node_id = node_id
        self.user = user
        self.action = action
        self.resource = resource
        self.result = result
        self.prev_hash = prev_hash
        self.entry_hash = entry_hash or self._compute_hash()

    def _compute_hash(self) -> str:
        content = json.dumps({
            "sequence": self.sequence, "timestamp": self.timestamp,
            "node_id": self.node_id, "user": self.user,
            "action": self.action, "resource": self.resource,
            "result": self.result, "prev_hash": self.prev_hash,
        }, sort_keys=True)
        return hashlib.sha256(content.encode()).hexdigest()

    def to_dict(self):
        return {
            "sequence": self.sequence, "timestamp": self.timestamp,
            "node_id": self.node_id, "user": self.user,
            "action": self.action, "resource": self.resource,
            "result": self.result, "prev_hash": self.prev_hash,
            "entry_hash": self.entry_hash,
        }

    @classmethod
    def from_dict(cls, d):
        return cls(**d)


class AuditLogger:
    """Tamper-proof audit logger with hash chain integrity."""

    def __init__(self, node_id: str, log_file: str = None):
        self.node_id = node_id
        self.log_file = log_file or f"audit_{node_id}.jsonl"
        self._entries: List[AuditEntry] = []
        self._sequence = 0
        self._load_from_file()

    def _load_from_file(self):
        """Load existing audit entries from file."""
        if os.path.exists(self.log_file):
            try:
                with open(self.log_file, "r") as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            entry = AuditEntry.from_dict(json.loads(line))
                            self._entries.append(entry)
                            self._sequence = max(self._sequence, entry.sequence)
            except Exception as e:
                logger.error(f"Error loading audit log: {e}")

    def log(self, user: str, action: str, resource: str, result: str):
        """Add a new audit log entry."""
        self._sequence += 1
        prev_hash = self._entries[-1].entry_hash if self._entries else "genesis"
        entry = AuditEntry(
            sequence=self._sequence, timestamp=time.time(),
            node_id=self.node_id, user=user, action=action,
            resource=resource, result=result, prev_hash=prev_hash)
        self._entries.append(entry)
        self._persist_entry(entry)

    def _persist_entry(self, entry: AuditEntry):
        """Append entry to file."""
        try:
            with open(self.log_file, "a") as f:
                f.write(json.dumps(entry.to_dict()) + "\n")
        except Exception as e:
            logger.error(f"Error persisting audit entry: {e}")

    def verify_chain(self) -> dict:
        """Verify the integrity of the hash chain."""
        if not self._entries:
            return {"valid": True, "entries": 0, "message": "Empty chain"}
        errors = []
        for i, entry in enumerate(self._entries):
            # Verify hash
            expected = entry._compute_hash()
            if entry.entry_hash != expected:
                errors.append({"sequence": entry.sequence, "error": "hash_mismatch"})
            # Verify chain
            if i == 0:
                if entry.prev_hash != "genesis":
                    errors.append({"sequence": entry.sequence, "error": "invalid_genesis"})
            else:
                if entry.prev_hash != self._entries[i - 1].entry_hash:
                    errors.append({"sequence": entry.sequence, "error": "chain_broken"})
        return {
            "valid": len(errors) == 0, "entries": len(self._entries),
            "errors": errors, "message": "Chain intact" if not errors else "Tampering detected",
        }

    def query(self, user: str = None, action: str = None, limit: int = 100) -> List[dict]:
        """Query audit logs with optional filters."""
        results = []
        for entry in reversed(self._entries):
            if user and entry.user != user:
                continue
            if action and entry.action != action:
                continue
            results.append(entry.to_dict())
            if len(results) >= limit:
                break
        return results

    def get_stats(self) -> dict:
        return {
            "total_entries": len(self._entries),
            "last_sequence": self._sequence,
            "chain_valid": self.verify_chain()["valid"],
        }
