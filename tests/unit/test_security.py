"""Tests for security features: TLS, RBAC, and audit logging."""

import os
import tempfile
import pytest
from src.security.rbac import RBACManager, Role, PERMISSIONS
from src.security.audit_logger import AuditLogger, AuditEntry


class TestRBACManager:
    @pytest.fixture
    def rbac(self):
        return RBACManager(secret_key="test-secret-key")

    def test_authenticate_valid(self, rbac):
        token = rbac.authenticate("admin", "admin123")
        assert token is not None

    def test_authenticate_invalid(self, rbac):
        token = rbac.authenticate("admin", "wrong-password")
        assert token is None

    def test_verify_token(self, rbac):
        token = rbac.authenticate("admin", "admin123")
        payload = rbac.verify_token(token)
        assert payload is not None
        assert payload["username"] == "admin"
        assert payload["role"] == "admin"

    def test_verify_invalid_token(self, rbac):
        assert rbac.verify_token("invalid.token") is None

    def test_admin_permissions(self, rbac):
        assert rbac.check_permission("admin", "lock:acquire")
        assert rbac.check_permission("admin", "queue:enqueue")
        assert rbac.check_permission("admin", "cache:put")
        assert rbac.check_permission("admin", "admin:anything")

    def test_operator_permissions(self, rbac):
        assert rbac.check_permission("operator", "lock:acquire")
        assert rbac.check_permission("operator", "queue:enqueue")
        assert not rbac.check_permission("operator", "admin:anything")

    def test_viewer_permissions(self, rbac):
        assert rbac.check_permission("viewer", "lock:status")
        assert rbac.check_permission("viewer", "cache:get")
        assert not rbac.check_permission("viewer", "lock:acquire")
        assert not rbac.check_permission("viewer", "queue:enqueue")

    def test_get_users(self, rbac):
        users = rbac.get_users()
        assert "admin" in users
        assert users["admin"]["role"] == "admin"


class TestAuditLogger:
    @pytest.fixture
    def audit(self, tmp_path):
        log_file = str(tmp_path / "test_audit.jsonl")
        return AuditLogger(node_id="test-node", log_file=log_file)

    def test_log_entry(self, audit):
        audit.log("admin", "lock_acquire", "resource-1", "success")
        assert len(audit._entries) == 1
        assert audit._entries[0].user == "admin"

    def test_hash_chain_integrity(self, audit):
        audit.log("admin", "action1", "r1", "success")
        audit.log("admin", "action2", "r2", "success")
        audit.log("admin", "action3", "r3", "success")
        result = audit.verify_chain()
        assert result["valid"] is True
        assert result["entries"] == 3

    def test_tamper_detection(self, audit):
        audit.log("admin", "action1", "r1", "success")
        audit.log("admin", "action2", "r2", "success")
        # Tamper with an entry
        audit._entries[0].action = "tampered_action"
        result = audit.verify_chain()
        assert result["valid"] is False

    def test_query_by_user(self, audit):
        audit.log("admin", "action1", "r1", "success")
        audit.log("viewer", "action2", "r2", "success")
        audit.log("admin", "action3", "r3", "success")
        results = audit.query(user="admin")
        assert len(results) == 2

    def test_query_by_action(self, audit):
        audit.log("admin", "lock_acquire", "r1", "success")
        audit.log("admin", "lock_release", "r2", "success")
        results = audit.query(action="lock_acquire")
        assert len(results) == 1

    def test_persistence(self, tmp_path):
        log_file = str(tmp_path / "persist_test.jsonl")
        audit1 = AuditLogger(node_id="test", log_file=log_file)
        audit1.log("admin", "test", "r1", "ok")
        audit1.log("admin", "test2", "r2", "ok")
        # Reload
        audit2 = AuditLogger(node_id="test", log_file=log_file)
        assert len(audit2._entries) == 2
        assert audit2.verify_chain()["valid"] is True

    def test_genesis_hash(self, audit):
        audit.log("admin", "first", "r1", "ok")
        assert audit._entries[0].prev_hash == "genesis"

    def test_stats(self, audit):
        audit.log("admin", "test", "r1", "ok")
        stats = audit.get_stats()
        assert stats["total_entries"] == 1
        assert stats["chain_valid"] is True
