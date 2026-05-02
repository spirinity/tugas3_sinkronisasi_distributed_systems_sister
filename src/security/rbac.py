"""
Role-Based Access Control (RBAC) with HMAC token authentication.
Provides middleware for aiohttp to enforce permissions per endpoint.
"""

import hashlib
import hmac
import json
import logging
import time
from enum import Enum
from typing import Dict, List, Optional, Set

from aiohttp import web

logger = logging.getLogger(__name__)


class Role(str, Enum):
    ADMIN = "admin"
    OPERATOR = "operator"
    VIEWER = "viewer"


# Permission matrix: role -> set of allowed action patterns
PERMISSIONS: Dict[Role, Set[str]] = {
    Role.ADMIN: {
        "lock:*", "queue:*", "cache:*", "admin:*",
        "auth:*", "audit:*", "security:*", "geo:*",
    },
    Role.OPERATOR: {
        "lock:acquire", "lock:release", "lock:status",
        "queue:enqueue", "queue:dequeue", "queue:ack", "queue:status",
        "cache:get", "cache:put", "cache:stats",
        "geo:regions", "geo:latency",
    },
    Role.VIEWER: {
        "lock:status", "lock:deadlocks",
        "queue:status", "queue:ring",
        "cache:get", "cache:stats", "cache:state",
        "geo:regions", "geo:latency", "geo:replication-status",
    },
}


class RBACManager:
    """Manages users, roles, and token-based authentication."""

    def __init__(self, secret_key: str):
        self.secret_key = secret_key
        # In-memory user store: username -> {"role", "password_hash"}
        self._users: Dict[str, dict] = {
            "admin": {"role": Role.ADMIN, "password_hash": self._hash_password("admin123")},
            "operator": {"role": Role.OPERATOR, "password_hash": self._hash_password("oper123")},
            "viewer": {"role": Role.VIEWER, "password_hash": self._hash_password("view123")},
        }

    def _hash_password(self, password: str) -> str:
        return hashlib.sha256(password.encode()).hexdigest()

    def authenticate(self, username: str, password: str) -> Optional[str]:
        """Authenticate user and return a token, or None if invalid."""
        user = self._users.get(username)
        if not user:
            return None
        if user["password_hash"] != self._hash_password(password):
            return None
        return self._generate_token(username, user["role"].value)

    def _generate_token(self, username: str, role: str) -> str:
        """Generate an HMAC-based token."""
        payload = json.dumps({
            "username": username, "role": role,
            "issued_at": time.time(), "expires_at": time.time() + 3600,
        })
        sig = hmac.new(self.secret_key.encode(), payload.encode(), hashlib.sha256).hexdigest()
        # Token = base64-like: payload_hex.signature
        return payload.encode().hex() + "." + sig

    def verify_token(self, token: str) -> Optional[dict]:
        """Verify a token and return the payload, or None."""
        try:
            parts = token.split(".")
            if len(parts) != 2:
                return None
            payload_hex, sig = parts
            payload = bytes.fromhex(payload_hex).decode()
            expected_sig = hmac.new(
                self.secret_key.encode(), payload.encode(), hashlib.sha256
            ).hexdigest()
            if not hmac.compare_digest(sig, expected_sig):
                return None
            data = json.loads(payload)
            if data.get("expires_at", 0) < time.time():
                return None
            return data
        except Exception:
            return None

    def check_permission(self, role_str: str, action: str) -> bool:
        """Check if a role has permission for an action."""
        try:
            role = Role(role_str)
        except ValueError:
            return False
        allowed = PERMISSIONS.get(role, set())
        # Check exact match or wildcard
        action_parts = action.split(":")
        if action in allowed:
            return True
        if f"{action_parts[0]}:*" in allowed:
            return True
        return False

    def get_users(self) -> dict:
        return {u: {"role": d["role"].value} for u, d in self._users.items()}


def rbac_middleware(rbac: RBACManager, enabled: bool = True):
    """aiohttp middleware for RBAC enforcement."""

    @web.middleware
    async def middleware(request: web.Request, handler):
        # Skip auth for health, metrics, and auth endpoints
        skip_paths = {"/health", "/metrics", "/rpc", "/auth/token"}
        if request.path in skip_paths or not enabled:
            return await handler(request)

        # Extract token from Authorization header
        auth_header = request.headers.get("Authorization", "")
        token = auth_header.replace("Bearer ", "") if auth_header.startswith("Bearer ") else None

        if not token:
            return web.json_response({"error": "unauthorized", "message": "Token required"}, status=401)

        payload = rbac.verify_token(token)
        if not payload:
            return web.json_response({"error": "unauthorized", "message": "Invalid token"}, status=401)

        # Derive action from path
        path_parts = request.path.strip("/").split("/")
        action = ":".join(path_parts[:2]) if len(path_parts) >= 2 else path_parts[0]

        if not rbac.check_permission(payload["role"], action):
            return web.json_response(
                {"error": "forbidden", "message": f"Role '{payload['role']}' cannot perform '{action}'"},
                status=403)

        # Attach user info to request
        request["user"] = payload
        return await handler(request)

    return middleware
