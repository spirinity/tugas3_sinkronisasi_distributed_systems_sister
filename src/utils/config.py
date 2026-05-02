"""
Configuration management using Pydantic Settings.
Loads from environment variables and .env files.
"""

import os
from typing import List, Optional
from pydantic_settings import BaseSettings
from pydantic import Field, field_validator


class NodeConfig(BaseSettings):
    """Configuration for a single node in the distributed system."""

    # Node identity
    node_id: str = Field(default="node-1", description="Unique identifier for this node")
    node_host: str = Field(default="0.0.0.0", description="Host to bind the node server")
    node_port: int = Field(default=8001, description="Port to bind the node server")

    # Redis
    redis_host: str = Field(default="redis", description="Redis server hostname")
    redis_port: int = Field(default=6379, description="Redis server port")

    # Cluster
    cluster_nodes: str = Field(
        default="node-1:8001,node-2:8002,node-3:8003",
        description="Comma-separated list of cluster nodes (id:port)",
    )

    # Geo-distributed
    node_region: str = Field(default="ap-southeast", description="Region for this node")

    # Security
    enable_tls: bool = Field(default=False, description="Enable TLS for inter-node communication")
    enable_rbac: bool = Field(default=False, description="Enable Role-Based Access Control")
    secret_key: str = Field(default="change-me-in-production", description="Secret key for token signing")

    # Logging
    log_level: str = Field(default="INFO", description="Logging level")

    # Cache settings
    cache_max_size: int = Field(default=1000, description="Maximum number of cache entries per node")

    # Queue settings
    queue_replication_factor: int = Field(default=2, description="Number of replicas for each queue message")

    # Lock settings
    lock_lease_timeout: float = Field(default=30.0, description="Lock lease timeout in seconds")

    # Raft settings
    raft_election_timeout_min: float = Field(default=0.15, description="Minimum election timeout (seconds)")
    raft_election_timeout_max: float = Field(default=0.30, description="Maximum election timeout (seconds)")
    raft_heartbeat_interval: float = Field(default=0.05, description="Heartbeat interval (seconds)")

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    def get_peer_nodes(self) -> List[dict]:
        """Parse cluster_nodes string into list of peer node dicts (excluding self)."""
        peers = []
        for entry in self.cluster_nodes.split(","):
            entry = entry.strip()
            if ":" not in entry:
                continue
            node_id, port = entry.rsplit(":", 1)
            if node_id != self.node_id:
                peers.append({
                    "node_id": node_id,
                    "host": node_id,  # In Docker, service name = hostname
                    "port": int(port),
                })
        return peers

    def get_all_nodes(self) -> List[dict]:
        """Parse cluster_nodes string into list of all node dicts."""
        nodes = []
        for entry in self.cluster_nodes.split(","):
            entry = entry.strip()
            if ":" not in entry:
                continue
            node_id, port = entry.rsplit(":", 1)
            nodes.append({
                "node_id": node_id,
                "host": node_id,
                "port": int(port),
            })
        return nodes

    @property
    def node_url(self) -> str:
        return f"http://{self.node_host}:{self.node_port}"
