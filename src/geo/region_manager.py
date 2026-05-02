"""
Multi-region simulation with configurable latency matrix.
Each node is assigned to a region and inter-region communication
is delayed according to the latency matrix.
"""

import asyncio
import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

# Simulated latency matrix (milliseconds)
LATENCY_MATRIX: Dict[str, Dict[str, float]] = {
    "us-east": {"us-east": 1, "ap-southeast": 180, "eu-west": 90},
    "ap-southeast": {"us-east": 180, "ap-southeast": 1, "eu-west": 200},
    "eu-west": {"us-east": 90, "ap-southeast": 200, "eu-west": 1},
}


class RegionManager:
    """Manages multi-region topology and simulated network latency."""

    def __init__(self, node_id: str, region: str):
        self.node_id = node_id
        self.region = region
        # node_id -> region mapping
        self._node_regions: Dict[str, str] = {node_id: region}
        # node_id -> {"host", "port", "region"}
        self._node_info: Dict[str, dict] = {}

    def register_node(self, node_id: str, host: str, port: int, region: str):
        """Register a node with its region."""
        self._node_regions[node_id] = region
        self._node_info[node_id] = {"host": host, "port": port, "region": region}

    def get_region(self, node_id: str) -> Optional[str]:
        return self._node_regions.get(node_id)

    def get_latency(self, from_region: str, to_region: str) -> float:
        """Get simulated latency in milliseconds between two regions."""
        return LATENCY_MATRIX.get(from_region, {}).get(to_region, 100)

    async def simulate_latency(self, target_node_id: str):
        """Simulate network latency to a target node based on region distance."""
        target_region = self._node_regions.get(target_node_id, self.region)
        latency_ms = self.get_latency(self.region, target_region)
        if latency_ms > 1:
            await asyncio.sleep(latency_ms / 1000.0)

    def get_nodes_in_region(self, region: str) -> List[str]:
        """Get all node IDs in a specific region."""
        return [nid for nid, r in self._node_regions.items() if r == region]

    def get_local_nodes(self) -> List[str]:
        """Get all nodes in the same region."""
        return self.get_nodes_in_region(self.region)

    def get_all_regions(self) -> Dict[str, List[str]]:
        """Get region -> node_ids mapping."""
        regions: Dict[str, List[str]] = {}
        for nid, r in self._node_regions.items():
            regions.setdefault(r, []).append(nid)
        return regions

    def get_latency_matrix(self) -> dict:
        return LATENCY_MATRIX

    def get_info(self) -> dict:
        return {
            "node_id": self.node_id,
            "region": self.region,
            "all_regions": self.get_all_regions(),
            "latency_matrix": LATENCY_MATRIX,
        }
