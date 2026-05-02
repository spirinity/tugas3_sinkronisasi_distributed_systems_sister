"""
Latency-aware routing for geo-distributed system.
Routes requests to the nearest (lowest latency) node.
"""

import logging
import time
from typing import Dict, List, Optional

from src.geo.region_manager import RegionManager

logger = logging.getLogger(__name__)


class LatencyRouter:
    """Routes requests to nodes based on measured and simulated latency."""

    def __init__(self, region_manager: RegionManager):
        self.region_manager = region_manager
        # Measured latencies: node_id -> [latency_samples]
        self._measured_latencies: Dict[str, List[float]] = {}

    def record_latency(self, node_id: str, latency_ms: float):
        """Record a measured latency to a node."""
        if node_id not in self._measured_latencies:
            self._measured_latencies[node_id] = []
        samples = self._measured_latencies[node_id]
        samples.append(latency_ms)
        # Keep last 50 samples
        if len(samples) > 50:
            self._measured_latencies[node_id] = samples[-50:]

    def get_avg_latency(self, node_id: str) -> float:
        """Get average measured latency to a node, or simulated if not measured."""
        samples = self._measured_latencies.get(node_id, [])
        if samples:
            return sum(samples) / len(samples)
        # Fall back to simulated
        target_region = self.region_manager.get_region(node_id)
        if target_region:
            return self.region_manager.get_latency(self.region_manager.region, target_region)
        return 100.0

    def get_nearest_node(self, candidates: List[dict], exclude: set = None) -> Optional[dict]:
        """Select the node with lowest latency from candidates."""
        exclude = exclude or set()
        best = None
        best_latency = float("inf")
        for node in candidates:
            nid = node["node_id"]
            if nid in exclude:
                continue
            lat = self.get_avg_latency(nid)
            if lat < best_latency:
                best_latency = lat
                best = node
        return best

    def get_local_first(self, candidates: List[dict]) -> List[dict]:
        """Sort candidates with local region first, then by latency."""
        local_region = self.region_manager.region

        def sort_key(node):
            nid = node["node_id"]
            region = self.region_manager.get_region(nid) or ""
            is_local = 0 if region == local_region else 1
            latency = self.get_avg_latency(nid)
            return (is_local, latency)

        return sorted(candidates, key=sort_key)

    def get_routing_table(self) -> dict:
        """Get current routing table with latencies."""
        table = {}
        all_regions = self.region_manager.get_all_regions()
        for region, nodes in all_regions.items():
            table[region] = []
            for nid in nodes:
                table[region].append({
                    "node_id": nid,
                    "avg_latency_ms": round(self.get_avg_latency(nid), 2),
                    "samples": len(self._measured_latencies.get(nid, [])),
                })
        return table
