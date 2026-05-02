"""
Prometheus-compatible metrics collection for the distributed system.
Tracks lock operations, queue throughput, cache hit/miss rates, and node health.
"""

import time
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class MetricsCollector:
    """Collects and exposes system metrics in Prometheus-compatible format."""

    node_id: str = ""

    # Counters
    _counters: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    # Gauges
    _gauges: Dict[str, float] = field(default_factory=lambda: defaultdict(float))
    # Histograms (store raw observations)
    _histograms: Dict[str, List[float]] = field(default_factory=lambda: defaultdict(list))

    def counter_inc(self, name: str, value: float = 1.0, labels: Dict[str, str] = None):
        """Increment a counter metric."""
        key = self._make_key(name, labels)
        self._counters[key] += value

    def gauge_set(self, name: str, value: float, labels: Dict[str, str] = None):
        """Set a gauge metric."""
        key = self._make_key(name, labels)
        self._gauges[key] = value

    def gauge_inc(self, name: str, value: float = 1.0, labels: Dict[str, str] = None):
        """Increment a gauge metric."""
        key = self._make_key(name, labels)
        self._gauges[key] += value

    def gauge_dec(self, name: str, value: float = 1.0, labels: Dict[str, str] = None):
        """Decrement a gauge metric."""
        key = self._make_key(name, labels)
        self._gauges[key] -= value

    def histogram_observe(self, name: str, value: float, labels: Dict[str, str] = None):
        """Record an observation for a histogram metric."""
        key = self._make_key(name, labels)
        self._histograms[key].append(value)

    def timer(self, name: str, labels: Dict[str, str] = None):
        """Context manager to time operations and record as histogram."""
        return _Timer(self, name, labels)

    def _make_key(self, name: str, labels: Dict[str, str] = None) -> str:
        if not labels:
            return name
        label_str = ",".join(f'{k}="{v}"' for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"

    def get_prometheus_output(self) -> str:
        """Export all metrics in Prometheus text exposition format."""
        lines = []
        lines.append(f'# Node: {self.node_id}')
        lines.append("")

        # Counters
        for key, value in sorted(self._counters.items()):
            lines.append(f"# TYPE {key.split('{')[0]} counter")
            lines.append(f"{key} {value}")

        # Gauges
        for key, value in sorted(self._gauges.items()):
            lines.append(f"# TYPE {key.split('{')[0]} gauge")
            lines.append(f"{key} {value}")

        # Histograms - output sum, count, and quantiles
        for key, observations in sorted(self._histograms.items()):
            base_name = key.split("{")[0]
            lines.append(f"# TYPE {base_name} summary")
            if observations:
                sorted_obs = sorted(observations)
                lines.append(f"{key}_count {len(observations)}")
                lines.append(f"{key}_sum {sum(observations):.6f}")
                # Quantiles
                for q in [0.5, 0.95, 0.99]:
                    idx = int(len(sorted_obs) * q)
                    idx = min(idx, len(sorted_obs) - 1)
                    lines.append(f'{key}{{quantile="{q}"}} {sorted_obs[idx]:.6f}')

        return "\n".join(lines) + "\n"

    def get_summary(self) -> dict:
        """Get a JSON-friendly summary of all metrics."""
        summary = {
            "node_id": self.node_id,
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {},
        }
        for key, obs in self._histograms.items():
            if obs:
                sorted_obs = sorted(obs)
                summary["histograms"][key] = {
                    "count": len(obs),
                    "sum": sum(obs),
                    "p50": sorted_obs[len(sorted_obs) // 2],
                    "p95": sorted_obs[int(len(sorted_obs) * 0.95)],
                    "p99": sorted_obs[min(int(len(sorted_obs) * 0.99), len(sorted_obs) - 1)],
                }
        return summary


class _Timer:
    """Context manager for timing operations."""

    def __init__(self, collector: MetricsCollector, name: str, labels: Dict[str, str] = None):
        self.collector = collector
        self.name = name
        self.labels = labels
        self.start_time = None

    def __enter__(self):
        self.start_time = time.monotonic()
        return self

    def __exit__(self, *args):
        elapsed = time.monotonic() - self.start_time
        self.collector.histogram_observe(self.name, elapsed, self.labels)


# Global metrics instance (set per node at startup)
metrics = MetricsCollector()
