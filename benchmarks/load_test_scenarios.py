"""
Load test scenarios using Locust for benchmarking the distributed system.
Tests: concurrent lock requests, queue throughput, cache hit rate.
"""

from locust import HttpUser, task, between, events
import json
import random
import time


class LockManagerUser(HttpUser):
    """Simulates clients performing lock operations."""
    wait_time = between(0.1, 0.5)
    weight = 3

    @task(3)
    def acquire_lock(self):
        resource = f"resource-{random.randint(1, 10)}"
        self.client.post("/lock/acquire", json={
            "resource": resource,
            "client_id": f"client-{self.environment.runner.user_count}",
            "lock_type": random.choice(["shared", "exclusive"]),
        }, name="/lock/acquire")

    @task(2)
    def release_lock(self):
        resource = f"resource-{random.randint(1, 10)}"
        self.client.post("/lock/release", json={
            "resource": resource,
            "client_id": f"client-{self.environment.runner.user_count}",
        }, name="/lock/release")

    @task(1)
    def check_status(self):
        self.client.get("/lock/status", name="/lock/status")


class QueueUser(HttpUser):
    """Simulates producers and consumers."""
    wait_time = between(0.05, 0.2)
    weight = 3

    @task(5)
    def enqueue(self):
        self.client.post("/queue/enqueue", json={
            "payload": {"data": f"msg-{random.randint(1, 10000)}",
                        "timestamp": time.time()},
            "partition_key": f"partition-{random.randint(1, 5)}",
        }, name="/queue/enqueue")

    @task(3)
    def dequeue(self):
        with self.client.post("/queue/dequeue", name="/queue/dequeue",
                              catch_response=True) as resp:
            if resp.status_code == 204:
                resp.success()

    @task(1)
    def queue_status(self):
        self.client.get("/queue/status", name="/queue/status")


class CacheUser(HttpUser):
    """Simulates cache read/write operations."""
    wait_time = between(0.05, 0.2)
    weight = 4

    @task(7)
    def cache_read(self):
        key = f"key-{random.randint(1, 50)}"
        with self.client.get(f"/cache/get/{key}", name="/cache/get/[key]",
                             catch_response=True) as resp:
            if resp.status_code == 404:
                resp.success()  # Cache miss is OK

    @task(3)
    def cache_write(self):
        key = f"key-{random.randint(1, 50)}"
        self.client.post("/cache/put", json={
            "key": key,
            "value": {"data": f"value-{random.randint(1, 1000)}"},
        }, name="/cache/put")

    @task(1)
    def cache_stats(self):
        self.client.get("/cache/stats", name="/cache/stats")
