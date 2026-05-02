# Distributed Synchronization System

Implementasi sistem sinkronisasi terdistribusi yang mensimulasikan skenario real-world dari distributed systems. Sistem ini menangani multiple nodes yang berkomunikasi dan mensinkronisasi data secara konsisten.

## Features

### Core (70 poin)
- **Distributed Lock Manager** — Raft Consensus, shared/exclusive locks, deadlock detection
- **Distributed Queue** — Consistent hashing, at-least-once delivery, Redis persistence
- **Cache Coherence** — MESI protocol, LRU replacement, invalidation propagation
- **Containerization** — Docker multi-stage build, docker-compose orchestration

### Bonus (+10 poin)
- **Geo-Distributed System** — Multi-region simulation, latency-aware routing, eventual consistency
- **Security & Encryption** — mTLS, RBAC, tamper-proof audit logs

## Tech Stack

- **Language**: Python 3.11 (asyncio)
- **HTTP**: aiohttp
- **State Store**: Redis
- **Containerization**: Docker & Docker Compose
- **Testing**: pytest, locust

## Quick Start

### Docker (Recommended)
```bash
cd docker
docker compose up --build
```

Ini akan menjalankan:
- **Redis** on port 6379
- **Node 1** (us-east) on port 8001
- **Node 2** (ap-southeast) on port 8002
- **Node 3** (eu-west) on port 8003

### Manual
```bash
# Install dependencies
pip install -r requirements.txt

# Start Redis
docker run -d -p 6379:6379 redis:7-alpine

# Start nodes (in separate terminals)
NODE_ID=node-1 NODE_PORT=8001 NODE_REGION=us-east python -m src.main
NODE_ID=node-2 NODE_PORT=8002 NODE_REGION=ap-southeast python -m src.main
NODE_ID=node-3 NODE_PORT=8003 NODE_REGION=eu-west python -m src.main
```

## API Endpoints

### Lock Manager
| Method | Path | Description |
|--------|------|-------------|
| POST | `/lock/acquire` | Acquire shared/exclusive lock |
| POST | `/lock/release` | Release a lock |
| GET | `/lock/status` | View all locks |
| GET | `/lock/deadlocks` | Check for deadlocks |

### Queue
| Method | Path | Description |
|--------|------|-------------|
| POST | `/queue/enqueue` | Add message to queue |
| POST | `/queue/dequeue` | Consume a message |
| POST | `/queue/ack/{id}` | Acknowledge message |
| GET | `/queue/status` | Queue statistics |

### Cache
| Method | Path | Description |
|--------|------|-------------|
| GET | `/cache/get/{key}` | Read from cache |
| POST | `/cache/put` | Write to cache |
| DELETE | `/cache/invalidate/{key}` | Invalidate entry |
| GET | `/cache/stats` | Performance stats |

### Geo-Distributed
| Method | Path | Description |
|--------|------|-------------|
| GET | `/geo/regions` | List all regions |
| GET | `/geo/latency` | Latency matrix |
| GET | `/geo/replication-status` | Replication status |

### Security
| Method | Path | Description |
|--------|------|-------------|
| POST | `/auth/token` | Get auth token |
| GET | `/audit/logs` | Query audit logs |
| GET | `/audit/verify` | Verify log integrity |

### General
| Method | Path | Description |
|--------|------|-------------|
| GET | `/health` | Health check |
| GET | `/metrics` | Prometheus metrics |
| GET | `/status` | Full node status |
| GET | `/raft/status` | Raft consensus state |

## Usage Examples

### Acquire a Lock
```bash
curl -X POST http://localhost:8001/lock/acquire \
  -H "Content-Type: application/json" \
  -d '{"resource": "db-connection", "client_id": "worker-1", "lock_type": "exclusive"}'
```

### Enqueue a Message
```bash
curl -X POST http://localhost:8001/queue/enqueue \
  -H "Content-Type: application/json" \
  -d '{"payload": {"task": "process-data"}, "partition_key": "partition-1"}'
```

### Cache Read/Write
```bash
# Write
curl -X POST http://localhost:8001/cache/put \
  -H "Content-Type: application/json" \
  -d '{"key": "user:123", "value": {"name": "John"}}'

# Read
curl http://localhost:8001/cache/get/user:123
```

## Testing

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (requires running cluster)
pytest tests/integration/ -v -m integration

# Load testing
locust -f benchmarks/load_test_scenarios.py --host http://localhost:8001
```

## Architecture

See [docs/architecture.md](docs/architecture.md) for detailed architecture documentation.

## Project Structure

```
distributed-sync-system/
├── src/
│   ├── nodes/          # Node implementations
│   ├── consensus/      # Raft consensus
│   ├── communication/  # Message passing & failure detection
│   ├── geo/            # Geo-distributed features
│   ├── security/       # TLS, RBAC, audit logging
│   ├── utils/          # Config & metrics
│   └── main.py         # Unified node entry point
├── tests/              # Unit & integration tests
├── benchmarks/         # Load testing
├── docker/             # Dockerfile & docker-compose
├── docs/               # Documentation
└── requirements.txt
```
