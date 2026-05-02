# Deployment Guide

## Prerequisites

- Docker & Docker Compose (v2.0+)
- Python 3.11+ (untuk manual deployment)
- Git

## Quick Start (Docker)

### 1. Clone & Build
```bash
git clone <repository-url>
cd distributed-sync-system

# Build and start semua services
cd docker
docker compose up --build -d
```

### 2. Verify
```bash
# Check semua nodes healthy
curl http://localhost:8001/health
curl http://localhost:8002/health
curl http://localhost:8003/health

# Check Raft leader
curl http://localhost:8001/raft/status
```

### 3. Stop
```bash
docker compose down
```

## Scaling

```bash
# Scale ke 5 nodes (memerlukan update CLUSTER_NODES env)
docker compose up --scale node=5 -d
```

## Manual Deployment

### 1. Install Dependencies
```bash
pip install -r requirements.txt
```

### 2. Start Redis
```bash
docker run -d --name redis -p 6379:6379 redis:7-alpine
```

### 3. Start Nodes
```bash
# Terminal 1
NODE_ID=node-1 NODE_PORT=8001 NODE_REGION=us-east \
  REDIS_HOST=localhost CLUSTER_NODES=node-1:8001,node-2:8002,node-3:8003 \
  python -m src.main

# Terminal 2
NODE_ID=node-2 NODE_PORT=8002 NODE_REGION=ap-southeast \
  REDIS_HOST=localhost CLUSTER_NODES=node-1:8001,node-2:8002,node-3:8003 \
  python -m src.main

# Terminal 3
NODE_ID=node-3 NODE_PORT=8003 NODE_REGION=eu-west \
  REDIS_HOST=localhost CLUSTER_NODES=node-1:8001,node-2:8002,node-3:8003 \
  python -m src.main
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| NODE_ID | node-1 | Unique node identifier |
| NODE_HOST | 0.0.0.0 | Bind host |
| NODE_PORT | 8001 | Bind port |
| REDIS_HOST | redis | Redis hostname |
| REDIS_PORT | 6379 | Redis port |
| CLUSTER_NODES | node-1:8001,node-2:8002,node-3:8003 | Cluster topology |
| NODE_REGION | ap-southeast | Node region (us-east/ap-southeast/eu-west) |
| ENABLE_TLS | false | Enable mTLS |
| ENABLE_RBAC | false | Enable RBAC |
| SECRET_KEY | change-me | Secret for token signing |
| LOG_LEVEL | INFO | Logging level |

## Troubleshooting

### Node tidak bisa konek ke peer
- Pastikan semua nodes dalam network yang sama (Docker: `distsync` network)
- Check firewall rules untuk port yang digunakan
- Verify CLUSTER_NODES env variable benar

### Redis connection error
- Pastikan Redis sudah running: `docker ps | grep redis`
- Check REDIS_HOST sesuai environment (Docker: `redis`, Manual: `localhost`)

### Raft election timeout
- Jika nodes lambat untuk memilih leader, check network latency
- Increase election timeout di config jika diperlukan

### Lock acquire returns "not_leader"
- Request harus dikirim ke leader node
- Check `/raft/status` untuk mengetahui leader saat ini
- Response akan menyertakan `leader_id` untuk redirect

## Testing

```bash
# Unit tests
pytest tests/unit/ -v

# Integration tests (requires running cluster)
pytest tests/integration/ -v -m integration

# Load testing
locust -f benchmarks/load_test_scenarios.py \
  --host http://localhost:8001 \
  --headless -u 50 -r 10 -t 60s
```
