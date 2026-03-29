# QueryMesh

**A distributed, cloud-native web crawler built for scale.**

QueryMesh is an event-driven web fetching system designed for horizontal scalability, fault tolerance, and polite crawling. Built with Go and orchestrated via Kubernetes, it processes URLs through Kafka, respects robots.txt directives, and stores raw content in S3-compatible storage.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              Q U E R Y M E S H                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐         ┌─────────────────────────────────────────────────┐
  │    URLs      │         │                   KAFKA                         │
  │   Producer   │────────►│  ┌───────────────┐    ┌───────────────────┐    │
  │   (Python)   │         │  │frontier.ready │    │ crawl.fetch.success│    │
  └──────────────┘         │  └───────┬───────┘    └─────────▲─────────┘    │
                           │          │                      │              │
                           │          │    ┌─────────────────┤              │
                           │          │    │  frontier.retry │              │
                           │          │    └────────┬────────┘              │
                           │          │             │                       │
                           │          │    ┌────────▼────────┐              │
                           │          │    │ crawl.fetch.dlq │              │
                           │          │    └─────────────────┘              │
                           └──────────┼─────────────────────────────────────┘
                                      │
                                      ▼
                           ┌──────────────────────────────────────────────────┐
                           │              F E T C H E R  (Go)                 │
                           │  ┌────────────────────────────────────────────┐  │
                           │  │  ┌─────────┐  ┌─────────┐  ┌─────────┐    │  │
                           │  │  │Worker 1 │  │Worker 2 │  │Worker N │    │  │
                           │  │  └────┬────┘  └────┬────┘  └────┬────┘    │  │
                           │  │       │            │            │         │  │
                           │  │       └────────────┼────────────┘         │  │
                           │  │                    ▼                      │  │
                           │  │         ┌──────────────────┐              │  │
                           │  │         │   Fetch Pipeline │              │  │
                           │  │         │  ├─ Robots Check │              │  │
                           │  │         │  ├─ Politeness   │              │  │
                           │  │         │  ├─ HTTP Fetch   │              │  │
                           │  │         │  └─ Store & Emit │              │  │
                           │  │         └──────────────────┘              │  │
                           │  └────────────────────────────────────────────┘  │
                           └───────────────────────┬──────────────────────────┘
                                                   │
                        ┌──────────────────────────┼───────────────────────────┐
                        │                          │                           │
                        ▼                          ▼                           ▼
               ┌─────────────────┐       ┌─────────────────┐         ┌─────────────────┐
               │     VALKEY      │       │    MINIO/S3     │         │  HEALTH SERVER  │
               │   (Redis Cache) │       │    (Storage)    │         │   :8080/health  │
               │                 │       │                 │         │                 │
               │ • Robots.txt    │       │ • Raw HTML      │         │ • /health/live  │
               │ • Politeness    │       │ • Headers       │         │ • /health/ready │
               │   rate limits   │       │ • Metadata      │         │ • /metrics      │
               └─────────────────┘       └─────────────────┘         └─────────────────┘
```

---

## Features

| Feature | Description |
|---------|-------------|
| **Distributed Processing** | Kafka consumer groups enable horizontal scaling |
| **Polite Crawling** | Respects `robots.txt` and enforces per-host crawl delays |
| **Fault Tolerant** | Exponential backoff retry with dead letter queue |
| **Cloud Native** | Kubernetes-first design with health probes |
| **S3 Compatible** | Custom AWS4 signature implementation (works with MinIO/S3) |
| **Content Preservation** | Stores full HTTP response with headers and metadata |
| **Intelligent Retry** | Classifies errors as retriable vs terminal |
| **Observable** | Health endpoints, structured logging, metrics-ready |

---

## Project Structure

```
QueryMesh/
├── crawler/                      # Main Go application
│   ├── cmd/
│   │   └── consumer/
│   │       └── main.go           # Application entry point
│   ├── internal/
│   │   ├── config/               # Configuration management
│   │   ├── fetcher/              # Core fetch pipeline
│   │   │   ├── classify.go       # Error classification logic
│   │   │   ├── handler.go        # URL fetch handling
│   │   │   ├── init.go           # Fetcher initialization
│   │   │   └── types.go          # Data structures
│   │   ├── health/               # Health check HTTP server
│   │   ├── kafka/                # Kafka consumer/producer utilities
│   │   ├── robots/               # Robots.txt parser with caching
│   │   ├── storage/              # S3 client with AWS4 signing
│   │   ├── utils/                # Helper utilities
│   │   └── worker/               # Legacy worker processor
│   ├── Dockerfile                # Multi-stage Docker build
│   └── go.mod                    # Go module definition
├── k8s/                          # Kubernetes manifests
│   ├── namespace.yaml            # querymesh namespace
│   ├── zookeeper.yaml            # Zookeeper for Kafka
│   ├── kafka.yaml                # Kafka broker
│   ├── kafka-topics-job.yaml     # Topic creation job
│   ├── valkey.yaml               # Valkey (Redis) StatefulSet
│   ├── minio.yaml                # MinIO S3 storage
│   ├── minio-bucket-job.yaml     # Bucket creation job
│   ├── fetcher-config.yaml       # Fetcher ConfigMap & Secrets
│   └── fetcher.yaml              # Fetcher Deployment
├── scripts/
│   ├── kafka_producer.py         # Test URL producer
│   └── requirements.txt          # Python dependencies
├── skaffold.yaml                 # Local development config
└── kafka_producer.py             # Convenience wrapper
```

---

## Fetch Pipeline

Every URL goes through a carefully orchestrated pipeline:

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         F E T C H   P I P E L I N E                     │
└─────────────────────────────────────────────────────────────────────────┘

    ╔═══════════════════════════════════════════════════════════════════╗
    ║  STEP 1: PARSE & VALIDATE                                         ║
    ║  • Unmarshal JSON job from Kafka                                  ║
    ║  • Validate URL format                                            ║
    ║  • Initialize attempt counter                                     ║
    ╚═══════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼
    ╔═══════════════════════════════════════════════════════════════════╗
    ║  STEP 2: ROBOTS.TXT CHECK                                         ║
    ║  • Fetch robots.txt (cached in Valkey, 24hr TTL)                  ║
    ║  • Check if URL path is allowed for our User-Agent                ║
    ║  • Blocked URLs → Dead Letter Queue                               ║
    ╚═══════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼
    ╔═══════════════════════════════════════════════════════════════════╗
    ║  STEP 3: POLITENESS ENFORCEMENT                                   ║
    ║  • Get Crawl-Delay from robots.txt (min: 1s, max: 60s)            ║
    ║  • Check per-host rate limit in Valkey                            ║
    ║  • Sleep if needed to respect delay                               ║
    ║  • Update next-allowed-fetch timestamp                            ║
    ╚═══════════════════════════════════════════════════════════════════╝
                                    │
                                    ▼
    ╔═══════════════════════════════════════════════════════════════════╗
    ║  STEP 4: HTTP FETCH                                               ║
    ║  • Create request with User-Agent header                          ║
    ║  • Execute with configured timeout (default: 30s)                 ║
    ║  • Read body up to max size (default: 10MB)                       ║
    ║  • Classify any errors (retriable vs terminal)                    ║
    ╚═══════════════════════════════════════════════════════════════════╝
                                    │
                    ┌───────────────┼───────────────┐
                    │               │               │
                    ▼               ▼               ▼
              ┌─────────┐    ┌───────────┐    ┌─────────┐
              │ SUCCESS │    │ RETRIABLE │    │ TERMINAL│
              │         │    │   ERROR   │    │  ERROR  │
              └────┬────┘    └─────┬─────┘    └────┬────┘
                   │               │               │
                   ▼               ▼               ▼
    ╔══════════════════╗  ╔═══════════════╗  ╔═══════════════╗
    ║ STEP 5a: STORE   ║  ║ STEP 5b:RETRY ║  ║ STEP 5c: DLQ  ║
    ║ • Hash content   ║  ║ • Exp backoff ║  ║ • Publish to  ║
    ║ • Upload to S3   ║  ║ • Publish to  ║  ║   DLQ topic   ║
    ║ • Emit success   ║  ║   retry topic ║  ║ • Log reason  ║
    ╚══════════════════╝  ╚═══════════════╝  ╚═══════════════╝
```

---

## Error Classification

The fetcher intelligently classifies errors to determine the appropriate action:

### Retriable Errors (Will Retry with Backoff)

| Error Type | Examples |
|------------|----------|
| **Network Timeouts** | Connection timeout, read timeout |
| **DNS Temporary Failures** | DNS server unavailable |
| **Connection Issues** | Connection refused, reset, EOF |
| **Server Errors** | HTTP 500, 502, 503, 504 |
| **Rate Limiting** | HTTP 408, 429 |
| **Cloudflare Errors** | HTTP 520-524 |

### Terminal Errors (Sent to DLQ)

| Error Type | Examples |
|------------|----------|
| **DNS Not Found** | NXDOMAIN |
| **Certificate Errors** | Invalid/expired SSL cert |
| **Client Errors** | HTTP 400, 401, 403, 404, 405, 410, 414, 451 |
| **Robots Blocked** | Disallowed by robots.txt |
| **Max Retries Exceeded** | Failed after 5 attempts |

### Retry Strategy

```
Attempt 1 failed  →  Wait ~1 min   →  Retry
Attempt 2 failed  →  Wait ~2 min   →  Retry
Attempt 3 failed  →  Wait ~4 min   →  Retry
Attempt 4 failed  →  Wait ~8 min   →  Retry
Attempt 5 failed  →  Wait ~16 min  →  Retry
Attempt 6 failed  →  Send to DLQ

* Delays include up to 25% jitter
* Maximum delay capped at 24 hours
```

---

## Kafka Topics

| Topic | Purpose |
|-------|---------|
| `frontier.ready` | Input queue - URLs ready to be fetched |
| `frontier.retry` | Retry queue - Failed URLs awaiting retry |
| `crawl.fetch.success` | Output - Successfully fetched URLs |
| `crawl.fetch.dlq` | Dead letter queue - Permanently failed URLs |

### Message Formats

**Input (frontier.ready):**
```json
{
  "url": "https://example.com/page",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "attempt": 1
}
```

**Success Output (crawl.fetch.success):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "url": "https://example.com/page",
  "s3_key": "raw/550e8400.../a1b2c3d4...",
  "status_code": 200,
  "content_type": "text/html",
  "fetch_timestamp_utc": "2024-01-15T10:30:00Z",
  "content_hash": "sha256:abcdef..."
}
```

**Dead Letter (crawl.fetch.dlq):**
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "url": "https://example.com/blocked",
  "attempt": 5,
  "reason": "max retries exceeded: connection timeout"
}
```

---

## S3 Storage Format

Fetched content is stored in S3 with full metadata:

**Key Pattern:** `raw/{job_id}/{sha256(url)}`

**Payload:**
```json
{
  "url": "https://example.com/page",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status_code": 200,
  "headers": {
    "Content-Type": ["text/html; charset=utf-8"],
    "Content-Length": ["12345"],
    "Last-Modified": ["Mon, 15 Jan 2024 10:00:00 GMT"]
  },
  "body_b64": "PCFET0NUWVBFIGh0bWw+Li4u",
  "content_type": "text/html",
  "fetch_timestamp_utc": "2024-01-15T10:30:00.123456789Z",
  "content_hash_sha256": "e3b0c44298fc1c149afbf4c8996fb924...",
  "attempt": 1
}
```

---

## Configuration

All configuration via environment variables:

| Variable | Default | Description |
|----------|---------|-------------|
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `FRONTIER_READY_TOPIC` | `frontier.ready` | Input topic |
| `FETCH_RETRY_TOPIC` | `frontier.retry` | Retry queue topic |
| `FETCH_SUCCESS_TOPIC` | `crawl.fetch.success` | Success output topic |
| `FETCH_DLQ_TOPIC` | `crawl.fetch.dlq` | Dead letter queue topic |
| `FETCHER_GROUP_ID` | `fetcher-group` | Kafka consumer group |
| `WORKER_COUNT` | `4` | Concurrent worker goroutines |
| `HTTP_TIMEOUT` | `10s` | General HTTP timeout |
| `FETCH_TIMEOUT` | `30s` | URL fetch timeout |
| `USER_AGENT` | `QueryMeshFetcher/0.1` | HTTP User-Agent header |
| `VALKEY_ADDR` | `valkey:6379` | Valkey/Redis address |
| `S3_ENDPOINT` | `s3:9000` | S3/MinIO endpoint |
| `S3_BUCKET` | `crawler-raw` | Storage bucket name |
| `S3_ACCESS_KEY` | - | S3 access key |
| `S3_SECRET_KEY` | - | S3 secret key |
| `S3_REGION` | `us-east-1` | S3 region |
| `HEALTH_PORT` | `8080` | Health check server port |

---

## Quick Start

### Prerequisites

- Docker & Kubernetes (Docker Desktop, minikube, or k3s)
- [Skaffold](https://skaffold.dev/docs/install/) for local development
- Python 3.8+ (for test producer)

### Local Development

```bash
# 1. Start the entire stack with Skaffold
skaffold dev

# 2. Wait for all pods to be ready
kubectl get pods -n querymesh -w

# 3. In another terminal, produce test URLs
cd scripts
pip install -r requirements.txt
python kafka_producer.py

# 4. Watch the logs
kubectl logs -n querymesh -l app=fetcher -f
```

### Manual Kubernetes Deployment

```bash
# 1. Create namespace and deploy infrastructure
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/zookeeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/valkey.yaml
kubectl apply -f k8s/minio.yaml

# 2. Wait for infrastructure to be ready
kubectl wait --for=condition=ready pod -l app=kafka -n querymesh --timeout=120s

# 3. Create Kafka topics and S3 bucket
kubectl apply -f k8s/kafka-topics-job.yaml
kubectl apply -f k8s/minio-bucket-job.yaml

# 4. Deploy fetcher
kubectl apply -f k8s/fetcher-config.yaml
kubectl apply -f k8s/fetcher.yaml
```

### Health Checks

```bash
# Port-forward to access health endpoints
kubectl port-forward -n querymesh svc/fetcher 8080:8080

# Check health
curl http://localhost:8080/health
curl http://localhost:8080/health/live
curl http://localhost:8080/health/ready
```

---

## Health Endpoints

| Endpoint | Purpose | Response |
|----------|---------|----------|
| `/health` | Full status with component details | JSON with all component statuses |
| `/health/live` | Kubernetes liveness probe | 200 if process is running |
| `/health/ready` | Kubernetes readiness probe | 200 if all dependencies are up |
| `/metrics` | Prometheus metrics (stub) | Placeholder for metrics |

**Health Response Example:**
```json
{
  "status": "healthy",
  "components": {
    "kafka": {"status": "healthy", "latency_ms": 2},
    "valkey": {"status": "healthy", "latency_ms": 1},
    "s3": {"status": "healthy", "latency_ms": 15}
  },
  "timestamp": "2024-01-15T10:30:00Z"
}
```

**Status Levels:**
- `healthy` - All components operational
- `degraded` - Non-critical component issues (Valkey, S3)
- `unhealthy` - Critical component failure (Kafka)

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Runtime** | Go 1.22 | High-performance, concurrent execution |
| **Container** | Alpine 3.20 | Minimal, secure base image |
| **Message Queue** | Apache Kafka | Event streaming & work distribution |
| **Cache** | Valkey (Redis fork) | Robots.txt & politeness caching |
| **Object Storage** | MinIO / S3 | Raw content storage |
| **Orchestration** | Kubernetes | Container orchestration |
| **Dev Workflow** | Skaffold | Local development & hot reload |

### Go Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/twmb/franz-go` | Modern, high-performance Kafka client |
| `github.com/redis/go-redis/v9` | Valkey/Redis client |
| `github.com/temoto/robotstxt` | Robots.txt parsing |
| `golang.org/x/sync` | errgroup for concurrent workers |

---

## Design Decisions

### Why Kafka?

- **Durability**: Messages persist until explicitly consumed
- **Scalability**: Consumer groups enable horizontal scaling
- **Ordering**: Per-partition ordering for rate limiting
- **Replay**: Can reprocess from any offset

### Why Custom S3 Signing?

- **No SDK bloat**: Zero external AWS SDK dependencies
- **Full control**: Custom retry and timeout behavior
- **Compatibility**: Works with any S3-compatible storage (MinIO, R2, etc.)

### Why Valkey over Redis?

- **Open source**: Truly open source Redis fork
- **API compatible**: Drop-in replacement
- **Community driven**: Active development after Redis license change

### Why Per-Host Politeness in Valkey?

- **Distributed**: All fetcher instances share rate limits
- **Persistent**: Survives fetcher restarts
- **Atomic**: Redis commands are atomic

---

## Roadmap

- [ ] Unit and integration tests
- [ ] Prometheus metrics integration
- [ ] URL deduplication (content hash-based)
- [ ] Link extraction and discovery
- [ ] Sitemap.xml parsing
- [ ] Respect `Retry-After` headers
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Configuration hot-reload
- [ ] Web UI for monitoring

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

## License

MIT License - see LICENSE file for details.
