# QueryMesh

**A distributed, cloud-native web crawler built for scale.**

QueryMesh is an event-driven web crawling system designed for horizontal scalability, fault tolerance, and polite crawling. Built with Go and orchestrated via Kubernetes, it processes URLs through Kafka, respects robots.txt directives, extracts content, and stores data in S3-compatible storage.

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────────────────────────────┐
│                                    Q U E R Y M E S H                                     │
└──────────────────────────────────────────────────────────────────────────────────────────┘

  ┌──────────────┐                         ┌─────────────────────────────────────────────┐
  │    Seed      │                         │                   KAFKA                     │
  │    URLs      │────────────────────────►│  frontier.ready ────────────────────────►   │
  │  (Producer)  │                         │       │              │                      │
  └──────────────┘                         │       │    crawl.fetch.success ──────────►  │
                                           │       │              │                      │
        ┌──────────────────────────────────┼───────┘              │    crawl.parse.success
        │                                  │                      │              │       │
        │  ◄───────── New URLs ────────────┼──────────────────────┼──────────────┘       │
        │                                  └──────────────────────┼──────────────────────┘
        │                                                         │
        ▼                                                         ▼
┌───────────────────────────────┐                    ┌───────────────────────────────┐
│       F E T C H E R           │                    │        P A R S E R            │
│  ┌─────────────────────────┐  │                    │  ┌─────────────────────────┐  │
│  │  • Robots.txt check     │  │                    │  │  • Read raw from S3     │  │
│  │  • Politeness delay     │  │                    │  │  • Extract text         │  │
│  │  • HTTP fetch           │  │     Success        │  │  • Extract links        │  │
│  │  • Error classification │  │ ──────────────────►│  │  • Bloom filter dedup   │  │
│  │  • Store raw to S3      │  │                    │  │  • Queue new URLs       │  │
│  │  • Emit success event   │  │                    │  │  • Store parsed to S3   │  │
│  └─────────────────────────┘  │                    │  └─────────────────────────┘  │
└───────────────────────────────┘                    └───────────────────────────────┘
        │                                                         │
        │                                                         │
        └────────────────────────┬────────────────────────────────┘
                                 │
         ┌───────────────────────┼───────────────────────┐
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│     VALKEY      │     │    MINIO/S3     │     │  HEALTH SERVER  │
│   (Redis Cache) │     │    (Storage)    │     │   :8080/health  │
│                 │     │                 │     │                 │
│ • Robots.txt    │     │ • crawler-raw   │     │ • /health/live  │
│ • Politeness    │     │ • crawler-parsed│     │ • /health/ready │
│ • Bloom filter  │     │                 │     │ • /metrics      │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

---

## Features

| Feature | Description |
|---------|-------------|
| **Distributed Processing** | Kafka consumer groups enable horizontal scaling |
| **Polite Crawling** | Respects `robots.txt` and enforces per-host crawl delays |
| **URL Deduplication** | Distributed Bloom filter prevents re-crawling URLs |
| **Content Extraction** | Extracts clean text and links from HTML |
| **Fault Tolerant** | Exponential backoff retry with dead letter queue |
| **Cloud Native** | Kubernetes-first design with health probes |
| **S3 Compatible** | Custom AWS4 signature implementation (works with MinIO/S3) |
| **Observable** | Health endpoints, structured logging, metrics-ready |

---

## Execution Modes

QueryMesh uses a single Docker image with multiple execution modes:

| Mode | `EXECUTION_MODE` | Description |
|------|------------------|-------------|
| **Fetcher** | `fetcher` | Fetches URLs, stores raw HTML in S3 |
| **Parser** | `parser` | Extracts text/links, queues new URLs |
| **Consumer** | `consumer` | Legacy consumer mode |

---

## Project Structure

```
QueryMesh/
├── crawler/                       # Main Go application
│   ├── cmd/consumer/main.go       # Entry point with mode switch
│   ├── internal/
│   │   ├── config/                # Configuration management
│   │   ├── fetcher/               # URL fetching pipeline
│   │   │   ├── classify.go        # Error classification
│   │   │   ├── handler.go         # Fetch handling
│   │   │   ├── init.go            # Initialization
│   │   │   └── types.go           # Data types
│   │   ├── parser/                # Content parsing pipeline
│   │   │   ├── extractor.go       # HTML text/link extraction
│   │   │   ├── handler.go         # Parse handling
│   │   │   ├── init.go            # Initialization
│   │   │   └── types.go           # Data types
│   │   ├── bloom/                 # Distributed Bloom filter
│   │   │   └── bloom.go           # Valkey-backed deduplication
│   │   ├── health/                # Health check server
│   │   ├── robots/                # Robots.txt cache
│   │   └── storage/               # S3 client
│   └── Dockerfile
├── k8s/                           # Kubernetes manifests
│   ├── fetcher.yaml               # Fetcher deployment
│   ├── fetcher-config.yaml        # Fetcher configuration
│   ├── parser.yaml                # Parser deployment
│   ├── parser-config.yaml         # Parser configuration
│   ├── kafka.yaml                 # Kafka broker
│   ├── valkey.yaml                # Valkey cache
│   └── minio.yaml                 # S3 storage
├── scripts/                       # Test utilities
└── skaffold.yaml                  # Local development
```

---

## Pipeline Flow

### 1. Fetcher Pipeline

```
frontier.ready ──► FETCHER ──► crawler-raw (S3) ──► crawl.fetch.success
                      │
                      ├── Robots.txt check
                      ├── Politeness enforcement
                      ├── HTTP fetch
                      ├── Error classification
                      └── Retry/DLQ handling
```

### 2. Parser Pipeline

```
crawl.fetch.success ──► PARSER ──► crawler-parsed (S3) ──► crawl.parse.success
                           │                                      │
                           ├── Read raw HTML from S3              │
                           ├── Extract text content               │
                           ├── Extract & normalize links          │
                           ├── Bloom filter deduplication ────────┘
                           └── Queue new URLs ──► frontier.ready
```

---

## URL Deduplication (Bloom Filter)

The parser uses a **distributed Bloom filter** backed by Valkey to efficiently deduplicate URLs:

```
┌─────────────────────────────────────────────────────────────────┐
│                    BLOOM FILTER DEDUPLICATION                   │
└─────────────────────────────────────────────────────────────────┘

   Extracted URL ──► Normalize ──► Check Bloom ──► Add to Bloom
                                        │
                         ┌──────────────┴──────────────┐
                         │                             │
                    NOT IN FILTER                  IN FILTER
                    (Definitely new)            (Probably seen)
                         │                             │
                         ▼                             ▼
                Queue to frontier              Skip (deduplicated)
```

**Configuration:**

| Variable | Default | Description |
|----------|---------|-------------|
| `BLOOM_FILTER_KEY` | `bloom:urls` | Redis key for bloom filter |
| `BLOOM_EXPECTED_ITEMS` | `10000000` | Expected number of URLs |
| `BLOOM_FALSE_POSITIVE_RATE` | `0.01` | 1% false positive rate |
| `BLOOM_TTL` | `0` | TTL in seconds (0 = no expiry) |

---

## Kafka Topics

| Topic | Direction | Description |
|-------|-----------|-------------|
| `frontier.ready` | Input (Fetcher) | URLs ready to be fetched |
| `frontier.retry` | Internal | Failed URLs awaiting retry |
| `crawl.fetch.success` | Output (Fetcher) → Input (Parser) | Successfully fetched |
| `crawl.fetch.dlq` | Output | Fetch dead letter queue |
| `crawl.parse.success` | Output (Parser) | Successfully parsed |
| `crawl.parse.dlq` | Output | Parse dead letter queue |

---

## Message Formats

### Frontier Job (frontier.ready)
```json
{
  "url": "https://example.com/page",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "attempt": 1,
  "parent_url": "https://example.com/",
  "depth": 2
}
```

### Fetch Success (crawl.fetch.success)
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

### Parse Success (crawl.parse.success)
```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "url": "https://example.com/page",
  "source_s3_key": "raw/550e8400.../a1b2c3d4...",
  "parsed_s3_key": "parsed/550e8400.../a1b2c3d4...",
  "links_extracted": 42,
  "new_links_queued": 15,
  "text_length": 5234,
  "parse_timestamp_utc": "2024-01-15T10:30:05Z"
}
```

---

## S3 Storage

### Raw Content Bucket (`crawler-raw`)

**Key:** `raw/{job_id}/{sha256(url)}`

```json
{
  "url": "https://example.com/page",
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "status_code": 200,
  "headers": {"Content-Type": ["text/html"]},
  "body_b64": "PCFET0NUWVBFIGh0bWw+...",
  "content_type": "text/html",
  "fetch_timestamp_utc": "2024-01-15T10:30:00Z",
  "content_hash_sha256": "e3b0c44298fc1c149afbf4c8996fb924..."
}
```

### Parsed Content Bucket (`crawler-parsed`)

**Key:** `parsed/{job_id}/{sha256(url)}`

```json
{
  "job_id": "550e8400-e29b-41d4-a716-446655440000",
  "url": "https://example.com/page",
  "source_s3_key": "raw/550e8400.../...",
  "title": "Example Page",
  "description": "This is an example page",
  "text": "Clean extracted text content...",
  "text_hash": "sha256:...",
  "text_length": 5234,
  "language": "en",
  "links": [
    {"url": "https://example.com/other", "text": "Other Page", "is_new": true, "type": "internal"},
    {"url": "https://external.com/", "text": "External", "is_new": false, "type": "external"}
  ],
  "links_count": 42,
  "new_links_count": 15,
  "skipped_count": 27,
  "parse_timestamp_utc": "2024-01-15T10:30:05Z",
  "processing_time_ms": 125
}
```

---

## Configuration

### Fetcher Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `EXECUTION_MODE` | `fetcher` | Set to `fetcher` |
| `KAFKA_BROKER` | `kafka:9092` | Kafka bootstrap server |
| `FETCHER_GROUP_ID` | `fetcher-group` | Consumer group |
| `FRONTIER_READY_TOPIC` | `frontier.ready` | Input topic |
| `FETCH_SUCCESS_TOPIC` | `crawl.fetch.success` | Output topic |
| `USER_AGENT` | `QueryMeshFetcher/0.1` | HTTP User-Agent |
| `S3_BUCKET` | `crawler-raw` | Raw content bucket |

### Parser Configuration

| Variable | Default | Description |
|----------|---------|-------------|
| `EXECUTION_MODE` | `parser` | Set to `parser` |
| `PARSER_GROUP_ID` | `parser-group` | Consumer group |
| `FETCH_SUCCESS_TOPIC` | `crawl.fetch.success` | Input topic |
| `PARSE_SUCCESS_TOPIC` | `crawl.parse.success` | Output topic |
| `FRONTIER_READY_TOPIC` | `frontier.ready` | URL queue topic |
| `S3_BUCKET` | `crawler-raw` | Raw content bucket |
| `S3_PARSED_BUCKET` | `crawler-parsed` | Parsed content bucket |
| `BLOOM_EXPECTED_ITEMS` | `10000000` | Expected URLs |
| `BLOOM_FALSE_POSITIVE_RATE` | `0.01` | FP rate (1%) |

---

## Quick Start

### Local Development with Skaffold

```bash
# 1. Start the entire stack
skaffold dev

# 2. Watch pods come up
kubectl get pods -n querymesh -w

# 3. Produce seed URLs
cd scripts
pip install -r requirements.txt
python kafka_producer.py

# 4. Watch the crawler work
kubectl logs -n querymesh -l app=fetcher -f
kubectl logs -n querymesh -l app=parser -f
```

### Manual Deployment

```bash
# Deploy infrastructure
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/zookeeper.yaml
kubectl apply -f k8s/kafka.yaml
kubectl apply -f k8s/valkey.yaml
kubectl apply -f k8s/minio.yaml

# Wait for infrastructure
kubectl wait --for=condition=ready pod -l app=kafka -n querymesh --timeout=120s

# Create topics and buckets
kubectl apply -f k8s/kafka-topics-job.yaml
kubectl apply -f k8s/minio-bucket-job.yaml

# Deploy fetcher and parser
kubectl apply -f k8s/fetcher-config.yaml
kubectl apply -f k8s/fetcher.yaml
kubectl apply -f k8s/parser-config.yaml
kubectl apply -f k8s/parser.yaml
```

---

## Scaling

Both fetcher and parser scale horizontally via Kafka consumer groups:

```bash
# Scale fetchers
kubectl scale deployment fetcher -n querymesh --replicas=5

# Scale parsers
kubectl scale deployment parser -n querymesh --replicas=3
```

The Bloom filter is distributed via Valkey, so all parser instances share deduplication state.

---

## Health Endpoints

Both fetcher and parser expose health endpoints:

| Endpoint | Purpose |
|----------|---------|
| `/health` | Full status with components |
| `/health/live` | Kubernetes liveness probe |
| `/health/ready` | Kubernetes readiness probe |

```bash
# Check fetcher health
kubectl port-forward -n querymesh svc/fetcher 8080:8080
curl http://localhost:8080/health

# Check parser health
kubectl port-forward -n querymesh svc/parser 8081:8080
curl http://localhost:8081/health
```

---

## Technology Stack

| Component | Technology | Purpose |
|-----------|------------|---------|
| **Runtime** | Go 1.22 | High-performance execution |
| **Message Queue** | Apache Kafka | Event streaming |
| **Cache** | Valkey (Redis) | Robots.txt, politeness, bloom filter |
| **Storage** | MinIO / S3 | Content storage |
| **Orchestration** | Kubernetes | Container orchestration |

### Go Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/twmb/franz-go` | Kafka client |
| `github.com/redis/go-redis/v9` | Valkey/Redis client |
| `github.com/temoto/robotstxt` | Robots.txt parsing |
| `github.com/google/uuid` | UUID generation |
| `golang.org/x/net/html` | HTML parsing |

---

## Design Decisions

### Single Image, Multiple Modes
- **Simpler CI/CD**: One image to build and deploy
- **Consistent dependencies**: Same Go binary for all workers
- **Easy scaling**: Just change `EXECUTION_MODE` env var

### Distributed Bloom Filter
- **Memory efficient**: ~120MB for 10M URLs at 1% FP rate
- **Shared state**: All parser instances use same filter
- **Persistent**: Survives restarts via Valkey

### Event-Driven Architecture
- **Decoupled**: Fetcher and parser are independent
- **Scalable**: Each component scales independently
- **Reliable**: Kafka provides durability and replay

---

## Roadmap

- [x] URL fetching with robots.txt compliance
- [x] Content parsing and text extraction
- [x] Link extraction and normalization
- [x] Distributed Bloom filter deduplication
- [ ] Prometheus metrics integration
- [ ] Content-hash based deduplication
- [ ] Sitemap.xml parsing
- [ ] Distributed tracing (OpenTelemetry)
- [ ] Web UI for monitoring

---

## Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

---

## License

MIT License - see LICENSE file for details.
