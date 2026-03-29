# QueryMesh Scripts

Utility scripts for testing and inspecting QueryMesh crawler data.

## Setup

```bash
cd scripts
pip install -r requirements.txt
```

## Scripts

### 1. kafka_producer.py - Seed URL Producer

Sends seed URLs to the frontier queue to start crawling.

```bash
# Send default test URLs
python kafka_producer.py

# Custom usage (edit the script to add more URLs)
```

**Output:**
```
[1/3] sent https://example.com/ -> partition=1 offset=0
[2/3] sent https://www.iana.org/domains/reserved -> partition=1 offset=1
[3/3] sent https://www.wikipedia.org/ -> partition=0 offset=0
```

---

### 2. read_content.py - Content Reader

Read and display crawled content from S3 in human-readable format.

#### Prerequisites

```bash
# Port-forward MinIO (if running in Kubernetes)
kubectl port-forward -n querymesh svc/s3 9000:9000

# Set environment variables (or use defaults)
export S3_ENDPOINT=localhost:9000
export S3_ACCESS_KEY=minioadmin
export S3_SECRET_KEY=minioadmin
```

#### Commands

| Command | Description |
|---------|-------------|
| `list-raw` | List all raw (fetched) content |
| `list-parsed` | List all parsed content |
| `raw <key>` | Read specific raw content by S3 key |
| `parsed <key>` | Read specific parsed content by S3 key |
| `url <url>` | Search for content by URL |

#### Examples

```bash
# List recent raw content (fetched HTML)
python read_content.py list-raw

# List recent parsed content (extracted text/links)
python read_content.py list-parsed

# Read raw content by key
python read_content.py raw raw/job-id/url-hash

# Read parsed content by key
python read_content.py parsed parsed/job-id/url-hash

# Search by URL (finds both raw and parsed)
python read_content.py url "https://www.wikipedia.org/"

# Limit list results
python read_content.py list-raw -n 50

# Disable colors (for piping to file)
python read_content.py list-raw --no-color > output.txt
```

#### Sample Output - List Raw

```
══════════════════════════════════════════════════════════════════════════════
  OBJECTS IN CRAWLER-RAW
══════════════════════════════════════════════════════════════════════════════

  Showing 2 of 2 objects

   1. raw/da900ebc-cfce-4515-8f97-d5b63e88815c/a32f3bb71...
      Size: 5,686 bytes  Modified: 2024-03-29 09:49

   2. raw/dc1fc2a6-a97b-4198-95b9-f76c92f76b4e/9a9d50ec8...
      Size: 37,955 bytes  Modified: 2024-03-29 09:49
```

#### Sample Output - Parsed Content

```
══════════════════════════════════════════════════════════════════════════════
  PARSED CONTENT
══════════════════════════════════════════════════════════════════════════════

▶ Document Info
────────────────────────────────────────
  URL: https://www.wikipedia.org/
  Job ID: dc1fc2a6-a97b-4198-95b9-f76c92f76b4e
  Title: Wikipedia
  Language: en

▶ Statistics
────────────────────────────────────────
  Text Length: 5,234 chars
  Links Found: 42
  New Links: 15
  Skipped (Dedup): 27

▶ Extracted Text (first 2000 chars)
────────────────────────────────────────
  Wikipedia The Free Encyclopedia
  
  English 6,000,000+ articles
  日本語 1,000,000+ 記事
  ...

▶ Extracted Links (42 total)
────────────────────────────────────────
  Internal: 30  External: 12  New: 15

  Sample Links:
    [NEW] [int] https://en.wikipedia.org/
          └─ "English"
    [seen] [ext] https://twitter.com/wikipedia
          └─ "Twitter"
```

---

### 3. read.sh - Convenience Wrapper

Automatically handles port-forwarding to MinIO.

```bash
# Make executable
chmod +x read.sh

# Use just like read_content.py
./read.sh list-raw
./read.sh list-parsed
./read.sh url "https://example.com"
```

---

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `S3_ENDPOINT` | `localhost:9000` | MinIO/S3 endpoint |
| `S3_ACCESS_KEY` | `minioadmin` | S3 access key |
| `S3_SECRET_KEY` | `minioadmin` | S3 secret key |
| `S3_USE_SSL` | `false` | Use HTTPS |
| `KAFKA_BROKER` | `localhost:9093` | Kafka bootstrap server |
| `NAMESPACE` | `querymesh` | Kubernetes namespace |

---

## Troubleshooting

### "Connection refused" error

Make sure port-forward is running:
```bash
kubectl port-forward -n querymesh svc/s3 9000:9000
```

### Garbled text in raw content

The content is likely gzip compressed. The `read_content.py` script automatically detects and decompresses gzip content. If you still see garbled text, check the `Content-Encoding` header in the raw content.

### No objects found

1. Check if the crawler has fetched any URLs:
   ```bash
   kubectl logs -n querymesh -l app=fetcher --tail=50
   ```

2. Check if URLs were sent to Kafka:
   ```bash
   python kafka_producer.py
   ```

3. Verify MinIO buckets exist:
   ```bash
   kubectl port-forward -n querymesh svc/s3 9000:9000
   # Then open http://localhost:9000 in browser
   # Login: minioadmin / minioadmin
   ```
