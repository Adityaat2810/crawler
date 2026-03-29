# ── Stage 1: build ─────────────────────────────────────────
FROM golang:1.22-alpine AS builder

WORKDIR /app

# System deps
RUN apk add --no-cache git ca-certificates tzdata

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Copy source and build
COPY . .
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build \
    -ldflags="-w -s" \
    -o crawler \
    ./cmd/consumer

# ── Stage 2: runtime ───────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app

COPY --from=builder /app/crawler .

# Run as non-root
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

# Health check port
EXPOSE 8080

ENTRYPOINT ["./crawler"]
