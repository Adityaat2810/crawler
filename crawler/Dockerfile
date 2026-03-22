# ── Stage 1: build ─────────────────────────────────────────
FROM golang:1.25-alpine AS builder

WORKDIR /app

# System deps
RUN apk add --no-cache git ca-certificates

# cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# copy source and build the binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o consumer ./cmd/consumer


# ── Stage 2: runtime ───────────────────────────────────────
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/consumer .

# Run as non-root
RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

ENTRYPOINT ["./consumer"]

