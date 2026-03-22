# ── Stage 1: build ─────────────────────────────────────────
FROM golang:1.22-alpine as builder

WORKDIR /app

# cache dependencies
COPY go.mod go.sum ./
RUN go mod downloand

# copy source and build and compile statically linkd binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o consumer .


# ── Stage 2: runtime ───────────────────────────────────────
FROM alpine:latest

RUN apk add --no-cache ca-certificates

WORKDIR /app
COPY --from=builder /app/consumer .

RUN addgroup -S appgroup && adduser -S appuser -G appgroup
USER appuser

ENTRYPOINT ["./consumer"]


