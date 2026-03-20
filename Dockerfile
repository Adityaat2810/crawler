# Stage 1: Build the binary
FROM golang:1.22-alpine AS builder

WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o crawler ./cmd/crawler/main.go

# Stage 2: Runtime
FROM alpine:latest
RUN apk --no-cache add ca-certificates
WORKDIR /root/

# copy binary from builder stage
COPY --from=builder /app/crawler .

ENTRYPOINT ["./crawler"]
