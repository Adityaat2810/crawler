package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

// Config holds runtime configuration populated from environment variables.
type Config struct {
	// Kafka settings
	KafkaBroker        string
	KafkaTopic         string
	KafkaGroupID       string
	FetcherGroupID     string
	FrontierReadyTopic string
	FetchRetryTopic    string
	FetchSuccessTopic  string
	FetchDLQTopic      string

	// Worker settings
	WorkerCount int

	// HTTP settings
	HTTPTimeout     time.Duration
	FetchTimeout    time.Duration
	ShutdownTimeout time.Duration
	UserAgent       string

	// Valkey (Redis) settings
	ValkeyAddr     string
	ValkeyPassword string

	// S3/MinIO settings
	S3Endpoint  string
	S3Bucket    string
	S3AccessKey string
	S3SecretKey string
	S3UseSSL    bool
	S3Region    string

	// Health server
	HealthPort int
}

// Load reads environment variables and returns a Config with sane defaults.
func Load() Config {
	cfg := Config{
		// Kafka
		KafkaBroker:        getenv("KAFKA_BROKER", "kafka:9092"),
		KafkaTopic:         getenv("KAFKA_TOPIC", "urls"),
		KafkaGroupID:       getenv("KAFKA_GROUP_ID", "crawler-consumer"),
		FetcherGroupID:     getenv("FETCHER_GROUP_ID", "fetcher-group"),
		FrontierReadyTopic: getenv("FRONTIER_READY_TOPIC", "frontier.ready"),
		FetchRetryTopic:    getenv("FETCH_RETRY_TOPIC", "frontier.retry"),
		FetchSuccessTopic:  getenv("FETCH_SUCCESS_TOPIC", "crawl.fetch.success"),
		FetchDLQTopic:      getenv("FETCH_DLQ_TOPIC", "crawl.fetch.dlq"),

		// Workers
		WorkerCount: getInt("WORKER_COUNT", 4),

		// HTTP
		HTTPTimeout:     getDuration("HTTP_TIMEOUT", 10*time.Second),
		FetchTimeout:    getDuration("FETCH_TIMEOUT", 30*time.Second),
		ShutdownTimeout: getDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
		UserAgent:       getenv("USER_AGENT", "QueryMeshFetcher/0.1"),

		// Valkey
		ValkeyAddr:     getenv("VALKEY_ADDR", "valkey:6379"),
		ValkeyPassword: getenv("VALKEY_PASSWORD", ""),

		// S3
		S3Endpoint:  getenv("S3_ENDPOINT", "s3:9000"),
		S3Bucket:    getenv("S3_BUCKET", "crawler-raw"),
		S3AccessKey: getenv("S3_ACCESS_KEY", "minioadmin"),
		S3SecretKey: getenv("S3_SECRET_KEY", "minioadmin"),
		S3UseSSL:    getBool("S3_USE_SSL", false),
		S3Region:    getenv("S3_REGION", "us-east-1"),

		// Health
		HealthPort: getInt("HEALTH_PORT", 8080),
	}

	if cfg.WorkerCount < 1 {
		log.Printf("WORKER_COUNT was %d; raising to 1", cfg.WorkerCount)
		cfg.WorkerCount = 1
	}

	return cfg
}

// GetExecutionMode returns the execution mode from environment
func GetExecutionMode() string {
	return getenv("EXECUTION_MODE", "fetcher")
}

func getenv(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func getInt(key string, fallback int) int {
	if v := os.Getenv(key); v != "" {
		parsed, err := strconv.Atoi(v)
		if err != nil {
			log.Printf("cannot parse %s=%s as int: %v", key, v, err)
			return fallback
		}
		return parsed
	}
	return fallback
}

func getDuration(key string, fallback time.Duration) time.Duration {
	if v := os.Getenv(key); v != "" {
		parsed, err := time.ParseDuration(v)
		if err != nil {
			log.Printf("cannot parse %s=%s as duration: %v", key, v, err)
			return fallback
		}
		return parsed
	}
	return fallback
}

func getBool(key string, fallback bool) bool {
	if v := os.Getenv(key); v != "" {
		parsed, err := strconv.ParseBool(v)
		if err != nil {
			log.Printf("cannot parse %s=%s as bool: %v", key, v, err)
			return fallback
		}
		return parsed
	}
	return fallback
}
