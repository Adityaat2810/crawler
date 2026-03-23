package config

import (
	"log"
	"os"
	"strconv"
	"time"
)

// Config holds runtime configuration populated from environment variables.
type Config struct {
	KafkaBroker     string
	KafkaTopic      string
	KafkaGroupID    string
	WorkerCount     int
	HTTPTimeout     time.Duration
	ShutdownTimeout time.Duration
}

// Load reads environment variables and returns a Config with sane defaults.
func Load() Config {
	cfg := Config{
		KafkaBroker:     getenv("KAFKA_BROKER", "kafka:9092"),
		KafkaTopic:      getenv("KAFKA_TOPIC", "urls"),
		KafkaGroupID:    getenv("KAFKA_GROUP_ID", "crawler-consumer"),
		WorkerCount:     getInt("WORKER_COUNT", 4),
		HTTPTimeout:     getDuration("HTTP_TIMEOUT", 10*time.Second),
		ShutdownTimeout: getDuration("SHUTDOWN_TIMEOUT", 15*time.Second),
	}

	if cfg.WorkerCount < 1 {
		log.Printf("WORKER_COUNT was %d; raising to 1", cfg.WorkerCount)
		cfg.WorkerCount = 1
	}
	return cfg
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
