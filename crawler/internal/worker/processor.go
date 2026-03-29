package worker

import (
	"context"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Processor handles Kafka messages in the legacy consumer mode
type Processor struct {
	client *http.Client
	logger *log.Logger
}

// Options configures the Processor
type Options struct {
	Client *http.Client
	Logger *log.Logger
}

// NewProcessor creates a new message processor with safe defaults
func NewProcessor(opts Options) *Processor {
	client := opts.Client
	if client == nil {
		client = &http.Client{Timeout: 10 * time.Second}
	}

	logger := opts.Logger
	if logger == nil {
		logger = log.Default()
	}

	return &Processor{client: client, logger: logger}
}

// Handle processes a single Kafka message
func (p *Processor) Handle(ctx context.Context, msg *kgo.Record) error {
	rawURL := strings.TrimSpace(string(msg.Value))

	if rawURL == "" {
		p.logger.Printf("empty message received, skipping")
		return nil
	}

	p.logger.Printf("received url: %s (partition=%d offset=%d)",
		rawURL, msg.Partition, msg.Offset)

	// In legacy consumer mode, this is just a stub
	// The fetcher mode handles the actual crawling
	return nil
}
