package worker

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Processor struct {
	client *http.Client
	logger *log.Logger
}

type Options struct {
	Client *http.Client
	Logger *log.Logger
}

// new processor with safe defaults
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

func (p *Processor) Handle(ctx context.Context, msg *kgo.Record) error {
	rawURL := strings.TrimSpace(string(msg.Value))
	fmt.Println("get this message", rawURL)

	return nil 
}