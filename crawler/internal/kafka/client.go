package kafka

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// Consumer handles Kafka message consumption
type Consumer struct {
	client      *kgo.Client
	topics      []string
	groupID     string
	logger      *log.Logger
	workerCount int
}

// ConsumerOptions configures the consumer
type ConsumerOptions struct {
	Brokers     []string
	Topics      []string
	GroupID     string
	Logger      *log.Logger
	WorkerCount int
}

// Handler processes a single Kafka record
type Handler func(ctx context.Context, rec *kgo.Record) error

// NewConsumer creates a new Kafka consumer
func NewConsumer(opts ConsumerOptions) (*Consumer, error) {
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}
	if opts.WorkerCount < 1 {
		opts.WorkerCount = 1
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
		kgo.ConsumerGroup(opts.GroupID),
		kgo.ConsumeTopics(opts.Topics...),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &Consumer{
		client:      client,
		topics:      opts.Topics,
		groupID:     opts.GroupID,
		logger:      opts.Logger,
		workerCount: opts.WorkerCount,
	}, nil
}

// Close shuts down the consumer
func (c *Consumer) Close() {
	c.client.Close()
}

// Consume starts consuming messages with multiple workers
func (c *Consumer) Consume(ctx context.Context, handler Handler) error {
	msgCh := make(chan *kgo.Record)
	g, ctx := errgroup.WithContext(ctx)

	// Poller goroutine
	g.Go(func() error {
		defer close(msgCh)
		for {
			fetches := c.client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return nil
			}

			if errs := fetches.Errors(); len(errs) > 0 {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				for _, err := range errs {
					c.logger.Printf("fetch error topic=%s partition=%d: %v",
						err.Topic, err.Partition, err.Err)
				}
			}

			iter := fetches.RecordIter()
			for !iter.Done() {
				rec := iter.Next()
				select {
				case <-ctx.Done():
					return ctx.Err()
				case msgCh <- rec:
				}
			}
		}
	})

	// Worker goroutines
	for i := 0; i < c.workerCount; i++ {
		g.Go(func() error {
			for rec := range msgCh {
				if err := handler(ctx, rec); err != nil {
					c.logger.Printf("handler error: %v", err)
				}
				if err := c.client.CommitRecords(ctx, rec); err != nil {
					c.logger.Printf("commit error: %v", err)
				}
			}
			return nil
		})
	}

	return g.Wait()
}

// Ping checks Kafka connectivity
func (c *Consumer) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return c.client.Ping(ctx)
}

// Producer handles Kafka message production
type Producer struct {
	client *kgo.Client
	logger *log.Logger
}

// ProducerOptions configures the producer
type ProducerOptions struct {
	Brokers []string
	Logger  *log.Logger
}

// NewProducer creates a new Kafka producer
func NewProducer(opts ProducerOptions) (*Producer, error) {
	if opts.Logger == nil {
		opts.Logger = log.Default()
	}

	client, err := kgo.NewClient(
		kgo.SeedBrokers(opts.Brokers...),
	)
	if err != nil {
		return nil, fmt.Errorf("create kafka client: %w", err)
	}

	return &Producer{
		client: client,
		logger: opts.Logger,
	}, nil
}

// Close shuts down the producer
func (p *Producer) Close() {
	p.client.Close()
}

// Emit sends a message to a topic
func (p *Producer) Emit(ctx context.Context, topic string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	rec := &kgo.Record{
		Topic: topic,
		Value: data,
	}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// EmitWithKey sends a message with a key to a topic
func (p *Producer) EmitWithKey(ctx context.Context, topic string, key string, payload interface{}) error {
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal payload: %w", err)
	}

	rec := &kgo.Record{
		Topic: topic,
		Key:   []byte(key),
		Value: data,
	}
	return p.client.ProduceSync(ctx, rec).FirstErr()
}

// Ping checks Kafka connectivity
func (p *Producer) Ping(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.client.Ping(ctx)
}

// Client returns the underlying kafka client for advanced usage
func (p *Producer) Client() *kgo.Client {
	return p.client
}
