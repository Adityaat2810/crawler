package parser

import (
	"context"
	"fmt"
	"log"

	"github.com/querymesh/crawler/internal/bloom"
	"github.com/querymesh/crawler/internal/config"
	"github.com/querymesh/crawler/internal/health"
	"github.com/querymesh/crawler/internal/storage"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Parser struct {
	cfg 			config.Config
	kafkaClient  	*kgo.Client
	valkeyClient 	*redis.Client
	bloom 			*bloom.Filter
	s3 				*storage.S3Store
	extractor 		*Extractor
	logger 			*log.Logger
	healthServer 	*health.Server
}

func Init(
	ctx context.Context, cfg config.Config,
	logger *log.Logger,
) (*Parser, error) {

	if logger == nil {
		logger = log.Default()
	}

	// Initailize valkey client
	valkeyClient := redis.NewClient(&redis.Options{
		Addr:         cfg.ValkeyAddr,
		Password:     cfg.ValkeyPassword,
		DB:           0,
		DialTimeout:  cfg.FetchTimeout,
		ReadTimeout:  cfg.FetchTimeout,
		WriteTimeout: cfg.FetchTimeout,
		PoolSize:     10,
	})

	if err := valkeyClient.Ping(ctx).Err(); err != nil {
		logger.Printf("warning: failed to connect to Valkey: %v", err)
	}

	// Initialize Bloom filter for URL duplication
	bloomFilter, err := bloom.New(bloom.Config{
		Client:            valkeyClient,
		Key:               cfg.BloomFilterKey,
		ExpectedItems:     cfg.BloomExpectedItems,
		FalsePositiveRate: cfg.BloomFalsePositiveRate,
		TTL:               cfg.BloomTTL,
	})
	if err != nil {
		return nil, fmt.Errorf("bloom filter: %w", err)
	}

	s3Store, err := storage.NewS3Store(storage.S3Config{
		Endpoint:  cfg.S3Endpoint,
		Region:    cfg.S3Region,
		Bucket:    cfg.S3Bucket,
		AccessKey: cfg.S3AccessKey,
		SecretKey: cfg.S3SecretKey,
		UseSSL:    cfg.S3UseSSL,
		Timeout:   cfg.FetchTimeout,
	})

	if err != nil {
		return nil, fmt.Errorf("s3 store: %w", err)
	}

	// Initialize Kafka client
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBroker),
		kgo.ConsumerGroup(cfg.ParserGroupID),
		kgo.ConsumeTopics(cfg.FetchSuccessTopic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	// Initialize health server
	healthServer := health.NewServer(cfg.HealthPort)
	healthServer.RegisterChecker("valkey", health.ValkeyChecker(func(ctx context.Context) error {
		return valkeyClient.Ping(ctx).Err()
	}))

	// Initialize HTML extractor
	extractor := NewExtractor()

	return &Parser{
		cfg: 			cfg,
		kafkaClient:  	kafkaClient,
		valkeyClient: 	valkeyClient,
		bloom: 			bloomFilter,
		s3: 				s3Store,
		extractor: 		extractor,
		logger: 			logger,
		healthServer: 	healthServer,
	}, nil
}

// Run starts the parser worker
func (p *Parser) Run(ctx context.Context) error {
	defer p.kafkaClient.Close()

	// Start health server in background
	go func() {
		if err := p.healthServer.Start(); err != nil {
			p.logger.Printf("health server error: %v", err)
		}
	}()

	p.logger.Printf(
		"starting parser broker=%s group=%s topics=%v",
		p.cfg.KafkaBroker, p.cfg.ParserGroupID, []string{p.cfg.FetchSuccessTopic},
	)

	for {
		fetches := p.kafkaClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				p.logger.Printf("poll error topic=%s partition=%d err=%v",
					err.Topic, err.Partition, err.Err)
			}
		}

		for _, rec := range fetches.Records() {
			commit, err := p.handleRecord(ctx, rec)
			if err != nil {
				p.logger.Printf("handle error: %v", err)
			}
			if commit {
				if err := p.kafkaClient.CommitRecords(ctx, rec); err != nil {
					p.logger.Printf("commit error: %v", err)
				}
			}
		}
	}
}

// Close gracefully shuts down the parser
func (p *Parser) Close() error {
	p.kafkaClient.Close()
	return p.valkeyClient.Close()
}