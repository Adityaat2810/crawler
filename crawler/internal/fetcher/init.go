package fetcher

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"time"

	"github.com/querymesh/crawler/internal/config"
	"github.com/querymesh/crawler/internal/health"
	"github.com/querymesh/crawler/internal/robots"
	"github.com/querymesh/crawler/internal/storage"
	"github.com/redis/go-redis/v9"
	"github.com/twmb/franz-go/pkg/kgo"
)


type Fetcher struct {
	cfg          config.Config
	kafkaClient  *kgo.Client
	valkey       *redis.Client
	robots       *robots.Cache
	s3           *storage.S3Store
	logger       *log.Logger
	httpClient   *http.Client
	healthServer *health.Server
	maxRetries   int
	maxBodySize  int64
}

func Init(
	ctx context.Context, cfg config.Config, logger *log.Logger,
)(*Fetcher, error) {

	if logger == nil {
		logger = log.Default()
	}

	valkeyClient := redis.NewClient(&redis.Options{
		Addr:         cfg.ValkeyAddr,
		Password:     cfg.ValkeyPassword,
		DB:           0,
		DialTimeout:  5 * time.Second,
		ReadTimeout:  3 * time.Second,
		WriteTimeout: 3 * time.Second,
		PoolSize:     10,
	})

	if err := valkeyClient.Ping(ctx).Err(); err != nil {
		logger.Printf("Failed to connect to Valkey: %v", err)
	}

	// Initialize robots.txt cache
	robotsCache := robots.NewCache(robots.CacheOptions{
		Valkey:    valkeyClient,
		UserAgent: cfg.UserAgent,
		TTL:       24 * time.Hour,
		Timeout:   10 * time.Second,
		Logger:    logger,
	})

	// Initialize S3 storage
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

	// Initailaize http client
	httpClient := &http.Client{
		Timeout: cfg.FetchTimeout,
		Transport: &http.Transport{
			DialContext: (&net.Dialer{
				Timeout: 10 * time.Second,
				KeepAlive: 30 * time.Second,
			}).DialContext,
			MaxIdleConns:        100,
			IdleConnTimeout:     90 * time.Second,
			TLSHandshakeTimeout: 10 * time.Second,
		},
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 10 {
				return fmt.Errorf("stopped after 10 redirects")
			}

			return nil
		},
	}

	// Initialize Kafka client
	kafkaClient, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBroker),
		kgo.ConsumerGroup(cfg.FetcherGroupID),
		kgo.ConsumeTopics(cfg.FrontierReadyTopic),
		kgo.DisableAutoCommit(),
	)

	if err != nil {
		return nil, fmt.Errorf("kafka client: %w", err)
	}

	// Initialize health server
	healthServer := health.NewServer(8080)
	healthServer.RegisterChecker("valkey", health.ValkeyChecker(func(ctx context.Context) error {
		return valkeyClient.Ping(ctx).Err()
	}))

	return &Fetcher{
		cfg:          cfg,
		kafkaClient:  kafkaClient,
		valkey:       valkeyClient,
		robots:       robotsCache,
		s3:           s3Store,
		logger:       logger,
		httpClient:   httpClient,
		healthServer: healthServer,
		maxRetries:   5,
		maxBodySize:  10 * 1024 * 1024, // 10MB max
	}, nil

}


func (f *Fetcher) Run(ctx context.Context) error {
	defer f.kafkaClient.Close()

	// start health server in bg
	go func(){
		if err := f.healthServer.Start(); err != nil && err != http.ErrServerClosed {
			f.logger.Printf("health server error: %v", err)
		}
	}()

	f.logger.Printf(
		"starting fetcher broker=%s group=%s topics=%v",
		f.cfg.KafkaBroker, f.cfg.FetcherGroupID, []string{f.cfg.FrontierReadyTopic},
	)

	for {
		fetches := f.kafkaClient.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return nil
		}

		if errs := fetches.Errors(); len(errs) > 0 {
			for _, err := range errs {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				f.logger.Printf("poll error topic=%s partition=%d err=%v",
					err.Topic, err.Partition, err.Err)
			}
		}

		for _, rec := range fetches.Records() {
			commit, err := f.handleRecord(ctx, rec)
			if err != nil {
				f.logger.Printf("handle error: %v", err)
			}
			if commit {
				if err := f.kafkaClient.CommitRecords(ctx, rec); err != nil {
					f.logger.Printf("commit error: %v", err)
				}
			}
		}
	}
}


