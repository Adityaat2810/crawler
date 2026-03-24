package kafkaconsumer

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"

	"github.com/example/consumer/internal/config"
	"github.com/example/consumer/internal/worker"
	"github.com/twmb/franz-go/pkg/kgo"
)

func InitConsumer(cfg config.Config) {
	logger := log.New(
		os.Stdout, "consumer", log.LstdFlags|log.Lmicroseconds,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(), syscall.SIGTERM,
	)

	defer stop()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cfg.KafkaBroker),
		kgo.ConsumerGroup(cfg.KafkaGroupID),
		kgo.ConsumeTopics(cfg.KafkaTopic),
	)

		if err != nil {
		logger.Fatalf("failed to create kafka client: %v", err)
	}

	defer client.Close()

	processor := worker.NewProcessor(worker.Options{
		Client: &http.Client{Timeout: cfg.HTTPTimeout},
		Logger: logger,
	})

	logger.Printf(
		"starting consumer broker=%s topic=%s group=%s workers=%d",
		cfg.KafkaBroker, cfg.KafkaTopic, cfg.KafkaGroupID, cfg.WorkerCount,
	)

	if err := Consume(
		ctx, client, processor.Handle, cfg.WorkerCount,
	); err != nil && err != context.Canceled {
		logger.Fatalf("consumer stopped with error: %v", err)
	}

	logger.Println("consumer exited cleanly")
}