package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/querymesh/crawler/internal/config"
	"github.com/querymesh/crawler/internal/fetcher"
	"github.com/querymesh/crawler/internal/fetcher/kafkaconsumer"
)

func main() {
	cfg := config.Load()
	execMode := config.GetExecutionMode()

	logger := log.New(os.Stdout, "["+execMode+"] ", log.LstdFlags|log.Lmicroseconds)
	logger.Printf("starting in %s mode", execMode)

	// Setup graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		logger.Printf("received signal %v, shutting down...", sig)
		cancel()
	}()

	switch execMode {
	case "consumer":
		// Legacy consumer mode
		kafkaconsumer.InitConsumer(cfg)

	case "fetcher":
		// Full fetcher mode
		f, err := fetcher.Init(ctx, cfg, logger)
		if err != nil {
			logger.Fatalf("init fetcher: %v", err)
		}
		if err := f.Run(ctx); err != nil && err != context.Canceled {
			logger.Fatalf("fetcher run: %v", err)
		}
		logger.Println("fetcher stopped gracefully")

	default:
		logger.Fatalf("unknown EXECUTION_MODE: %s (valid: consumer, fetcher)", execMode)
	}
}
