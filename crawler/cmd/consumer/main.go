package main

import (
	"log"

	"github.com/example/consumer/internal/config"
	"github.com/example/consumer/internal/fetcher/kafkaconsumer"
)

func main(){
	cfg := config.Load()
	execMode := config.GetExecutionMode()

	switch execMode {
		case "consumer":
			kafkaconsumer.InitConsumer(cfg)
		default:
			log.Fatalf("unknown EXECUTION_MODE: %s", execMode)
	}
}
