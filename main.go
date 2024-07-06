package main

import (
	"arkis_test/database"
	"arkis_test/exchange"
	"arkis_test/processor"
	"arkis_test/queue"
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	log "github.com/sirupsen/logrus"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	rabbitURL := os.Getenv("RABBITMQ_URL")
	if rabbitURL == "" {
		log.Fatal("RABBITMQ_URL environment variable is not set")
	}

	queueNames := os.Getenv("QUEUE_NAMES")
	if queueNames == "" {
		log.Fatal("QUEUE_NAMES environment variable is not set")
	}

	db := database.D{}
	HandleSignals(cancel)

	_, err := exchange.New(rabbitURL, "exchange-input", "direct")
	if err != nil {
		log.WithError(err).Error("Failed to create input exchange")
	}

	exchange, err := exchange.New(rabbitURL, "exchange-output", "direct")
	if err != nil {
		log.WithError(err).Error("Failed to create output exchange")
	}

	queuePairs := strings.Split(queueNames, ",")

	for _, pair := range queuePairs {
		names := strings.Split(pair, ":")
		inputQueueName := names[0]
		outputQueueName := names[1]

		inputQueue, err := queue.New(rabbitURL, inputQueueName)
		if err != nil {
			log.WithError(err).WithField("queue", inputQueueName).Error("Failed to create input queue")
		}

		outputQueue, err := queue.New(rabbitURL, outputQueueName)
		if err != nil {
			log.WithError(err).WithField("queue", outputQueueName).Error("Failed to create output queue")
		}

		if err := inputQueue.BindToExchange("exchange-input", inputQueueName); err != nil {
			log.WithError(err).WithField("queue", inputQueueName).Error("Failed to bind input queue to exchange")
		}

		if err := outputQueue.BindToExchange("exchange-output", outputQueueName); err != nil {
			log.WithError(err).WithField("queue", outputQueueName).Error("Failed to bind output queue to exchange")
		}

		pr := processor.New(inputQueue, exchange, outputQueueName, db)
		go pr.Run(ctx)
	}

	log.Info("Application is ready to run")
	<-ctx.Done()
	log.Info("Application has shut down gracefully")
}

func HandleSignals(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigChan
		log.WithField("signal", sig).Info("Received signal, shutting down gracefully...")
		cancel()
	}()
}
