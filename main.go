package main

import (
	"arkis_test/database"
	"arkis_test/exchange"
	"arkis_test/processor"
	"arkis_test/queue"
	"context"
	"os"

	log "github.com/sirupsen/logrus"
)

func main() {
	ctx := context.Background()

	inputQueueA, err := queue.New(os.Getenv("RABBITMQ_URL"), "input-A")
	if err != nil {
		log.WithError(err).Panic("Cannot create input queue A")
	}

	outputQueueA, err := queue.New(os.Getenv("RABBITMQ_URL"), "output-A")
	if err != nil {
		log.WithError(err).Panic("Cannot create output queue A")
	}

	inputQueueB, err := queue.New(os.Getenv("RABBITMQ_URL"), "input-B")
	if err != nil {
		log.WithError(err).Panic("Cannot create input queue B")
	}

	outputQueueB, err := queue.New(os.Getenv("RABBITMQ_URL"), "output-B")
	if err != nil {
		log.WithError(err).Panic("Cannot create output queue B")
	}

	_, err = exchange.New(os.Getenv("RABBITMQ_URL"), "exchange-input", "direct")
	if err != nil {
		log.WithError(err).Panic("Cannot create input exchange")
	}

	_, err = exchange.New(os.Getenv("RABBITMQ_URL"), "exchange-output", "direct")
	if err != nil {
		log.WithError(err).Panic("Cannot create output exchange")
	}

	if err := inputQueueA.BindToExchange("exchange-input", "input-A"); err != nil {
		log.WithError(err).Panic("Cannot bind input queue A to exchange A")
	}

	if err := outputQueueA.BindToExchange("exchange-output", "output-A"); err != nil {
		log.WithError(err).Panic("Cannot bind output queue A to exchange A")
	}

	if err := inputQueueB.BindToExchange("exchange-input", "input-B"); err != nil {
		log.WithError(err).Panic("Cannot bind input queue B to exchange B")
	}

	if err := outputQueueB.BindToExchange("exchange-output", "output-B"); err != nil {
		log.WithError(err).Panic("Cannot bind output queue B to exchange B")
	}

	log.Info("Application is ready to run")

	processor.New(inputQueueA, outputQueueA, inputQueueB, outputQueueB, database.D{}).Run(ctx)
}
