package processor

import (
	"arkis_test/queue"
	"context"

	log "github.com/sirupsen/logrus"
)

type Queue interface {
	Consume(ctx context.Context) (<-chan queue.Delivery, error)
	Publish(ctx context.Context, msg []byte) error
}

type Database interface {
	Get([]byte) (string, error)
}

type processor struct {
	inputA   Queue
	outputA  Queue
	inputB   Queue
	outputB  Queue
	database Database
}

func New(inputA, outputA, inputB, outputB Queue, db Database) processor {
	return processor{inputA, outputA, inputB, outputB, db}
}

func (p processor) Run(ctx context.Context) error {
	deliveriesA, err := p.inputA.Consume(ctx)
	if err != nil {
		return err
	}

	deliveriesB, err := p.inputB.Consume(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery := <-deliveriesA:
			if err := p.process(ctx, delivery, p.outputA); err != nil {
				return err
			}
		case delivery := <-deliveriesB:
			if err := p.process(ctx, delivery, p.outputB); err != nil {
				return err
			}
		}
	}
}

func (p processor) process(ctx context.Context, delivery queue.Delivery, output Queue) error {
	log.WithField("delivery", string(delivery.Body)).Info("Processing the delivery")

	data, err := p.database.Get(delivery.Body)
	if err != nil {
		return err
	}

	log.WithField("result", string(data)).Info("Processed the delivery")

	return output.Publish(ctx, []byte(data))
}
