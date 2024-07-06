package processor

import (
	"arkis_test/queue"
	"context"

	log "github.com/sirupsen/logrus"
)

type Queue interface {
	Consume(ctx context.Context) (<-chan queue.Delivery, error)
}

type Exchange interface {
	Publish(ctx context.Context, routingKey string, msg []byte) error
}

type Database interface {
	Get([]byte) (string, error)
}

type processor struct {
	input      Queue
	exchange   Exchange
	routingKey string
	database   Database
}

func New(input Queue, exchange Exchange, routingKey string, db Database) processor {
	return processor{input: input, exchange: exchange, routingKey: routingKey, database: db}
}

func (p processor) Run(ctx context.Context) error {
	deliveries, err := p.input.Consume(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case delivery := <-deliveries:
			if err := p.process(ctx, delivery); err != nil {
				return err
			}
		}
	}
}

func (p processor) process(ctx context.Context, delivery queue.Delivery) error {
	log.WithField("delivery", string(delivery.Body)).Info("Processing the delivery")

	data, err := p.database.Get(delivery.Body)
	if err != nil {
		return err
	}

	log.WithField("result", string(data)).Info("Processed the delivery")

	return p.exchange.Publish(ctx, p.routingKey, []byte(data))
}
