package exchange

import (
	"context"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type exchange struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	name       string
	kind       string
}

func New(connectionURL, name, kind string) (*exchange, error) {
	conn, err := amqp.Dial(connectionURL)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		name,  // name
		kind,  // type
		true,  // durable
		false, // auto-deleted
		false, // internal
		false, // no-wait
		nil,   // arguments
	)
	if err != nil {
		return nil, err
	}

	return &exchange{connection: conn, channel: ch, name: name, kind: kind}, nil
}

func (e *exchange) Publish(ctx context.Context, routingKey string, msg []byte) error {
	data := amqp.Publishing{
		DeliveryMode:    amqp.Transient,
		Timestamp:       time.Now(),
		Body:            msg,
		ContentEncoding: "application/json",
	}

	return e.channel.PublishWithContext(ctx, e.name, routingKey, true, false, data)
}
