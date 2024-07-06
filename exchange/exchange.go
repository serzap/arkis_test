package exchange

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type Exchange struct {
	connection *amqp.Connection
	channel    *amqp.Channel
	name       string
	kind       string
}

func New(connectionURL, name, kind string) (*Exchange, error) {
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

	return &Exchange{connection: conn, channel: ch, name: name, kind: kind}, nil
}
