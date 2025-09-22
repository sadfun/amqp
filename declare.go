// Package amqp wraps RabbitMQ AMQP 0-9-1 declare helpers.
package amqp

import "github.com/rabbitmq/amqp091-go"

// ExchangeOptions maps to AMQP 0-9-1 exchange.declare.
type ExchangeOptions struct {
	Kind       string        // Exchange type: "direct" | "fanout" | "topic" | "headers".
	Durable    bool          // Survive broker restart.
	AutoDelete bool          // Delete when no queues are bound (no longer in use).
	Internal   bool          // Not directly publishable; for exchange-to-exchange bindings only.
	args       amqp091.Table // Optional arguments (e.g., "alternate-exchange").
}

// ExchangeDeclare issues AMQP exchange.declare with the given options.
func ExchangeDeclare(ch *amqp091.Channel, name string, options ExchangeOptions) error {
	if options.Kind == "" {
		options.Kind = "direct"
	}

	return ch.ExchangeDeclare(name, options.Kind, options.Durable, options.AutoDelete, options.Internal, false, options.args)
}

// QueueOptions maps to AMQP 0-9-1 queue.declare.
type QueueOptions struct {
	Name       string        // Queue name (empty => server-generated name).
	Durable    bool          // Survive broker restart (messages require delivery-mode persistent).
	AutoDelete bool          // Delete when last consumer unsubscribes.
	Exclusive  bool          // Scope to this connection; delete when it closes.
	args       amqp091.Table // Optional arguments (e.g., "x-message-ttl", "x-dead-letter-exchange").
}

// QueueDeclare issues AMQP queue.declare with the given options.
func QueueDeclare(ch *amqp091.Channel, name string, options QueueOptions) (amqp091.Queue, error) {
	return ch.QueueDeclare(name, options.Durable, options.AutoDelete, options.Exclusive, false, options.args)
}
