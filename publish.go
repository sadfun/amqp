package amqp

import (
	"context"
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Publish publishes with automatic retry across reconnects. It retries on
// channel/connection closed errors until ctx is done.
func (s *Session) Publish(ctx context.Context, exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	for {
		if err := s.WaitReady(ctx); err != nil {
			return err
		}
		ch := s.channel()
		if ch == nil {
			continue // race with reconnect; loop and try again
		}
		if err := ch.PublishWithContext(ctx, exchange, key, mandatory, immediate, msg); err != nil {
			// Retry on transport-level closures; otherwise return.
			if errors.Is(err, amqp.ErrClosed) || isConnClosed(err) {
				continue
			}
			return err
		}
		return nil
	}
}
