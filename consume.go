package amqp

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/rabbitmq/amqp091-go"
)

type ConsumeOptions struct {
	AutoAck     bool
	Exclusive   bool
	NoLocal     bool
	Args        amqp091.Table
	Concurrency int // number of handler workers; default 1
}

type Delivery amqp091.Delivery

type ConsumeResult int

const (
	Ack ConsumeResult = iota
	NackRequeue
	NackDiscard
)

// Handler is invoked for each delivery. It must return a ConsumeResult,
// which controls how the delivery is acknowledged:
// - Ack: acknowledge the delivery (= d.Ack(false)).
// - NackRequeue: negative-acknowledge the delivery and requeue it (= d.Nack(false, true)).
// - NackDiscard: negative-acknowledge the delivery and discard it (= d.Nack(false, false)).
// You must not Ack/Nack the delivery yourself.
type Handler func(context.Context, Delivery) ConsumeResult

// Consume blocks until ctx is done, (re)subscribing after reconnects.
// It never declares topology, do that in your TopologyFunc.
func (s *Session) Consume(ctx context.Context, queue, consumerTag string, opt ConsumeOptions, h Handler) error {
	if opt.Concurrency <= 0 {
		opt.Concurrency = 1
	}
	for {
		// Wait until we have a channel/session.
		if err := s.WaitReady(ctx); err != nil {
			return err
		}

		var (
			deliveries <-chan amqp091.Delivery
			err        error
			cancelC    <-chan string
			closeC     <-chan *amqp091.Error
		)

		// Establish the consumer on the live channel and hook cancel/close notifs.
		err = s.WithChannel(ctx, func(ch *amqp091.Channel) error {
			cancelC = ch.NotifyCancel(make(chan string, 1))
			closeC = ch.NotifyClose(make(chan *amqp091.Error, 1))
			d, e := ch.Consume(
				queue,
				consumerTag,
				opt.AutoAck,
				opt.Exclusive,
				opt.NoLocal,
				false, // noWait
				opt.Args,
			)
			if e != nil {
				return e
			}
			deliveries = d
			return nil
		})
		if err != nil {
			// Likely disconnected or consume failed; loop to retry.
			continue
		}

		// Spin up workers to handle deliveries.
		wg := new(sync.WaitGroup)
		stop := make(chan struct{})
		// Signal used to detect that 'deliveries' closed (e.g., channel/conn dropped).
		resub := make(chan struct{})
		var once sync.Once

		startWorker := func() {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for {
					select {
					case <-ctx.Done():
						return
					case <-stop:
						return
					case d, ok := <-deliveries:
						if !ok {
							// The consume stream ended: trigger resubscription.
							once.Do(func() { close(resub) })
							return
						}
						if h == nil {
							if !opt.AutoAck {
								_ = d.Ack(false)
							}
							continue
						}

						result := h(ctx, Delivery(d))
						if opt.AutoAck {
							// Broker already acked; handler controls side effects only.
							continue
						}

						switch result {
						case Ack:
							_ = d.Ack(false)
						case NackRequeue:
							_ = d.Nack(false, true)
						case NackDiscard:
							_ = d.Nack(false, false)
						default:
							slog.Warn("Unknown ConsumeResult from handler; Nacking with requeue", "result", result)
							_ = d.Nack(false, true)
						}
					}
				}
			}()
		}
		for i := 0; i < opt.Concurrency; i++ {
			startWorker()
		}

		// Wait for one of:
		// - context cancellation,
		// - session closed,
		// - channel/consumer canceled/closed,
		// - deliveries stream closed (workers signal via resub).
		var retErr error
		select {
		case <-ctx.Done():
			retErr = ctx.Err()
		case <-s.closedCh:
			retErr = errors.New("session closed")
		case <-closeC:
			// Channel closed; resubscribe.
		case <-cancelC:
			// Consumer canceled; resubscribe.
		case <-resub:
			// Deliveries closed; resubscribe.
		}

		// Clean up workers before either returning or resubscribing.
		close(stop)
		wg.Wait()

		// If retErr is set, we're done; otherwise loop to resubscribe.
		if retErr != nil {
			return retErr
		}
		// Loop continues -> WaitReady, then re-Consume.
	}
}
