package amqp

import (
	"context"
	"errors"
	"sync"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ConsumeOptions struct {
	AutoAck        bool
	Exclusive      bool
	NoLocal        bool
	Args           amqp.Table
	Concurrency    int  // number of handler workers; default 1
	RequeueOnError bool // when handler returns error and AutoAck==false
}

// Handler is invoked for each delivery. Return nil to ack (when AutoAck==false),
// or an error to Nack (with RequeueOnError). If AutoAck==true, your return value
// is ignored for acking purposes.
type Handler func(context.Context, amqp.Delivery) error

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
			deliveries <-chan amqp.Delivery
			err        error
			cancelC    <-chan string
			closeC     <-chan *amqp.Error
		)

		// Establish the consumer on the live channel and hook cancel/close notifs.
		err = s.WithChannel(ctx, func(ch *amqp.Channel) error {
			cancelC = ch.NotifyCancel(make(chan string, 1))
			closeC = ch.NotifyClose(make(chan *amqp.Error, 1))
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
						if err := h(ctx, d); opt.AutoAck {
							// Broker already acked; handler controls side effects only.
							continue
						} else if err == nil {
							_ = d.Ack(false)
						} else {
							_ = d.Nack(false, opt.RequeueOnError)
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
