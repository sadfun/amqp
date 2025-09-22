// Package amqp provides a tiny reconnection helper around amqp091-go.
//
// It reconnects the TCP/AMQP transport and re-opens a channel with basic
// exponential backoff + jitter. Importantly, it does NOT attempt to guess or
// rebuild your AMQP topology. Instead you provide a TopologyFunc that is called
// every time a new channel is established, where you can idempotently declare
// exchanges, queues, bindings, QoS, confirms, etc.
//
// This design follows the library’s guidance: AMQP topology must be owned by
// the caller to avoid undefined behavior on reconnect.
package amqp

import (
	"context"
	"errors"
	"math"
	"math/rand"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// TopologyFunc is invoked after a new connection+channel are opened,
// before the session is marked ready. Return an error to reject this
// connection attempt (the session will retry).
type TopologyFunc func(ch *amqp.Channel) error

// Backoff controls reconnect delays.
type Backoff struct {
	Initial    time.Duration // default: 500ms
	Max        time.Duration // default: 30s
	Multiplier float64       // default: 2.0
	Jitter     float64       // 0..1 fraction of delay to randomize, default: 0.2
}

func (b Backoff) norm() Backoff {
	nb := b
	if nb.Initial <= 0 {
		nb.Initial = 500 * time.Millisecond
	}
	if nb.Max <= 0 {
		nb.Max = 30 * time.Second
	}
	if nb.Multiplier < 1.1 {
		nb.Multiplier = 2.0
	}
	if nb.Jitter < 0 || nb.Jitter > 1 {
		nb.Jitter = 0.2
	}
	return nb
}

// Session maintains a resilient AMQP connection and a single channel.
type Session struct {
	url      string
	topo     TopologyFunc
	backoff  Backoff
	closedCh chan struct{}

	mu    sync.RWMutex
	conn  *amqp.Connection
	ch    *amqp.Channel
	ready chan struct{} // closed when ready, replaced on disconnect

	wg sync.WaitGroup
}

// New creates an unstarted Session.
// Provide a TopologyFunc that DECLARES your topology idempotently (exchanges,
// queues, bindings, QoS, confirm mode, etc.). It will be run on every connect.
func New(url string, topo TopologyFunc, backoff Backoff) *Session {
	return &Session{
		url:      url,
		topo:     topo,
		backoff:  backoff.norm(),
		ready:    make(chan struct{}), // not ready until Start runs a connect
		closedCh: make(chan struct{}),
	}
}

// Start spawns the background reconnect loop.
// It returns immediately; use WaitReady or Publish/WithChannel which block
// until a connection is available.
func (s *Session) Start(ctx context.Context) {
	s.wg.Add(1)
	go s.loop(ctx)
}

// Close stops the session and closes underlying AMQP objects.
func (s *Session) Close() {
	select {
	case <-s.closedCh:
		// already closed
	default:
		close(s.closedCh)
	}
	s.mu.Lock()
	ch := s.ch
	conn := s.conn
	s.ch = nil
	s.conn = nil
	// Create a fresh, unclosed ready channel so future WaitReady will block
	s.ready = make(chan struct{})
	s.mu.Unlock()

	if ch != nil {
		_ = ch.Close()
	}
	if conn != nil {
		_ = conn.Close()
	}
	s.wg.Wait()
}

// Ready returns a channel that is closed whenever the session is currently ready.
// NOTE: If a disconnect happens after you grabbed this channel, it will stay
// closed. Prefer WaitReady/Publish/WithChannel for robust flows.
func (s *Session) Ready() <-chan struct{} {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ready
}

// WaitReady blocks until the session has an open channel (or the context ends).
func (s *Session) WaitReady(ctx context.Context) error {
	ready := s.Ready()
	select {
	case <-ready:
		return nil
	case <-s.closedCh:
		return errors.New("session closed")
	case <-ctx.Done():
		return ctx.Err()
	}
}

// WithChannel gives you safe access to the live channel for RPCs (e.g. declare,
// consume, txns, confirms interactions, etc.). It will block until the channel
// is available or ctx is done. If the channel closes mid-operation, your fn will
// see amqp.ErrClosed; you may choose to retry by calling WithChannel again.
func (s *Session) WithChannel(ctx context.Context, fn func(*amqp.Channel) error) error {
	if err := s.WaitReady(ctx); err != nil {
		return err
	}
	ch := s.channel()
	if ch == nil {
		return amqp.ErrClosed
	}
	return fn(ch)
}

// loop keeps the session connected.
func (s *Session) loop(ctx context.Context) {
	defer s.wg.Done()

	bo := s.backoff
	delay := bo.Initial

	for {
		// Stop requested?
		select {
		case <-s.closedCh:
			return
		default:
		}

		// Try to connect.
		conn, ch, err := s.openOnce()
		if err != nil {
			// Backoff and retry.
			sleepCtx := context.WithValue(context.Background(), "backoff", true)
			sleepCtx, cancel := context.WithTimeout(sleepCtx, jitter(delay, bo.Jitter))
			select {
			case <-s.closedCh:
				cancel()
				return
			case <-sleepCtx.Done():
				cancel()
			}
			// Exponential growth capped at Max.
			delay = time.Duration(math.Min(float64(bo.Max), float64(delay)*bo.Multiplier))
			continue
		}
		// Connected; reset delay.
		delay = bo.Initial

		// Mark ready.
		s.mu.Lock()
		s.conn = conn
		s.ch = ch
		// Close the current ready channel (which may be a fresh, unclosed one)
		// and replace it with a new already-closed channel for future Waiters.
		oldReady := s.ready
		// We close oldReady to signal "ready now".
		select {
		case <-oldReady:
			// already closed
		default:
			close(oldReady)
		}
		s.mu.Unlock()

		// Watch for closure.
		connClosed := conn.NotifyClose(make(chan *amqp.Error, 1))
		chClosed := ch.NotifyClose(make(chan *amqp.Error, 1))

		// Block until something closes or we are told to stop.
		select {
		case <-s.closedCh:
			_ = ch.Close()
			_ = conn.Close()
			return
		case <-connClosed:
		case <-chClosed:
		}

		// Mark not-ready & cleanup, then loop to reconnect.
		s.mu.Lock()
		_ = ch.Close()
		_ = conn.Close()
		s.conn = nil
		s.ch = nil
		// Replace ready with a NEW, unclosed channel so waiters block until next connect.
		s.ready = make(chan struct{})
		s.mu.Unlock()
	}
}

// openOnce dials, opens a channel, and runs topology setup. If topology fails,
// it will tear down and return the error.
func (s *Session) openOnce() (*amqp.Connection, *amqp.Channel, error) {
	conn, err := amqp.Dial(s.url)
	if err != nil {
		return nil, nil, err
	}
	ch, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return nil, nil, err
	}
	if s.topo != nil {
		if err := s.topo(ch); err != nil {
			_ = ch.Close()
			_ = conn.Close()
			return nil, nil, err
		}
	}
	return conn, ch, nil
}

func (s *Session) channel() *amqp.Channel {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ch
}

func jitter(d time.Duration, frac float64) time.Duration {
	if frac <= 0 {
		return d
	}
	delta := int64(float64(d) * frac)
	if delta <= 0 {
		return d
	}
	return d + time.Duration(rand.Int63n(2*delta)-delta)
}

// isConnClosed heuristically recognizes connection closed errors that aren't
// wrapped as amqp.ErrClosed by the client.
func isConnClosed(err error) bool {
	// The amqp library uses amqp.ErrClosed but some paths may wrap or deliver
	// *amqp.Error. Treat both as retryable transport closure.
	var aerr *amqp.Error
	return errors.Is(err, amqp.ErrClosed) || errors.As(err, &aerr)
}
