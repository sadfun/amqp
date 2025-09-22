package amqp

import (
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"testing"
	"time"

	tc "github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/rabbitmq/amqp091-go"
)

// ---- Test harness: RabbitMQ via Testcontainers ----

type rabbitEnv struct {
	container tc.Container
	amqpURL   string
}

func startRabbitMQ(t *testing.T) *rabbitEnv {
	t.Helper()

	ctx := context.Background()

	req := tc.ContainerRequest{
		Image:        getenv("TEST_RABBIT_IMAGE", "rabbitmq:4.1-management-alpine"),
		ExposedPorts: []string{"5672/tcp", "15672/tcp"},
		Env: map[string]string{
			"RABBITMQ_DEFAULT_USER": "guest",
			"RABBITMQ_DEFAULT_PASS": "guest",
		},
		WaitingFor: wait.ForAll(
			wait.ForListeningPort("5672/tcp"),
			wait.ForLog("Server startup complete"),
		).WithDeadline(2 * time.Minute),
	}

	c, err := tc.GenericContainer(ctx, tc.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("start container: %v", err)
	}

	host, err := c.Host(ctx)
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("container host: %v", err)
	}
	p, err := c.MappedPort(ctx, "5672/tcp")
	if err != nil {
		_ = c.Terminate(ctx)
		t.Fatalf("mapped port: %v", err)
	}
	url := fmt.Sprintf("amqp://guest:guest@%s:%s/", host, p.Port())

	t.Cleanup(func() {
		_ = c.Terminate(context.Background())
	})

	return &rabbitEnv{container: c, amqpURL: url}
}

func getenv(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// common topology used in tests (idempotent)
func testTopology(exchange, queue, rk string) TopologyFunc {
	return func(ch *amqp091.Channel) error {
		if err := ExchangeDeclare(ch, exchange, ExchangeOptions{
			Kind:    "direct",
			Durable: true,
		}); err != nil {
			return err
		}

		if _, err := QueueDeclare(ch, queue, QueueOptions{
			Durable: true,
		}); err != nil {
			return err
		}

		if err := ch.QueueBind(queue, rk, exchange, false, nil); err != nil {
			return err
		}

		// Friendly QoS for tests.
		if err := ch.Qos(10, 0, false); err != nil {
			return err
		}
		return nil
	}
}

// consume helper using the library’s Consume with a handler
func startConsumer(t *testing.T, s *Session, queue, tag string, out chan<- string) (context.CancelFunc, *sync.WaitGroup) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := s.Consume(ctx, queue, tag, ConsumeOptions{
			AutoAck:        false,
			Concurrency:    2,
			RequeueOnError: false,
		}, func(_ context.Context, d amqp091.Delivery) error {
			// Simulate small work
			out <- string(d.Body)
			return nil // ack
		})
		if err != nil && ctx.Err() == nil {
			// Only log unexpected errors (not context cancellation).
			log.Printf("consumer ended unexpectedly: %v", err)
		}
	}()

	return cancel, wg
}

// ---- Small test rig to reduce repetition ----

type rig struct {
	t   *testing.T
	env *rabbitEnv
	s   *Session

	ctx    context.Context
	cancel context.CancelFunc
}

func newRig(t *testing.T, topo TopologyFunc) *rig {
	t.Helper()

	env := startRabbitMQ(t)

	// Build session with common backoff + lifecycle wiring.
	s := New(env.amqpURL, topo, Backoff{
		Initial:    100 * time.Millisecond,
		Max:        2 * time.Second,
		Multiplier: 1.8,
		Jitter:     0.1,
	})

	ctx, cancel := context.WithCancel(context.Background())
	s.Start(ctx)

	// Cleanup always runs, regardless of test path.
	t.Cleanup(func() {
		cancel()
		s.Close()
	})

	// Block until ready with a reasonable deadline.
	if err := s.WaitReady(t.Context()); err != nil {
		t.Fatalf("session not ready: %v", err)
	}

	return &rig{
		t:      t,
		env:    env,
		s:      s,
		ctx:    ctx,
		cancel: cancel,
	}
}

// consumer spins a library consumer and wires cleanup.
func (r *rig) consumer(queue, tag string, buf int) (<-chan string, func()) {
	r.t.Helper()
	inbox := make(chan string, buf)
	stop, wg := startConsumer(r.t, r.s, queue, tag, inbox)

	cleanup := func() { stop(); wg.Wait() }
	r.t.Cleanup(cleanup)
	return inbox, cleanup
}

// publishN just forwards to the shared helper to keep call sites short.
func (r *rig) publishN(exchange, rk string, n int) {
	r.t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for i := 0; i < n; i++ {
		body := []byte(fmt.Sprintf("msg-%d", i))
		if err := r.s.Publish(ctx, exchange, rk, false, false, amqp091.Publishing{
			ContentType: "text/plain",
			Body:        body,
		}); err != nil {
			r.t.Fatalf("publish %d: %v", i, err)
		}
	}
}

// waitReceive forwards to the shared helper to keep timeouts consistent at call sites.
func (r *rig) waitReceive(inbox <-chan string, want int, d time.Duration) []string {
	r.t.Helper()
	got := make([]string, 0, want)
	deadline := time.NewTimer(d)
	defer deadline.Stop()

	for len(got) < want {
		select {
		case m := <-inbox:
			got = append(got, m)
		case <-deadline.C:
			r.t.Fatalf("timeout waiting for %d messages (got %d)", want, len(got))
		}
	}
	return got
}

// restartRabbit stops/starts the broker inside the container.
func (r *rig) restartRabbit() {
	r.t.Helper()
	r.t.Log("stopping RabbitMQ service to simulate outage…")
	if _, _, err := r.env.container.Exec(context.Background(), []string{"rabbitmqctl", "stop_app"}); err != nil {
		r.t.Fatalf("stop rabbitmq service: %v", err)
	}
	r.t.Log("starting RabbitMQ service again…")
	if _, _, err := r.env.container.Exec(context.Background(), []string{"rabbitmqctl", "start_app"}); err != nil {
		r.t.Fatalf("start rabbitmq service: %v", err)
	}
}

// ---- Tests (now much shorter/readable) ----

func TestSessionPublishConsumeBasic(t *testing.T) {
	const (
		ex = "ex.it.basic"
		q  = "q.it.basic"
		rk = "rk.basic"
	)

	r := newRig(t, testTopology(ex, q, rk))

	inbox, _ := r.consumer(q, "c1", 64)
	r.publishN(ex, rk, 10)

	got := r.waitReceive(inbox, 10, 15*time.Second)
	if len(got) != 10 {
		t.Fatalf("expected 10 messages, got %d", len(got))
	}
}

func TestSessionReconnectPublishRetry(t *testing.T) {
	const (
		ex = "ex.it.retry"
		q  = "q.it.retry"
		rk = "rk.retry"
	)

	r := newRig(t, testTopology(ex, q, rk))

	inbox, _ := r.consumer(q, "c2", 128)

	// Pre-restart messages.
	r.publishN(ex, rk, 5)

	// Restart the broker mid-flight.
	r.restartRabbit()

	// While broker is down/coming up, keep publishing; Publish should block/retry and succeed.
	const more = 7
	done := make(chan error, 1)
	go func() {
		defer close(done)
		ctxPub, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		for i := 0; i < more; i++ {
			body := []byte(fmt.Sprintf("after-%d", i))
			if err := r.s.Publish(ctxPub, ex, rk, false, false, amqp091.Publishing{
				ContentType: "text/plain",
				Body:        body,
			}); err != nil {
				done <- fmt.Errorf("publish after restart %d: %w", i, err)
				return
			}
		}
		done <- nil
	}()
	if err := <-done; err != nil {
		t.Fatalf("publish during reconnect failed: %v", err)
	}

	// Expect pre + post restart messages.
	want := 5 + more
	got := r.waitReceive(inbox, want, 60*time.Second)
	if len(got) != want {
		t.Fatalf("expected %d messages after restart, got %d", want, len(got))
	}
}

func TestSessionConsumeResubscribeAfterRestart(t *testing.T) {
	const (
		ex = "ex.it.resub"
		q  = "q.it.resub"
		rk = "rk.resub"
	)

	r := newRig(t, testTopology(ex, q, rk))

	inbox, stop := r.consumer(q, "c3", 256)
	defer stop()

	// Initial deliveries.
	r.publishN(ex, rk, 5)
	_ = r.waitReceive(inbox, 5, 20*time.Second)

	// Restart; consumer stream should be re-established by the library.
	r.restartRabbit()

	// More deliveries post-resubscribe.
	r.publishN(ex, rk, 8)
	_ = r.waitReceive(inbox, 8, 60*time.Second)
}

func TestTopologyFailureThenSuccess(t *testing.T) {
	const (
		ex = "ex.it.topo"
		q  = "q.it.topo"
		rk = "rk.topo"
	)

	var failOnce sync.Once
	topo := func(ch *amqp091.Channel) error {
		var retErr error
		failOnce.Do(func() { retErr = fmt.Errorf("intentional topology failure") })
		if retErr != nil {
			return retErr
		}
		return testTopology(ex, q, rk)(ch)
	}

	r := newRig(t, topo)

	// Sanity: publish/consume one message via basic.get.
	body := "hello-topology"
	if err := r.s.Publish(context.Background(), ex, rk, false, false, amqp091.Publishing{
		ContentType: "text/plain",
		Body:        []byte(body),
	}); err != nil {
		t.Fatalf("publish after topology recovery: %v", err)
	}

	var got string
	err := r.s.WithChannel(context.Background(), func(ch *amqp091.Channel) error {
		d, ok, e := ch.Get(q, true)
		if e != nil {
			return e
		}
		if !ok {
			return fmt.Errorf("queue was empty")
		}
		got = string(d.Body)
		return nil
	})
	if err != nil {
		t.Fatalf("basic.get: %v", err)
	}
	if got != body {
		t.Fatalf("expected %q, got %q", body, got)
	}
}

func TestSessionClose(t *testing.T) {
	// no topology needed for this smoke
	r := newRig(t, nil)

	// Already ready thanks to newRig.
	r.s.Close()

	// WaitReady should fail once closed.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := r.s.WaitReady(ctx); err == nil {
		t.Fatalf("expected WaitReady to fail after Close()")
	}
}
