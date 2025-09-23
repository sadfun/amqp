# ⚡️ amqp

The most popular package for working with RabbitMQ (or [LavinMQ](https://lavinmq.com/)) is [amqp091-go](https://github.com/rabbitmq/amqp091-go). 
However, the README says:
> ### Non-goals
> Things not intended to be supported.
> 
> * Auto reconnect and re-synchronization of client and server topologies.
Reconnection would require understanding the error paths when the topology cannot be declared on reconnect. This would require a new set of types and code paths that are best suited at the call-site of this package. AMQP has a dynamic topology that needs all peers to agree. If this doesn't happen, the behavior is undefined. Instead of producing a possible interface with undefined behavior, this package is designed to be simple for the caller to implement the necessary connection-time topology declaration so that reconnection is trivial and encapsulated in the caller's application code.

This package aims to fill that gap by providing a thin wrapper around `amqp091-go` that adds:
- Automatic reconnection and topology re-declaration
- Slightly more typed API
- Out-of-the-box concurrency & back-off strategies


## Quick Start

```bash
go get -u github.com/sadfun/amqp
```

```go
package main

import (
    "context"
    "fmt"
    "log"
    "time"

    "github.com/rabbitmq/amqp091-go"
    "github.com/sadfun/amqp"
)

func main() {
    ctx := context.Background()
    
    // Define topology (exchanges, queues, bindings) - this runs on every reconnect
    topology := func(ch *amqp091.Channel) error {
        // Declare exchange
        if err := ch.ExchangeDeclare("events", "direct", true, false, false, false, nil); err != nil {
            return err
        }
        // Declare queue
        if _, err := ch.QueueDeclare("notifications", true, false, false, false, nil); err != nil {
            return err
        }
        // Bind queue to exchange
        return ch.QueueBind("notifications", "user.signup", "events", false, nil)
    }
    
    // Create session with backoff configuration for reconnects
    session := amqp.New("amqp://guest:guest@localhost:5672/", topology, amqp.Backoff{
        Initial:    100 * time.Millisecond,
        Max:        5 * time.Second,
        Multiplier: 2.0,
        Jitter:     0.1,
    })
    
    // Start the session (handles connections in background)
    session.Start(ctx)
    defer session.Close()
    
    // Wait for initial connection
    if err := session.WaitReady(ctx); err != nil {
        log.Fatal("Failed to connect:", err)
    }
    
    // Start a consumer (automatically resubscribes on reconnects)
    go func() {
        err := session.Consume(ctx, "notifications", "my-consumer", amqp.ConsumeOptions{
            AutoAck:        false, // manual ack for reliability
            Concurrency:    3,     // process up to 3 messages concurrently
            RequeueOnError: true,  // requeue failed messages
        }, func(ctx context.Context, delivery amqp091.Delivery) error {
            fmt.Printf("Received: %s\n", string(delivery.Body))
            // Simulate processing
            time.Sleep(100 * time.Millisecond)
            return nil // nil = ACK, error = NACK
        })
        if err != nil {
            log.Printf("Consumer error: %v", err)
        }
    }()
    
    // Publish some messages (automatically retries on connection failures)
    for i := 0; i < 5; i++ {
        message := amqp091.Publishing{
            ContentType: "text/plain",
            Body:        []byte(fmt.Sprintf("Hello World #%d", i+1)),
            Timestamp:   time.Now(),
        }
        
        err := session.Publish(ctx, "events", "user.signup", false, false, message)
        if err != nil {
            log.Printf("Publish failed: %v", err)
        } else {
            fmt.Printf("Published message #%d\n", i+1)
        }
        
        time.Sleep(500 * time.Millisecond)
    }
    
    // Let consumer process messages
    time.Sleep(3 * time.Second)
    fmt.Println("Done! The consumer will automatically reconnect if RabbitMQ restarts.")
}
```