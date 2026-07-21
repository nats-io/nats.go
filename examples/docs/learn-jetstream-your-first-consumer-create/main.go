package main

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	// Connect to the NATS server.
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// Create a JetStream context (the stream manager).
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// NATS-DOC-START
	// Create a durable pull consumer named "shipping" on the ORDERS stream.
	// DeliverAllPolicy starts from the first message in the stream, and
	// AckExplicitPolicy means every message must be acknowledged by hand.
	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:       "shipping",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		panic(err)
	}

	// Confirm the consumer is ready.
	fmt.Printf("Created consumer: %s\n", cons.CachedInfo().Name)
	// NATS-DOC-END
}
