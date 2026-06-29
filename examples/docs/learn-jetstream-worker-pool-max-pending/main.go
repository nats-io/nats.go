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

	// Create a JetStream context.
	js, err := jetstream.New(nc)
	if err != nil {
		panic(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// NATS-DOC-START
	// Raise MaxAckPending so a larger pool can hold more orders in progress at
	// once. The cap is shared across the whole "shipping" consumer, not per
	// worker, so size it to at least your worker count. CreateOrUpdateConsumer
	// updates the existing consumer in place.
	_, err = js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:       "shipping",
		AckPolicy:     jetstream.AckExplicitPolicy,
		MaxAckPending: 5000,
	})
	if err != nil {
		panic(err)
	}
	fmt.Println("shipping MaxAckPending set to 5000")
	// NATS-DOC-END
}
