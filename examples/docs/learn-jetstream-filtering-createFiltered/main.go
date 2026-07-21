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
	// Create a durable pull consumer named "analytics" on the ORDERS stream.
	// FilterSubject narrows delivery to "orders.shipped", so the consumer never
	// sees "orders.created" messages even though the stream stores both.
	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:       "analytics",
		FilterSubject: "orders.shipped",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Created consumer: %s\n", cons.CachedInfo().Name)
	fmt.Printf("Filter: %s\n", cons.CachedInfo().Config.FilterSubject)

	// Fetch up to 5 messages with a short expiry. Only "orders.shipped"
	// messages come back; the filter excludes everything else.
	msgs, err := cons.Fetch(5, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		panic(err)
	}
	for msg := range msgs.Messages() {
		fmt.Printf("Received on %s\n", msg.Subject())
		msg.Ack()
	}
	if err := msgs.Error(); err != nil {
		panic(err)
	}
	// NATS-DOC-END
}
