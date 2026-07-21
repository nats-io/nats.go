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
	// Create a durable pull consumer named "analytics-typo" on the ORDERS
	// stream. The filter "orders.shiped" has a typo (one "p"), so it matches
	// no subject the stream actually stores.
	cons, err := js.CreateOrUpdateConsumer(ctx, "ORDERS", jetstream.ConsumerConfig{
		Durable:       "analytics-typo",
		FilterSubject: "orders.shiped",
		DeliverPolicy: jetstream.DeliverAllPolicy,
		AckPolicy:     jetstream.AckExplicitPolicy,
	})
	if err != nil {
		panic(err)
	}

	// Fetch with a short expiry. The fetch succeeds with no error, but the
	// channel yields zero messages because the filter matched no stored subject.
	msgs, err := cons.Fetch(5, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		panic(err)
	}
	count := 0
	for range msgs.Messages() {
		count++
	}
	if err := msgs.Error(); err != nil {
		panic(err)
	}

	// A wrong filter fails silently: the pull returned nothing, and no error
	// was raised. The consumer is healthy, it just never matches a message.
	fmt.Printf("Received %d messages: the filter matched no stored subject, so the pull returned nothing (no error).\n", count)
	// NATS-DOC-END
}
