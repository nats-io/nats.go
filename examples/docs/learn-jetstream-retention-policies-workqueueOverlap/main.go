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

	// Make sure the FULFILLMENT WorkQueue stream exists.
	stream, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "FULFILLMENT",
		Subjects:  []string{"fulfill.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		panic(err)
	}

	// NATS-DOC-START
	// One unfiltered consumer covers every order in the queue.
	if _, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "shippers",
		AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		panic(err)
	}
	fmt.Println("created unfiltered consumer: shippers")

	// A WorkQueue stream delivers each message to exactly one consumer, so it
	// refuses a second consumer whose reach overlaps the first. Two unfiltered
	// consumers both cover fulfill.> , so this is rejected
	// (err 10099: multiple non-filtered consumers not allowed on workqueue stream).
	if _, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "eu-shippers",
		AckPolicy: jetstream.AckExplicitPolicy,
	}); err != nil {
		fmt.Printf("second unfiltered consumer rejected: %v\n", err)
	}

	// Drop the catch-all consumer so we can split the queue by region instead.
	if err := stream.DeleteConsumer(ctx, "shippers"); err != nil {
		panic(err)
	}

	// Filtered consumers are allowed as long as their subjects don't overlap.
	// us-shippers takes fulfill.us, eu-shippers takes fulfill.eu, and each
	// order still goes to exactly one worker.
	if _, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "us-shippers",
		FilterSubject: "fulfill.us",
		AckPolicy:     jetstream.AckExplicitPolicy,
	}); err != nil {
		panic(err)
	}
	if _, err := stream.CreateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:       "eu-shippers",
		FilterSubject: "fulfill.eu",
		AckPolicy:     jetstream.AckExplicitPolicy,
	}); err != nil {
		panic(err)
	}
	fmt.Println("created filtered consumers: us-shippers (fulfill.us), eu-shippers (fulfill.eu)")
	// NATS-DOC-END
}
