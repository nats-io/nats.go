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
	if _, err := js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:      "FULFILLMENT",
		Subjects:  []string{"fulfill.>"},
		Retention: jetstream.WorkQueuePolicy,
	}); err != nil {
		panic(err)
	}

	// NATS-DOC-START
	// Load FULFILLMENT and take its current config.
	stream, err := js.Stream(ctx, "FULFILLMENT")
	if err != nil {
		panic(err)
	}
	cfg := stream.CachedInfo().Config

	// Try to turn the WorkQueue stream into a Limits stream by flipping
	// retention and pushing the update back.
	cfg.Retention = jetstream.LimitsPolicy
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		// The server refuses: a stream's retention policy is fixed for its
		// lifetime (err 10052: stream configuration update can not change
		// retention policy to/from workqueue). To change it, recreate the stream.
		fmt.Printf("retention switch rejected: %v\n", err)
		return
	}
	fmt.Println("update unexpectedly succeeded")
	// NATS-DOC-END
}
