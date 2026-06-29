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
	// Switch ORDERS to Discard New. Discard New never drops messages already
	// stored, so capping it at one message leaves the existing orders in place
	// and puts the stream instantly over its limit. The next publish is
	// rejected rather than evicting an older order.
	s, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		panic(err)
	}
	cfg := s.CachedInfo().Config
	cfg.Discard = jetstream.DiscardNew
	cfg.MaxMsgs = 1
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		panic(err)
	}

	// This publish hits the full stream and fails with "maximum messages
	// exceeded" instead of succeeding silently. Handle it in the publisher.
	if _, err := js.Publish(ctx, "orders.created", []byte(`{"order_id":"ord_8w2k"}`)); err != nil {
		fmt.Printf("publish rejected: %v\n", err)
	}

	// Put ORDERS back: Discard Old, no message cap (age and byte limits stay).
	cfg.Discard = jetstream.DiscardOld
	cfg.MaxMsgs = -1
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		panic(err)
	}
	// NATS-DOC-END
}
