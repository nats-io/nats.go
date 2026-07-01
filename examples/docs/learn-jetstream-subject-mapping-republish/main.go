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

	// Look up the ORDERS stream.
	stream, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		panic(err)
	}

	// NATS-DOC-START
	// Read the current config, add a republish rule, and push the update.
	// Every message stored on "orders.>" is also echoed live to "dash.orders.>"
	// so a dashboard can subscribe without touching the stream.
	cfg := stream.CachedInfo().Config
	cfg.RePublish = &jetstream.RePublish{
		Source:      "orders.>",
		Destination: "dash.orders.>",
		// HeadersOnly: true, // republish only the headers, not the body
	}

	updated, err := js.UpdateStream(ctx, cfg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Republish destination: %s\n", updated.CachedInfo().Config.RePublish.Destination)
	// NATS-DOC-END
}
