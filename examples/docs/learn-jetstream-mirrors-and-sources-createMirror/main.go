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
	// Create ORDERS-ARCHIVE as a read-only mirror of ORDERS. A mirror takes
	// no subjects of its own; it follows the upstream stream.
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:   "ORDERS-ARCHIVE",
		Mirror: &jetstream.StreamSource{Name: "ORDERS"},
	})
	if err != nil {
		panic(err)
	}

	// Confirm: the new stream mirrors ORDERS.
	cfg := stream.CachedInfo().Config
	fmt.Printf("Created mirror %s of %s\n", cfg.Name, cfg.Mirror.Name)
	// NATS-DOC-END
}
