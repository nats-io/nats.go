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

	// Per-message TTL only works when the stream opts in with AllowMsgTTL.
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        "ORDERS",
		Subjects:    []string{"orders.>"},
		AllowMsgTTL: true,
	})
	if err != nil {
		panic(err)
	}

	// NATS-DOC-START
	// Publish with a 60-second TTL. WithMsgTTL sets the Nats-TTL header, so the
	// server deletes this single message 60s after it is stored, ahead of the
	// stream's MaxAge.
	ack, err := js.Publish(ctx, "orders.cancelled", []byte("order 42 cancelled"),
		jetstream.WithMsgTTL(60*time.Second))
	if err != nil {
		panic(err)
	}

	fmt.Printf("Stored in %s at sequence %d\n", ack.Stream, ack.Sequence)
	// NATS-DOC-END
}
