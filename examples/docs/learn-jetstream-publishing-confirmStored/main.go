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

	payload := `{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}`

	// NATS-DOC-START
	// Publish one order and confirm it was stored. A failed publish returns an
	// error rather than losing the message silently, so check err first.
	ack, err := js.Publish(ctx, "orders.created", []byte(payload))
	if err != nil {
		panic(err)
	}

	// The returned PubAck is proof the message is on disk in the stream.
	fmt.Printf("Stored in %s at sequence %d\n", ack.Stream, ack.Sequence)
	// NATS-DOC-END
}
