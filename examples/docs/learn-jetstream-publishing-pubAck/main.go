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
	// Publish one order and inspect the PubAck the server returns.
	ack, err := js.Publish(ctx, "orders.created", []byte(payload))
	if err != nil {
		panic(err)
	}

	// The PubAck confirms which stream stored the message, at what sequence,
	// and whether it was treated as a duplicate.
	fmt.Printf("Stream:    %s\n", ack.Stream)
	fmt.Printf("Sequence:  %d\n", ack.Sequence)
	fmt.Printf("Duplicate: %t\n", ack.Duplicate)
	// NATS-DOC-END
}
