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
	// Publish the same message twice with a message ID. The stream uses the ID
	// to detect duplicates within its dedupe window.
	first, err := js.Publish(ctx, "orders.created", []byte(payload), jetstream.WithMsgID("ord_8w2k-created"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("First:  sequence %d, duplicate %t\n", first.Sequence, first.Duplicate)

	// Re-publish with the same ID. The server recognizes it and reports the
	// original sequence instead of storing the message again.
	second, err := js.Publish(ctx, "orders.created", []byte(payload), jetstream.WithMsgID("ord_8w2k-created"))
	if err != nil {
		panic(err)
	}
	fmt.Printf("Second: sequence %d, duplicate %t\n", second.Sequence, second.Duplicate)
	// NATS-DOC-END
}
