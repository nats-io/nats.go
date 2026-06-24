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
	// Publish three order messages into the ORDERS stream. Each Publish blocks
	// until the server replies with a PubAck confirming where it was stored.
	orders := []struct {
		subject string
		payload string
	}{
		{"orders.created", `{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}`},
		{"orders.created", `{"order_id":"ord_2zr9","customer":"globex","total_cents":7800,"ts":"2026-05-22T10:14:25Z"}`},
		{"orders.shipped", `{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:31Z"}`},
	}

	for _, order := range orders {
		ack, err := js.Publish(ctx, order.subject, []byte(order.payload))
		if err != nil {
			panic(err)
		}
		fmt.Printf("Stored in %s, sequence %d\n", ack.Stream, ack.Sequence)
	}
	// NATS-DOC-END
}
