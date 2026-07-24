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
	// Bind to the durable "shipping" consumer created earlier.
	cons, err := js.Consumer(ctx, "ORDERS", "shipping")
	if err != nil {
		panic(err)
	}

	// Fetch a single message from the consumer.
	msg, err := cons.Next()
	if err != nil {
		panic(err)
	}
	fmt.Printf("Received on %s: %s\n", msg.Subject(), string(msg.Data()))

	// Acknowledge so the server advances the consumer past this message.
	if err := msg.Ack(); err != nil {
		panic(err)
	}
	// NATS-DOC-END
}
