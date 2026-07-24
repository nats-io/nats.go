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
	// Ask for an ordered consumer over the stream. There's no name to manage and
	// no ack to send: the library runs the consumer for you and recreates it if
	// it ever misses a message, so you read every order in stream order. An
	// empty config starts from the first order.
	cons, err := js.OrderedConsumer(ctx, "ORDERS", jetstream.OrderedConsumerConfig{})
	if err != nil {
		panic(err)
	}

	iter, err := cons.Messages()
	if err != nil {
		panic(err)
	}
	defer iter.Stop()

	// Read the whole log once, in order, stopping when caught up (NumPending 0).
	for {
		msg, err := iter.Next()
		if err != nil {
			break
		}
		meta, err := msg.Metadata()
		if err != nil {
			panic(err)
		}
		fmt.Printf("order %s\n", string(msg.Data()))
		if meta.NumPending == 0 {
			break
		}
	}
	// NATS-DOC-END
}
