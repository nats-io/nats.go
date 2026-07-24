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
	// Bind to the durable "shipping" consumer.
	cons, err := js.Consumer(ctx, "ORDERS", "shipping")
	if err != nil {
		panic(err)
	}

	// Each message carries how many times it has been delivered. A count above
	// one means a redelivery: the server handed this order out before, but a
	// worker crashed or ran past AckWait before acking. Key your side effects by
	// order_id so handling the same order twice is harmless.
	msgs, err := cons.Fetch(10)
	if err != nil {
		panic(err)
	}
	for msg := range msgs.Messages() {
		meta, err := msg.Metadata()
		if err != nil {
			panic(err)
		}
		if meta.NumDelivered > 1 {
			fmt.Printf("redelivery #%d of %s\n", meta.NumDelivered, string(msg.Data()))
		} else {
			fmt.Printf("first delivery of %s\n", string(msg.Data()))
		}
		msg.Ack()
	}
	if msgs.Error() != nil {
		panic(msgs.Error())
	}
	// NATS-DOC-END
}
