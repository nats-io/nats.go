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

	// Fetch a batch of up to 10 orders, waiting up to 2 seconds for them. The
	// call returns when the batch is full or the wait elapses, whichever comes
	// first. Process and ack each one, then fetch again to keep going.
	msgs, err := cons.Fetch(10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		panic(err)
	}
	for msg := range msgs.Messages() {
		fmt.Printf("shipping %s\n", string(msg.Data()))
		msg.Ack()
	}
	if msgs.Error() != nil {
		panic(msgs.Error())
	}
	// NATS-DOC-END
}
