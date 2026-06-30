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
	// FULFILLMENT is the queue of paid orders awaiting shipment. With WorkQueue
	// retention the server keeps each message only until a consumer acks it,
	// then deletes it from the stream.
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:      "FULFILLMENT",
		Subjects:  []string{"fulfill.>"},
		Retention: jetstream.WorkQueuePolicy,
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("FULFILLMENT retention: %s\n", stream.CachedInfo().Config.Retention)

	// Queue one order to ship.
	if _, err := js.Publish(ctx, "fulfill.us", []byte(`{"order_id":"ord_8w2k","customer":"acme-co"}`)); err != nil {
		panic(err)
	}

	// A shipping worker binds to a durable pull consumer with explicit ack.
	cons, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "shippers",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		panic(err)
	}

	// Fetch the order and ack it once the worker has shipped it.
	msgs, err := cons.Fetch(1)
	if err != nil {
		panic(err)
	}
	for msg := range msgs.Messages() {
		fmt.Printf("shipping order: %s\n", string(msg.Data()))
		if err := msg.Ack(); err != nil {
			panic(err)
		}
	}
	if msgs.Error() != nil {
		panic(msgs.Error())
	}

	// The ack removed the task from the queue, so the stream is now empty. A
	// Limits stream like ORDERS would still hold the message; a WorkQueue
	// stream drains to zero as workers finish.
	info, err := stream.Info(ctx)
	if err != nil {
		panic(err)
	}
	fmt.Printf("messages left in FULFILLMENT: %d\n", info.State.Msgs)
	// NATS-DOC-END
}
