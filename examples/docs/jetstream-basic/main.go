package main

import (
	"context"
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	ctx := context.Background()

	// NATS-DOC-START
	// Create a stream that captures any subject under `orders.`
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
		Storage:  jetstream.FileStorage,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Publish a few orders
	js.Publish(ctx, "orders.new", []byte("Order #1001"))
	js.Publish(ctx, "orders.new", []byte("Order #1002"))
	js.Publish(ctx, "orders.shipped", []byte("Order #1001 shipped"))

	// Create a durable pull consumer that delivers from the beginning
	consumer, err := stream.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:   "order-processor",
		AckPolicy: jetstream.AckExplicitPolicy,
	})
	if err != nil {
		log.Fatal(err)
	}

	// Fetch a batch and acknowledge each message
	msgs, err := consumer.Fetch(3)
	if err != nil {
		log.Fatal(err)
	}
	for msg := range msgs.Messages() {
		fmt.Printf("Received on %s: %s\n", msg.Subject(), string(msg.Data()))
		msg.Ack()
	}
	// NATS-DOC-END
}
