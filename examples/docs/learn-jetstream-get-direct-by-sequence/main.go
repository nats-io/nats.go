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

	// Look up the ORDERS stream.
	stream, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		panic(err)
	}

	// NATS-DOC-START
	// Read the message stored at stream sequence 2. This is the regular get,
	// served by the stream leader.
	msg, err := stream.GetMsg(ctx, 2)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Subject: %s\n", msg.Subject)
	fmt.Printf("Payload: %s\n", string(msg.Data))
	// NATS-DOC-END
}
