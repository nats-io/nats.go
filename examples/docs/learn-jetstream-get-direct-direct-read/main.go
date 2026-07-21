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

	// Look up the ORDERS stream. The handle caches the stream config, including
	// AllowDirect, which must already be enabled on the stream.
	stream, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		panic(err)
	}

	// NATS-DOC-START
	// Read the message at sequence 1. Because the stream has AllowDirect
	// enabled, GetMsg is served by the Direct Get API, so any replica can
	// answer it, not only the leader.
	msg, err := stream.GetMsg(ctx, 1)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Subject: %s\n", msg.Subject)
	fmt.Printf("Payload: %s\n", string(msg.Data))
	// NATS-DOC-END
}
