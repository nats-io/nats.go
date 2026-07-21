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
	// Look up the ORDERS stream and fetch its latest info from the server.
	stream, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		panic(err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		panic(err)
	}

	// Print key fields: name, captured subjects, and current message count.
	fmt.Printf("Name:     %s\n", info.Config.Name)
	fmt.Printf("Subjects: %v\n", info.Config.Subjects)
	fmt.Printf("Messages: %d\n", info.State.Msgs)
	// NATS-DOC-END
}
