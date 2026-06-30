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
	// A mirror is eventually consistent. Read its Lag before trusting it to
	// hold what the upstream just received: 0 means fully caught up.
	stream, err := js.Stream(ctx, "ORDERS-ARCHIVE")
	if err != nil {
		panic(err)
	}

	info, err := stream.Info(ctx)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Upstream:  %s\n", info.Mirror.Name)
	fmt.Printf("Lag:       %d\n", info.Mirror.Lag)
	fmt.Printf("Last seen: %s ago\n", info.Mirror.Active)
	// NATS-DOC-END
}
