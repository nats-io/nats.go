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
	// Read the current config, turn on direct access, and push the update.
	// AllowDirect lets any replica serve single-message gets.
	info, err := stream.Info(ctx)
	if err != nil {
		panic(err)
	}

	cfg := info.Config
	cfg.AllowDirect = true

	updated, err := js.UpdateStream(ctx, cfg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("AllowDirect: %v\n", updated.CachedInfo().Config.AllowDirect)
	// NATS-DOC-END
}
