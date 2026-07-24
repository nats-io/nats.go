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
	// Cap ORDERS so it can't grow without bound. Fetch the current config, add
	// a seven-day age limit and a 1 GiB byte ceiling, and update the stream in
	// place. Editing limits leaves the messages already stored alone.
	s, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		panic(err)
	}
	cfg := s.CachedInfo().Config
	cfg.MaxAge = 7 * 24 * time.Hour
	cfg.MaxBytes = 1 << 30 // 1 GiB
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		panic(err)
	}
	fmt.Println("ORDERS capped at 7d age and 1 GiB")
	// NATS-DOC-END
}
