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
	// Add a per-subject ceiling so one noisy subject can't evict another's
	// messages. MaxMsgsPerSubject keeps the most recent N messages for every
	// subject independently, alongside the whole-stream limits.
	s, err := js.Stream(ctx, "ORDERS")
	if err != nil {
		panic(err)
	}
	cfg := s.CachedInfo().Config
	cfg.MaxMsgsPerSubject = 100000
	if _, err := js.UpdateStream(ctx, cfg); err != nil {
		panic(err)
	}
	fmt.Println("ORDERS now keeps 100000 messages per subject")
	// NATS-DOC-END
}
