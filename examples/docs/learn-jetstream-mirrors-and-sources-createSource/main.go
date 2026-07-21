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

	// Setup: the three regional streams ALL-ORDERS aggregates, each with its
	// own subjects.
	regions := map[string]string{
		"ORDERS-US":   "us.orders.>",
		"ORDERS-EU":   "eu.orders.>",
		"ORDERS-APAC": "apac.orders.>",
	}
	for name, subj := range regions {
		if _, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     name,
			Subjects: []string{subj},
		}); err != nil {
			panic(err)
		}
	}

	// NATS-DOC-START
	// Create ALL-ORDERS as an aggregate that sources the three regional streams
	// into one. Unlike a mirror, a stream can list several sources.
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name: "ALL-ORDERS",
		Sources: []*jetstream.StreamSource{
			{Name: "ORDERS-US"},
			{Name: "ORDERS-EU"},
			{Name: "ORDERS-APAC"},
		},
	})
	if err != nil {
		panic(err)
	}

	cfg := stream.CachedInfo().Config
	fmt.Printf("Created %s sourcing %d streams\n", cfg.Name, len(cfg.Sources))
	// NATS-DOC-END
}
