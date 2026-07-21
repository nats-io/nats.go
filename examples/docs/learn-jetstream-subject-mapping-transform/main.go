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
	// Create a stream that rewrites subjects as it stores them. The transform
	// shards each customer into one of three buckets: partition(3,1) hashes the
	// first wildcard token into 0, 1, or 2, and wildcard(1) keeps that token.
	// So "ingest.alice" might land on "orders.2.alice".
	stream, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS-SHARDED",
		Subjects: []string{"ingest.*"},
		SubjectTransform: &jetstream.SubjectTransformConfig{
			Source:      "ingest.*",
			Destination: "orders.{{partition(3,1)}}.{{wildcard(1)}}",
		},
	})
	if err != nil {
		panic(err)
	}

	fmt.Printf("Created stream: %s\n", stream.CachedInfo().Config.Name)
	// NATS-DOC-END
}
