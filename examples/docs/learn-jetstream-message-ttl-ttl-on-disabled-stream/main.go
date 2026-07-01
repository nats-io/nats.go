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

	// This stream never opted in: AllowMsgTTL is left at its default (false).
	_, err = js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "ORDERS_NO_TTL",
		Subjects: []string{"no-ttl.>"},
	})
	if err != nil {
		panic(err)
	}

	// NATS-DOC-START
	// Publishing with a TTL to a stream that hasn't enabled AllowMsgTTL fails.
	// The server returns "per-message TTL is disabled" (err_code 10166) and
	// stores nothing. The fix is to opt the stream in with AllowMsgTTL: true.
	_, err = js.Publish(ctx, "no-ttl.msg", []byte("order 42 cancelled"),
		jetstream.WithMsgTTL(60*time.Second))
	if err != nil {
		fmt.Printf("Publish rejected, message not stored: %s\n", err)
		return
	}
	// NATS-DOC-END
}
