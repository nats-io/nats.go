package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
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
	// Bind to the durable "shipping" consumer created earlier.
	cons, err := js.Consumer(ctx, "ORDERS", "shipping")
	if err != nil {
		panic(err)
	}

	// Consume runs the handler for every order the server hands this worker.
	// Start this same program in several processes: they all share the one
	// "shipping" consumer, and the server splits the stored orders across them,
	// one order to one worker.
	consCtx, err := cons.Consume(func(msg jetstream.Msg) {
		fmt.Printf("shipping %s\n", string(msg.Data()))
		msg.Ack()
	})
	if err != nil {
		panic(err)
	}
	defer consCtx.Stop()

	// Keep this worker running until interrupted (Ctrl-C).
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt)
	<-sig
	// NATS-DOC-END
}
