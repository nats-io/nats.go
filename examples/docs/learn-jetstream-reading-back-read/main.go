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
	// Bind to the durable consumer created earlier.
	cons, err := js.Consumer(ctx, "ORDERS", "orders-reader")
	if err != nil {
		panic(err)
	}

	// Ask the consumer how many messages are still waiting, then read exactly
	// that many. This drains everything stored without assuming a count.
	info, err := cons.Info(ctx)
	if err != nil {
		panic(err)
	}
	pending := info.NumPending
	if pending == 0 {
		fmt.Println("nothing to read")
		return
	}

	msgs, err := cons.Fetch(int(pending))
	if err != nil {
		panic(err)
	}

	for msg := range msgs.Messages() {
		// Metadata carries the position of this message in the stream and in
		// the consumer's own delivery sequence.
		meta, err := msg.Metadata()
		if err != nil {
			panic(err)
		}
		fmt.Printf("stream seq=%d consumer seq=%d payload=%s\n",
			meta.Sequence.Stream, meta.Sequence.Consumer, string(msg.Data()))

		// Acknowledge so the server advances the consumer past this message.
		if err := msg.Ack(); err != nil {
			panic(err)
		}
	}
	if msgs.Error() != nil {
		panic(msgs.Error())
	}
	// NATS-DOC-END
}
