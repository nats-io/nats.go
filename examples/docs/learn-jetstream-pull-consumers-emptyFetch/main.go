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
	// Bind to the durable "shipping" consumer.
	cons, err := js.Consumer(ctx, "ORDERS", "shipping")
	if err != nil {
		panic(err)
	}

	// A fetch on a drained consumer returns an empty batch once the wait
	// elapses, not an error. Treat "nothing right now" as normal: if no orders
	// came back, wait and fetch again instead of failing.
	msgs, err := cons.Fetch(10, jetstream.FetchMaxWait(2*time.Second))
	if err != nil {
		panic(err)
	}
	count := 0
	for msg := range msgs.Messages() {
		fmt.Printf("shipping %s\n", string(msg.Data()))
		msg.Ack()
		count++
	}
	if msgs.Error() != nil {
		panic(msgs.Error())
	}
	if count == 0 {
		fmt.Println("no orders waiting, will retry")
		time.Sleep(time.Second)
	}
	// NATS-DOC-END
}
