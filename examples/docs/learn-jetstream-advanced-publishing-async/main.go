package main

import (
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

	// NATS-DOC-START
	// Async publish: fire every order without waiting for its PubAck, then
	// collect the futures and check each one. The round trips overlap, so this
	// is far faster than publishing one at a time -- but you still have to
	// confirm every ack, because a publish whose ack never arrives is a lost
	// order, not a stored one.
	orders := []string{
		`{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200}`,
		`{"order_id":"ord_2zr9","customer":"globex","total_cents":7800}`,
		`{"order_id":"ord_5t1m","customer":"initech","total_cents":1500}`,
		`{"order_id":"ord_9p3x","customer":"hooli","total_cents":9900}`,
	}

	// PublishAsync returns immediately, before the server replies.
	futures := make([]jetstream.PubAckFuture, 0, len(orders))
	for _, order := range orders {
		f, err := js.PublishAsync("orders.created", []byte(order))
		if err != nil {
			panic(err)
		}
		futures = append(futures, f)
	}

	// Wait until every publish has been answered, or give up after a timeout.
	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		panic(fmt.Sprintf("timed out with %d publishes unconfirmed", js.PublishAsyncPending()))
	}

	// Check each ack. A publish whose ack failed has to be re-published.
	for i, f := range futures {
		select {
		case ack := <-f.Ok():
			fmt.Printf("order %d stored at sequence %d\n", i+1, ack.Sequence)
		case err := <-f.Err():
			fmt.Printf("order %d failed, re-publish it: %v\n", i+1, err)
		}
	}
	// NATS-DOC-END
}
