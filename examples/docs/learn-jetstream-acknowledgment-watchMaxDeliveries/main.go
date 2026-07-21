package main

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

func main() {
	// Connect to the NATS server.
	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// Max-deliveries advisories are plain core NATS messages, so subscribe with
	// the core client instead of a JetStream consumer. The server publishes one
	// each time a message on the "shipping" consumer hits its delivery limit.
	subject := "$JS.EVENT.ADVISORY.CONSUMER.MAX_DELIVERIES.ORDERS.shipping"
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		panic(err)
	}
	defer sub.Unsubscribe()

	// Print each advisory as it arrives.
	for {
		msg, err := sub.NextMsg(nats.DefaultTimeout)
		if err != nil {
			panic(err)
		}
		fmt.Printf("Max-deliveries advisory: %s\n", string(msg.Data))
	}
	// NATS-DOC-END
}
