package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	// NATS-DOC-START
	// Subscriber 1
	nc.Subscribe("events.data", func(msg *nats.Msg) {
		fmt.Printf("Subscriber 1 received: %s\n", string(msg.Data))
	})

	// Subscriber 2
	nc.Subscribe("events.data", func(msg *nats.Msg) {
		fmt.Printf("Subscriber 2 received: %s\n", string(msg.Data))
	})

	// Publisher
	nc.Publish("events.data", []byte("Hello from NATS!"))
	// Both subscribers receive the message
	// NATS-DOC-END

	time.Sleep(100 * time.Millisecond)
}
