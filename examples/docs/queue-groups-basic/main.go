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
	// Create three workers in the same queue group
	nc.QueueSubscribe("orders.new", "workers", func(m *nats.Msg) {
		fmt.Printf("Worker A processed: %s\n", string(m.Data))
	})

	nc.QueueSubscribe("orders.new", "workers", func(m *nats.Msg) {
		fmt.Printf("Worker B processed: %s\n", string(m.Data))
	})

	nc.QueueSubscribe("orders.new", "workers", func(m *nats.Msg) {
		fmt.Printf("Worker C processed: %s\n", string(m.Data))
	})

	// Publish messages - automatically load balanced
	for i := 1; i <= 10; i++ {
		nc.Publish("orders.new", []byte(fmt.Sprintf("Order %d", i)))
	}
	// NATS-DOC-END

	time.Sleep(100 * time.Millisecond)
}
