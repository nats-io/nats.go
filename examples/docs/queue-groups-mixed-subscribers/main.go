package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// Audit logger - receives all messages
	nc.Subscribe("orders.>", func(m *nats.Msg) {
		log.Printf("[AUDIT] %s: %s", m.Subject, string(m.Data))
	})

	// Metrics collector - receives all messages
	nc.Subscribe("orders.>", func(m *nats.Msg) {
		log.Printf("[METRICS] %s: %s", m.Subject, string(m.Data))
	})

	// Workers in queue group - load balanced
	nc.QueueSubscribe("orders.new", "workers", func(m *nats.Msg) {
		fmt.Printf("[WORKER A] Processing: %s\n", string(m.Data))
		processOrder(m.Data)
	})

	nc.QueueSubscribe("orders.new", "workers", func(m *nats.Msg) {
		fmt.Printf("[WORKER B] Processing: %s\n", string(m.Data))
		processOrder(m.Data)
	})

	// Publish orders
	nc.Publish("orders.new", []byte("Order 123"))
	nc.Publish("orders.new", []byte("Order 124"))
	// Audit and metrics see them, one worker processes each
	// NATS-DOC-END

	time.Sleep(500 * time.Millisecond)
}

func processOrder(data []byte) {
	// Process order implementation
}
