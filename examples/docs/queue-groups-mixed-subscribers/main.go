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
		updateMetrics(m.Subject)
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

	// Publish order
	nc.Publish("orders.new", []byte("Order 123"))
	// Audit and metrics see it, one worker processes it
	// NATS-DOC-END

	time.Sleep(100 * time.Millisecond)
}

func updateMetrics(subject string) {
	// Update metrics implementation
}

func processOrder(data []byte) {
	// Process order implementation
}
