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
	// Create JetStream work queue
	js, _ := nc.JetStream()

	// Add work queue stream
	js.AddStream(&nats.StreamConfig{
		Name:      "WORK_QUEUE",
		Subjects:  []string{"work.tasks"},
		Retention: nats.WorkQueuePolicy, // Work queue retention
		Storage:   nats.FileStorage,
		Replicas:  3,
	})

	// Create consumer for workers
	js.AddConsumer("WORK_QUEUE", &nats.ConsumerConfig{
		Durable:    "WORKERS",
		AckPolicy:  nats.AckExplicitPolicy,
		MaxDeliver: 3,
	})

	// Worker processing loop
	sub, _ := js.PullSubscribe("work.tasks", "WORKERS")

	for {
		msgs, _ := sub.Fetch(1, nats.MaxWait(time.Second))

		for _, msg := range msgs {
			fmt.Printf("Processing task: %s\n", string(msg.Data))
			processTask(msg.Data)
			msg.Ack()
		}
	}
	// NATS-DOC-END
}

func processTask(data []byte) {
	// Process task implementation
}
