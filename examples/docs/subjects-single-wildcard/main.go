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
	// Subscribe with single token wildcard
	nc.Subscribe("weather.*.east", func(m *nats.Msg) {
		fmt.Printf("Received on %s: %s\n", m.Subject, string(m.Data))
	})

	// Publish to specific subjects
	nc.Publish("weather.us.east", []byte("Temperature: 72F"))
	nc.Publish("weather.eu.east", []byte("Temperature: 18C"))
	// NATS-DOC-END

	time.Sleep(100 * time.Millisecond)
}
