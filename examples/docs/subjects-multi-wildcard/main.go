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
	// Subscribe to all weather updates
	nc.Subscribe("weather.>", func(m *nats.Msg) {
		fmt.Printf("Received on %s: %s\n", m.Subject, string(m.Data))
	})

	// All these match the subscription
	nc.Publish("weather.us", []byte("US weather update"))
	nc.Publish("weather.us.east", []byte("East coast update"))
	nc.Publish("weather.eu.north.finland", []byte("Finland weather"))
	// NATS-DOC-END

	time.Sleep(100 * time.Millisecond)
}
