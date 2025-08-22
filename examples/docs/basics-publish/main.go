package main

import "github.com/nats-io/nats.go"

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// Publish a message to the 'weather.updates' subject
	nc.Publish("weather.updates", []byte("Temperature: 72°F"))
	// NATS-DOC-END
}
