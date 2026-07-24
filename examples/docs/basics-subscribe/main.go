package main

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// Subscribe to weather updates
	sub, _ := nc.Subscribe("weather.updates", func(msg *nats.Msg) {
		fmt.Printf("Received: %s\n", string(msg.Data))
	})
	// NATS-DOC-END
	defer sub.Unsubscribe()
}
