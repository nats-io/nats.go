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
	processCalculation := func(data []byte) []byte {
		return []byte("calculated result")
	}

	// Multiple responders - only first response is returned
	nc.Subscribe("calc.add", func(m *nats.Msg) {
		result := processCalculation(m.Data)
		m.Respond(append(result, []byte(" from A")...))
	})

	nc.Subscribe("calc.add", func(m *nats.Msg) {
		result := processCalculation(m.Data)
		m.Respond(append(result, []byte(" from B")...))
	})

	// Gets one response
	msg, _ := nc.Request("calc.add", []byte("data"), time.Second)
	fmt.Printf("Got response: %s\n", msg.Data)
	// NATS-DOC-END
}
