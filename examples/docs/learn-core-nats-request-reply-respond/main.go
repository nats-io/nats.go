package main

import "github.com/nats-io/nats.go"

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// The inventory service: subscribe to orders.inventory.check and answer
	// every request by responding on the reply subject it carries.
	nc.Subscribe("orders.inventory.check", func(m *nats.Msg) {
		m.Respond([]byte(`{"in_stock":true,"warehouse":"us-east"}`))
	})
	// NATS-DOC-END

	// Block so the service keeps answering.
	select {}
}
