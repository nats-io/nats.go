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
	// Regional analytics: one subscription catches created orders from every
	// region. The single-token wildcard * matches exactly one token, so
	// orders.us.created and orders.eu.created both match, while orders.created
	// and orders.us.west.created do not.
	nc.Subscribe("orders.*.created", func(m *nats.Msg) {
		fmt.Printf("analytics: new order on %s\n", m.Subject)
	})
	// NATS-DOC-END

	select {}
}
