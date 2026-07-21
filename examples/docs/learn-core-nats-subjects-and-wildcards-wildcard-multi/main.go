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
	// Audit service: catch every order message at any depth. The multi-token
	// wildcard > matches one or more tokens and must be the last token, so
	// orders.> matches orders.created, orders.us.created, and
	// orders.us.west.created alike.
	nc.Subscribe("orders.>", func(m *nats.Msg) {
		fmt.Printf("audit: %s\n", m.Subject)
	})
	// NATS-DOC-END

	select {}
}
