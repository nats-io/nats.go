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
	// Subscribe as the warehouse service to orders.created. The callback runs
	// for each matching message as it is published.
	nc.Subscribe("orders.created", func(m *nats.Msg) {
		fmt.Printf("warehouse received: %s\n", string(m.Data))
	})
	// NATS-DOC-END

	// Block so the subscriber keeps running.
	select {}
}
