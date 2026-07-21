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
	// Join the "packers" queue group on orders.created. Every subscriber that
	// names the same group shares the load: each order is delivered to exactly
	// one member. Run this in several processes to watch the load balance.
	nc.QueueSubscribe("orders.created", "packers", func(m *nats.Msg) {
		fmt.Printf("packer handling: %s\n", string(m.Data))
	})
	// NATS-DOC-END

	select {}
}
