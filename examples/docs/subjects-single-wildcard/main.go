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
	nc.Subscribe("orders.*.shipped", func(m *nats.Msg) {
		fmt.Printf("[orders.*.shipped] %s: %s\n", string(m.Data), m.Subject)
	})

	nc.Subscribe("orders.*.placed", func(m *nats.Msg) {
		fmt.Printf("[orders.*.placed]  %s: %s\n", string(m.Data), m.Subject)
	})

	nc.Subscribe("orders.retail.*", func(m *nats.Msg) {
		fmt.Printf("[orders.retail.*]  %s: %s\n", string(m.Data), m.Subject)
	})

	// Publish to specific subjects
	nc.Publish("orders.wholesale.placed", []byte("Order W73737"));
	nc.Publish("orders.retail.placed", []byte("Order R65432"));
	nc.Publish("orders.wholesale.shipped", []byte("Order W73001"));
	nc.Publish("orders.retail.shipped", []byte("Order R65321"));
	// NATS-DOC-END

	time.Sleep(100 * time.Millisecond)
}
