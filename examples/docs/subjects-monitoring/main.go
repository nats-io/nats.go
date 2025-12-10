package main

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	// NATS-DOC-START
	// Create a wire tap for monitoring
	nc.Subscribe(">", func(m *nats.Msg) {
		fmt.Printf("[MONITOR] %s: %s\n", m.Subject, string(m.Data))
	})
	// NATS-DOC-END
}
