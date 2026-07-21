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
	// Set up a service
	nc.Subscribe("time", func(m *nats.Msg) {
		timeStr := time.Now().Format(time.RFC3339)
		m.Respond([]byte(timeStr))
	})

	// Make a request
	msg, err := nc.Request("time", nil, 1*time.Second)
	if err != nil {
		fmt.Printf("Request failed: %v\n", err)
		return
	}
	fmt.Printf("Response: %s\n", string(msg.Data))
	// NATS-DOC-END
}
