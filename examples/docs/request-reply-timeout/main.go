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
	// Request with custom timeout
	msg, err := nc.Request("service", []byte("data"), 2*time.Second)
	if err != nil {
		if err == nats.ErrTimeout {
			fmt.Println("Request timed out")
		} else {
			fmt.Printf("Request failed: %v\n", err)
		}
		return
	}
	fmt.Printf("Response: %s\n", string(msg.Data))
	// NATS-DOC-END
}
