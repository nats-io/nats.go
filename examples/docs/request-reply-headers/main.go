package main

import (
	"fmt"
	"log"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// Create message with headers
	msg := nats.NewMsg("service")
	msg.Header.Add("X-Request-ID", "123")
	msg.Header.Add("X-Priority", "high")
	msg.Data = []byte("data")

	// Send request with headers
	response, err := nc.RequestMsg(msg, time.Second)
	if err == nil {
		fmt.Printf("Response: %s\n", response.Data)
		fmt.Printf("Response ID: %s\n", response.Header.Get("X-Response-ID"))
	}
	// NATS-DOC-END
}
