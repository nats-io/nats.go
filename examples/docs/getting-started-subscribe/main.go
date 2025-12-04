package main

import (
	"log"
	"runtime"

	"github.com/nats-io/nats.go"
)

func main() {
	// Connect to NATS
	nc, err := nats.Connect("localhost:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	log.Println("Connected to NATS")

	// NATS-DOC-START
	// Subscribe to 'hello'
	nc.Subscribe("hello", func(msg *nats.Msg) {
		log.Printf("Received: %s", string(msg.Data))
	})
	// NATS-DOC-END

	log.Println("Waiting for messages...")

	// Keep the connection alive
	runtime.Goexit()
}
