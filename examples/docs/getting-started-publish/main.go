package main

import (
	"log"

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
	// Publish messages
	nc.Publish("hello", []byte("Hello NATS!"))
	nc.Publish("hello", []byte("Welcome to messaging"))
	// NATS-DOC-END

	log.Println("Messages published")

	// Flush to ensure messages are sent
	nc.Flush()
}
