package main

import (
	"log"

	"github.com/nats-io/nats.go"
)

func main() {
	// Connect to NATS demo server
	nc, err := nats.Connect("demo.nats.io")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Publish a message
	err = nc.Publish("hello", []byte("Hello NATS!"))
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Message published to hello")
}
