package main

import (
	"log"
	"runtime"

	"github.com/nats-io/nats.go"
)

func main() {
	// Connect to NATS demo server
	nc, err := nats.Connect("demo.nats.io")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Subscribe to 'hello'
	_, err = nc.Subscribe("hello", func(msg *nats.Msg) {
		log.Printf("Received: %s", string(msg.Data))
	})
	if err != nil {
		log.Fatal(err)
	}

	log.Println("Listening for messages on 'hello'...")

	// Keep the connection alive
	runtime.Goexit()
}
