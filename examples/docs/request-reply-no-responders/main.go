package main

import (
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
	msg, err := nc.Request("no.such.service", []byte("test"), time.Second)
	if err == nats.ErrNoResponders {
		log.Println("No services available to handle request")
	}
	// NATS-DOC-END

	_ = msg
}
