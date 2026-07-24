package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// Ask the inventory service whether an order's item is in stock. The client
	// creates a private inbox, sends the request, and waits up to the timeout
	// for one reply. A missing service surfaces immediately as
	// nats.ErrNoResponders; a slow one as nats.ErrTimeout.
	order := `{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}`
	msg, err := nc.Request("orders.inventory.check", []byte(order), 2*time.Second)
	switch err {
	case nats.ErrNoResponders:
		fmt.Println("no inventory service is running")
	case nats.ErrTimeout:
		fmt.Println("inventory service did not answer in time")
	case nil:
		fmt.Printf("inventory replied: %s\n", string(msg.Data))
	default:
		fmt.Printf("request failed: %v\n", err)
	}
	// NATS-DOC-END
}
