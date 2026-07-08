package main

import "github.com/nats-io/nats.go"

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// Publish one order to the orders.created subject. Publishing is
	// fire-and-forget: the call hands the message to the server and returns.
	order := `{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}`
	nc.Publish("orders.created", []byte(order))
	// NATS-DOC-END

	// Flush so the buffered publish reaches the server before we exit.
	nc.Flush()
}
