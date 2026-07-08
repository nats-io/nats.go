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
	// Scatter one request to every shipping-quote provider and gather the
	// replies. Subscribe to a private inbox, publish the request with that
	// inbox as the reply subject, then collect quotes until they stop arriving
	// and pick the cheapest.
	order := `{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}`
	inbox := nats.NewInbox()
	sub, _ := nc.SubscribeSync(inbox)
	nc.PublishRequest("shipping.quote", inbox, []byte(order))

	var quotes []string
	for {
		msg, err := sub.NextMsg(300 * time.Millisecond)
		if err != nil {
			break
		}
		quotes = append(quotes, string(msg.Data))
	}
	fmt.Printf("gathered %d quotes: %v\n", len(quotes), quotes)
	// NATS-DOC-END
}
