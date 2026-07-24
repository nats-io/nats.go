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
	// Gather more than one reply to a single request. A plain Request returns
	// only the first reply, so when several services may answer, subscribe to
	// your own inbox, publish the request with that inbox as the reply subject,
	// and collect replies until they stop arriving.
	order := `{"order_id":"ord_8w2k","customer":"acme-co","total_cents":4200,"ts":"2026-05-22T10:14:22Z"}`
	inbox := nats.NewInbox()
	sub, _ := nc.SubscribeSync(inbox)
	nc.PublishRequest("orders.inventory.check", inbox, []byte(order))

	var replies [][]byte
	for {
		// Stop once no further reply arrives within the gap deadline.
		msg, err := sub.NextMsg(300 * time.Millisecond)
		if err != nil {
			break
		}
		replies = append(replies, msg.Data)
	}
	fmt.Printf("gathered %d replies\n", len(replies))
	// NATS-DOC-END
}
