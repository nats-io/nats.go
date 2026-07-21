package main

import "github.com/nats-io/nats.go"

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	// NATS-DOC-START
	// A shipping-quote provider. Subscribe plainly to shipping.quote (NOT a
	// queue group, so every provider sees each request) and reply with a price.
	// Run several copies, each quoting a different number.
	nc.Subscribe("shipping.quote", func(m *nats.Msg) {
		m.Respond([]byte(`{"carrier":"carrier-a","quote_cents":1500}`))
	})
	// NATS-DOC-END

	select {}
}
