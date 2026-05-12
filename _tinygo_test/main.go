//go:build tinygo

package main

import (
	"fmt"

	nats "github.com/nats-io/nats.go"
)

func main() {
	nc, err := nats.Connect(nats.DefaultURL)
	if err != nil {
		panic(err)
	}
	defer nc.Close()

	received := make(chan string, 1)
	nc.Subscribe("tinygo.test", func(m *nats.Msg) {
		received <- string(m.Data)
	})

	nc.Publish("tinygo.test", []byte("hello from tinygo"))

	msg := <-received
	if msg != "hello from tinygo" {
		panic("wrong message: " + msg)
	}
	fmt.Println("OK: pub/sub works under TinyGo native target")
}
