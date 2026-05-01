package main

import (
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	// NATS-DOC-START
	// Header Aware service
	nc.Subscribe("service", func(msg *nats.Msg) {
		responseMsg := nats.NewMsg(msg.Reply)
		responseMsg.Data = msg.Data
		responseMsg.Header.Add("X-Response-ID", "123")
		responseMsg.Header.Add("X-Request-ID", msg.Header.Get("X-Response-ID"))
		responseMsg.Header.Add("X-Priority", msg.Header.Get("X-Priority"))
		nc.PublishMsg(responseMsg)
	})

	// Create message with headers
	msg := nats.NewMsg("service")
	msg.Header.Add("X-Request-ID", "123")
	msg.Header.Add("X-Priority", "high")
	msg.Data = []byte("data")

	// Send request with headers
	response, err := nc.RequestMsg(msg, time.Second)
	if err == nil {
		fmt.Printf("Response: %s\n", response.Data)
		fmt.Printf("Response ID: %s\n", response.Header.Get("X-Response-ID"))
	} else {
		fmt.Printf("Error: %s\n", err)
	}
	// NATS-DOC-END
	time.Sleep(100 * time.Millisecond)
}
