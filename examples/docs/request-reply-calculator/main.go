package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

func main() {
	nc, _ := nats.Connect(nats.DefaultURL)
	defer nc.Close()

	// NATS-DOC-START
	// Calculator service
	nc.Subscribe("calc.add", func(msg *nats.Msg) {
		parts := strings.Fields(string(msg.Data))
		if len(parts) == 2 {
			a, _ := strconv.Atoi(parts[0])
			b, _ := strconv.Atoi(parts[1])
			result := fmt.Sprintf("%d", a+b)
			msg.Respond([]byte(result))
		}
	})

	// Make calculations
	time.Sleep(100 * time.Millisecond)

	resp, _ := nc.Request("calc.add", []byte("5 3"), time.Second)
	fmt.Printf("5 + 3 = %s\n", string(resp.Data))

	resp, _ = nc.Request("calc.add", []byte("10 7"), time.Second)
	fmt.Printf("10 + 7 = %s\n", string(resp.Data))
	// NATS-DOC-END

	time.Sleep(100 * time.Millisecond)
}
