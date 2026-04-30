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
	// Subscribe to all non-critical alarms
	nc.Subscribe("sensor.alarm.*", func(m *nats.Msg) {
		fmt.Printf("[sensor.alarm.*]       %-15s (%s)\n", string(m.Data), m.Subject)
	})

	// Subscribe to all critical
	nc.Subscribe("sensor.*.*.critical", func(m *nats.Msg) {
		fmt.Printf("[sensor.*.*.critical]  %-15s (%s)\n", string(m.Data), m.Subject)
	})

	// Subscribe to everything
	nc.Subscribe("sensor.>", func(m *nats.Msg) {
		fmt.Printf("[sensor.>]             %-15s (%s)\n", string(m.Data), m.Subject)
	})

	// Publish to specific subjects
	nc.Publish("sensor.alarm.smoke", []byte("kitchen,14:22"))
	nc.Publish("sensor.alarm.smoke.critical", []byte("kitchen,14:23"))
	nc.Publish("sensor.alarm.water", []byte("basement,16:42"))
	nc.Publish("sensor.alarm.water.critical", []byte("basement,16:43"))
	// NATS-DOC-END

	time.Sleep(100 * time.Millisecond)
}
