package nats

import (
	"testing"
	"time"
)

func BenchmarkPublishSpeed(b *testing.B) {
	b.StopTimer()
	server := startServer(b, DefaultPort, "")
	defer server.stopServer()
	nc, err := Connect(DefaultURL)
	if err != nil {
		b.Fatalf("Could not connect: %v\n", err)
	}
	defer nc.Close()
	b.StartTimer()

	msg := []byte("Hello World")

	for i := 0; i < b.N; i++ {
		err = nc.Publish("foo", msg)
		if err != nil {
			b.Fatalf("Error in benchmark during Publish: %v\n", err)
		}
	}
	// Make sure they are all processed.
	nc.Flush()
	b.StopTimer()
}

func BenchmarkPubSubSpeed(b *testing.B) {
	b.StopTimer()
	server := startServer(b, DefaultPort, "")
	defer server.stopServer()
	nc, err := Connect(DefaultURL)
	if err != nil {
		b.Fatalf("Could not connect: %v\n", err)
	}
	defer nc.Close()

	ch := make(chan bool)
	b.StartTimer()

	nc.Opts.AsyncErrorCB = func(nc *Conn, s *Subscription, err error) {
		b.Fatalf("Error : %v\n", err)
	}

	received := 0

	nc.Subscribe("foo", func(m *Msg) {
		received += 1
		if received >= b.N {
			ch <- true
		}
	})

	msg := []byte("Hello World")

	for i := 0; i < b.N; i++ {
		err = nc.Publish("foo", msg)
		if err != nil {
			b.Fatalf("Error in benchmark during Publish: %v\n", err)
		}
		// Don't overrun ourselves and be a slow consumer
		if i%1000 == 0 {
			time.Sleep(10)
		}
	}

	// Make sure they are all processed.
	err = waitTime(ch, 10*time.Second)
	if err != nil {
		b.Fatal("Timed out waiting for messages")
	} else if received != b.N {
		b.Fatal(nc.LastError())
	}
	b.StopTimer()
}
