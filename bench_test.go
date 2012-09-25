package nats

import (
	"testing"
)

func BenchmarkPublishSpeed(b *testing.B) {
	b.StopTimer()
	server := startServer(b, DefaultPort, "")
	defer server.stopServer()
	nc, err := Connect(DefaultURL)
	defer nc.Close()
	b.StartTimer()

	msg := []byte("Hello World")

	for i := 1; i < b.N; i++ {
		err = nc.Publish("foo", msg)
		if err != nil {
			b.Fatalf("Error in benchmark during Publish: %v\n", err)
		}
		b.SetBytes(1)
	}
	// Make sure they are all processed.
	nc.Flush()
	b.StopTimer()
}
