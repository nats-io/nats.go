// Copyright 2013-2014 Apcera Inc. All rights reserved.

package nats

import (
	"runtime"
	"testing"
	"time"
)

func TestSimpleSendChan(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	recv := make(chan bool)

	numSent := int32(22)
	ch := make(chan int32)

	if err := ec.BindSendChan("foo", ch); err != nil {
		t.Fatalf("Failed to bind to a send channel: %v\n", err)
	}

	ec.Subscribe("foo", func(num int32) {
		if num != numSent {
			t.Fatalf("Failed to receive correct value: %d vs %d\n", num, numSent)
		}
		recv <- true
	})

	// Send to 'foo'
	ch <- numSent

	if e := wait(recv); e != nil {
		if ec.LastError() != nil {
			e = ec.LastError()
		}
		t.Fatalf("Did not receive the message: %s", e)
	}
	close(ch)
}

func TestSimpleRecvChan(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	numSent := int32(22)
	ch := make(chan int32)

	if _, err := ec.BindRecvChan("foo", ch); err != nil {
		t.Fatalf("Failed to bind to a send channel: %v\n", err)
	}

	ec.Publish("foo", numSent)

	// Receive from 'foo'
	select {
	case num := <-ch:
		if num != numSent {
			t.Fatalf("Failed to receive correct value: %d vs %d\n", num, numSent)
		}
	case <-time.After(1 * time.Second):
		t.Fatalf("Failed to receive a value, timed-out\n")
	}
	close(ch)
}

func TestRecvChanPanicOnClosedChan(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	ch := make(chan int)

	if _, err := ec.BindRecvChan("foo", ch); err != nil {
		t.Fatalf("Failed to bind to a send channel: %v\n", err)
	}

	close(ch)
	ec.Publish("foo", 22)
	ec.Flush()
}

func TestRecvChanAsyncLeakGoRoutines(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	before := runtime.NumGoroutine()

	ch := make(chan int)

	if _, err := ec.BindRecvChan("foo", ch); err != nil {
		t.Fatalf("Failed to bind to a send channel: %v\n", err)
	}

	// Close the receive Channel
	close(ch)

	// The publish will trugger the close and shutdown of the Go routines
	ec.Publish("foo", 22)
	ec.Flush()

	after := runtime.NumGoroutine()

	if before != after {
		t.Fatalf("Leaked Go routine(s) : %d, closing channel should have closed them\n", after-before)
	}
}

func TestRecvChanLeakGoRoutines(t *testing.T) {
	ec := NewEConn(t)
	defer ec.Close()

	before := runtime.NumGoroutine()

	ch := make(chan int)

	sub, err := ec.BindRecvChan("foo", ch)
	if err != nil {
		t.Fatalf("Failed to bind to a send channel: %v\n", err)
	}
	sub.Unsubscribe()
	// Sleep a bit to wait for the Go routine to exit.
	time.Sleep(50 * time.Millisecond)

	after := runtime.NumGoroutine()

	if before != after {
		t.Fatalf("Leaked Go routine(s) : %d, closing channel should have closed them\n", after-before)
	}
}

func BenchmarkPublishSpeedViaChan(b *testing.B) {
	b.StopTimer()
	server := startServer(b, DefaultPort, "")
	defer server.stopServer()
	nc, err := Connect(DefaultURL)
	if err != nil {
		b.Fatalf("Could not connect: %v\n", err)
	}
	ec, err := NewEncodedConn(nc, "default")
	defer ec.Close()

	ch := make(chan int32, 1024)
	if err := ec.BindSendChan("foo", ch); err != nil {
		b.Fatalf("Failed to bind to a send channel: %v\n", err)
	}

	b.StartTimer()

	num := int32(22)

	for i := 0; i < b.N; i++ {
		ch <- num
	}
	// Make sure they are all processed.
	nc.Flush()
	b.StopTimer()
}
