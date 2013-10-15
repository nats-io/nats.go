package nats

import (
	"sync/atomic"
	"testing"
	"time"
)

func startReconnectServer(t *testing.T) *server {
	return startServer(t, 22222, "")
}

func TestReconnectDisallowedFlags(t *testing.T) {
	ts := startReconnectServer(t)
	ch := make(chan bool)
	opts := DefaultOptions
	opts.Url = "nats://localhost:22222"
	opts.AllowReconnect = false
	opts.ClosedCB = func(_ *Conn) {
		ch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}

	ts.stopServer()
	if e := wait(ch); e != nil {
		t.Fatal("Did not trigger ClosedCB correctly")
	}
	nc.Close()
}

func TestReconnectAllowedFlags(t *testing.T) {
	ts := startReconnectServer(t)
	ch := make(chan bool)
	opts := DefaultOptions
	opts.Url = "nats://localhost:22222"
	opts.AllowReconnect = true
	opts.MaxReconnect = 2
	opts.ReconnectWait = 1 * time.Second

	opts.ClosedCB = func(_ *Conn) {
		ch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	ts.stopServer()

	// We want wait to timeout here, and the connection
	// should not trigger the Close CB.
	if e := wait(ch); e == nil {
		t.Fatal("Triggered ClosedCB incorrectly")
	}
	if !nc.isReconnecting() {
		t.Fatal("Expected to be in a reconnecting state")
	}

	// clear the CloseCB since ch will block
	nc.Opts.ClosedCB = nil
	nc.Close()
}

var reconnectOpts = Options{
	Url:            "nats://localhost:22222",
	AllowReconnect: true,
	MaxReconnect:   10,
	ReconnectWait:  100 * time.Millisecond,
	Timeout:        DefaultTimeout,
}

func TestBasicReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)
	ch := make(chan bool)

	opts := reconnectOpts
	nc, _ := opts.Connect()
	ec, err := NewEncodedConn(nc, "default")
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}

	testString := "bar"
	ec.Subscribe("foo", func(s string) {
		if s != testString {
			t.Fatal("String doesn't match")
		}
		ch <- true
	})
	ec.Flush()

	ts.stopServer()
	// server is stopped here...

	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *Conn) {
		dch <- true
	}
	wait(dch)

	if err := ec.Publish("foo", testString); err != nil {
		t.Fatalf("Failed to publish message: %v\n", err)
	}

	ts = startReconnectServer(t)
	defer ts.stopServer()

	if err := ec.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}

	if e := wait(ch); e != nil {
		t.Fatal("Did not receive our message")
	}

	expectedReconnectCount := uint64(1)
	if ec.Conn.Stats.Reconnects != expectedReconnectCount {
		t.Fatalf("Reconnect count incorrect: %d vs %d\n",
			ec.Conn.Stats.Reconnects, expectedReconnectCount)
	}

	nc.Close()
}

func TestExtendedReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)

	opts := reconnectOpts
	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *Conn) {
		dch <- true
	}
	rch := make(chan bool)
	opts.ReconnectedCB = func(_ *Conn) {
		rch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	ec, err := NewEncodedConn(nc, "default")
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	testString := "bar"
	received := int32(0)

	ec.Subscribe("foo", func(s string) {
		atomic.AddInt32(&received, 1)
	})

	sub, _ := ec.Subscribe("foobar", func(s string) {
		atomic.AddInt32(&received, 1)
	})

	ec.Publish("foo", testString)
	ec.Flush()

	ts.stopServer()
	// server is stopped here..

	// wait for disconnect
	if e := waitTime(dch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	// Sub while disconnected
	ec.Subscribe("bar", func(s string) {
		atomic.AddInt32(&received, 1)
	})

	// Unsub while disconnected
	sub.Unsubscribe()

	if err = ec.Publish("foo", testString); err != nil {
		t.Fatalf("Received an error after disconnect: %v\n", err)
	}

	if err = ec.Publish("bar", testString); err != nil {
		t.Fatalf("Received an error after disconnect: %v\n", err)
	}

	ts = startReconnectServer(t)
	defer ts.stopServer()

	// server is restarted here..
	// wait for reconnect
	if e := waitTime(rch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a reconnect callback message")
	}

	if err = ec.Publish("foobar", testString); err != nil {
		t.Fatalf("Received an error after server restarted: %v\n", err)
	}

	if err = ec.Publish("foo", testString); err != nil {
		t.Fatalf("Received an error after server restarted: %v\n", err)
	}

	ch := make(chan bool)
	ec.Subscribe("done", func(b bool) {
		ch <- true
	})
	ec.Publish("done", true)

	if e := wait(ch); e != nil {
		t.Fatal("Did not receive our message")
	}

	if received != 4 {
		t.Fatalf("Received != %d, equals %d\n", 4, received)
	}
}

func TestParseStateReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)
	ch := make(chan bool)

	opts := reconnectOpts
	nc, _ := opts.Connect()
	ec, err := NewEncodedConn(nc, "default")
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}

	testString := "bar"
	ec.Subscribe("foo", func(s string) {
		if s != testString {
			t.Fatal("String doesn't match")
		}
		ch <- true
	})
	ec.Flush()

	// Simulate partialState, this needs to be cleared
	nc.mu.Lock()
	nc.ps.state = OP_PON
	nc.mu.Unlock()

	ts.stopServer()
	// server is stopped here...

	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *Conn) {
		dch <- true
	}
	wait(dch)

	if err := ec.Publish("foo", testString); err != nil {
		t.Fatalf("Failed to publish message: %v\n", err)
	}

	ts = startReconnectServer(t)
	defer ts.stopServer()

	if err := ec.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}

	if e := wait(ch); e != nil {
		t.Fatal("Did not receive our message")
	}

	expectedReconnectCount := uint64(1)
	if ec.Conn.Stats.Reconnects != expectedReconnectCount {
		t.Fatalf("Reconnect count incorrect: %d vs %d\n",
			ec.Conn.Stats.Reconnects, expectedReconnectCount)
	}

	nc.Close()
}

func TestIsClosed(t *testing.T) {
	ts := startReconnectServer(t)
	nc := newConnection(t)
	if nc.IsClosed() == true {
		t.Fatalf("IsClosed returned true when the connection is still open.")
	}
	ts.stopServer()
	if nc.IsClosed() == true {
		t.Fatalf("IsClosed returned true when the connection is still open.")
	}
	ts = startReconnectServer(t)
	if nc.IsClosed() == true {
		t.Fatalf("IsClosed returned true when the connection is still open.")
	}
	nc.Close()
	if nc.IsClosed() == false {
		t.Fatalf("IsClosed returned false after Close() was called.")
	}
	ts.stopServer()
}
