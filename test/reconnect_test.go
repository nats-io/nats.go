package test

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats"
)

func startReconnectServer(t *testing.T) *server.Server {
	return RunServerOnPort(22222)
}

func TestReconnectTotalTime(t *testing.T) {
	opts := nats.DefaultOptions
	totalReconnectTime := time.Duration(opts.MaxReconnect) * opts.ReconnectWait
	if totalReconnectTime < (2 * time.Minute) {
		t.Fatalf("Total reconnect time should be at least 2 mins: Currently %v\n",
			totalReconnectTime)
	}
}

func TestReconnectDisallowedFlags(t *testing.T) {
	ts := startReconnectServer(t)
	ch := make(chan bool)
	opts := nats.DefaultOptions
	opts.Url = "nats://localhost:22222"
	opts.AllowReconnect = false
	opts.ClosedCB = func(_ *nats.Conn) {
		ch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}

	ts.Shutdown()

	if e := Wait(ch); e != nil {
		t.Fatal("Did not trigger ClosedCB correctly")
	}
	nc.Close()
}

func TestReconnectAllowedFlags(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()
	ch := make(chan bool)
	opts := nats.DefaultOptions
	opts.Url = "nats://localhost:22222"
	opts.AllowReconnect = true
	opts.MaxReconnect = 2
	opts.ReconnectWait = 1 * time.Second

	opts.ClosedCB = func(_ *nats.Conn) {
		ch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	ts.Shutdown()

	// We want wait to timeout here, and the connection
	// should not trigger the Close CB.
	if e := Wait(ch); e == nil {
		t.Fatal("Triggered ClosedCB incorrectly")
	}
	if !nc.IsReconnecting() {
		t.Fatal("Expected to be in a reconnecting state")
	}

	// clear the CloseCB since ch will block
	nc.Opts.ClosedCB = nil
	nc.Close()
}

var reconnectOpts = nats.Options{
	Url:            "nats://localhost:22222",
	AllowReconnect: true,
	MaxReconnect:   10,
	ReconnectWait:  100 * time.Millisecond,
	Timeout:        nats.DefaultTimeout,
}

func TestBasicReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)

	ch := make(chan bool)

	opts := reconnectOpts
	nc, _ := opts.Connect()
	ec, err := nats.NewEncodedConn(nc, nats.DEFAULT_ENCODER)
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

	ts.Shutdown()
	// server is stopped here...

	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *nats.Conn) {
		dch <- true
	}
	Wait(dch)

	if err := ec.Publish("foo", testString); err != nil {
		t.Fatalf("Failed to publish message: %v\n", err)
	}

	ts = startReconnectServer(t)
	defer ts.Shutdown()

	if err := ec.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}

	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive our message")
	}

	expectedReconnectCount := uint64(1)
	if ec.Conn.Reconnects != expectedReconnectCount {
		t.Fatalf("Reconnect count incorrect: %d vs %d\n",
			ec.Conn.Reconnects, expectedReconnectCount)
	}
	nc.Close()
}

func TestExtendedReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)

	opts := reconnectOpts
	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *nats.Conn) {
		dch <- true
	}
	rch := make(chan bool)
	opts.ReconnectedCB = func(_ *nats.Conn) {
		rch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	ec, err := nats.NewEncodedConn(nc, nats.DEFAULT_ENCODER)
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

	ts.Shutdown()
	// server is stopped here..

	// wait for disconnect
	if e := WaitTime(dch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	// Sub while disconnected
	ec.Subscribe("bar", func(s string) {
		atomic.AddInt32(&received, 1)
	})

	// Unsub foobar while disconnected
	sub.Unsubscribe()

	if err = ec.Publish("foo", testString); err != nil {
		t.Fatalf("Received an error after disconnect: %v\n", err)
	}

	if err = ec.Publish("bar", testString); err != nil {
		t.Fatalf("Received an error after disconnect: %v\n", err)
	}

	ts = startReconnectServer(t)
	defer ts.Shutdown()

	// server is restarted here..
	// wait for reconnect
	if e := WaitTime(rch, 2*time.Second); e != nil {
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

	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive our message")
	}

	// Sleep a bit to guarantee scheduler runs and process all subs.
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&received) != 4 {
		t.Fatalf("Received != %d, equals %d\n", 4, received)
	}
}

func TestQueueSubsOnReconnect(t *testing.T) {
	ts := startReconnectServer(t)

	opts := reconnectOpts

	// Allow us to block on reconnect complete.
	reconnectsDone := make(chan bool)
	opts.ReconnectedCB = func(nc *nats.Conn) {
		reconnectsDone <- true
	}

	// Helper to wait on a reconnect.
	waitOnReconnect := func() {
		select {
		case <-reconnectsDone:
			break
		case <-time.After(2 * time.Second):
			t.Fatalf("Expected a reconnect, timedout!\n")
		}
	}

	// Create connection
	nc, _ := opts.Connect()
	ec, err := nats.NewEncodedConn(nc, nats.JSON_ENCODER)
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}

	// To hold results.
	results := make(map[int]int)
	var mu sync.Mutex

	// Make sure we got what we needed, 1 msg only and all seqnos accounted for..
	checkResults := func(numSent int) {
		mu.Lock()
		defer mu.Unlock()

		for i := 0; i < numSent; i++ {
			if results[i] != 1 {
				t.Fatalf("Received incorrect number of messages, [%d] for seq: %d\n", results[i], i)
			}
		}

		// Auto reset results map
		results = make(map[int]int)
	}

	subj := "foo.bar"
	qgroup := "workers"

	cb := func(seqno int) {
		mu.Lock()
		defer mu.Unlock()
		results[seqno] = results[seqno] + 1
	}

	// Create Queue Subscribers
	ec.QueueSubscribe(subj, qgroup, cb)
	ec.QueueSubscribe(subj, qgroup, cb)

	ec.Flush()

	// Helper function to send messages and check results.
	sendAndCheckMsgs := func(numToSend int) {
		for i := 0; i < numToSend; i++ {
			ec.Publish(subj, i)
		}
		// Wait for processing.
		ec.Flush()
		time.Sleep(50 * time.Millisecond)

		// Check Results
		checkResults(numToSend)
	}

	// Base Test
	sendAndCheckMsgs(10)

	// Stop and restart server
	ts.Shutdown()
	ts = startReconnectServer(t)
	defer ts.Shutdown()

	waitOnReconnect()

	// Reconnect Base Test
	sendAndCheckMsgs(10)
}

func TestIsClosed(t *testing.T) {
	ts := startReconnectServer(t)
	nc := NewConnection(t, 22222)
	if nc.IsClosed() == true {
		t.Fatalf("IsClosed returned true when the connection is still open.")
	}
	ts.Shutdown()
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
	ts.Shutdown()
}

func TestIsReconnectingAndStatus(t *testing.T) {
	ts := startReconnectServer(t)
	// This will kill the last 'ts' server that is created
	defer func() { ts.Shutdown() }()
	disconnectedch := make(chan bool)
	reconnectch := make(chan bool)
	opts := nats.DefaultOptions
	opts.Url = "nats://localhost:22222"
	opts.AllowReconnect = true
	opts.MaxReconnect = 10000
	opts.ReconnectWait = 100 * time.Millisecond

	opts.DisconnectedCB = func(_ *nats.Conn) {
		disconnectedch <- true
	}
	opts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect, verify initial reconnecting state check, then stop the server
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	if nc.IsReconnecting() == true {
		t.Fatalf("IsReconnecting returned true when the connection is still open.")
	}
	if status := nc.Status(); status != nats.CONNECTED {
		t.Fatalf("Status returned %d when connected instead of CONNECTED", status)
	}
	ts.Shutdown()

	// Wait until we get the disconnected callback
	if e := Wait(disconnectedch); e != nil {
		t.Fatalf("Disconnect callback wasn't triggered: %v", e)
	}
	if nc.IsReconnecting() == false {
		t.Fatalf("IsReconnecting returned false when the client is reconnecting.")
	}
	if status := nc.Status(); status != nats.RECONNECTING {
		t.Fatalf("Status returned %d when reconnecting instead of CONNECTED", status)
	}

	ts = startReconnectServer(t)

	// Wait until we get the reconnect callback
	if e := Wait(reconnectch); e != nil {
		t.Fatalf("Reconnect callback wasn't triggered: %v", e)
	}
	if nc.IsReconnecting() == true {
		t.Fatalf("IsReconnecting returned true after the connection was reconnected.")
	}
	if status := nc.Status(); status != nats.CONNECTED {
		t.Fatalf("Status returned %d when reconnected instead of CONNECTED", status)
	}

	// Close the connection, reconnecting should still be false
	nc.Close()
	if nc.IsReconnecting() == true {
		t.Fatalf("IsReconnecting returned true after Close() was called.")
	}
	if status := nc.Status(); status != nats.CLOSED {
		t.Fatalf("Status returned %d after Close() was called instead of CLOSED", status)
	}
}
