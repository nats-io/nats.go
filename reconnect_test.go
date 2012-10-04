package nats

import (
	"testing"
	"time"
)

func startReconnectServer(t *testing.T) *server {
	return startServer(t, 22222, "-DV -l /tmp/rc.log")
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
	if nc, err := opts.Connect(); err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	} else {
		defer nc.Close()
	}
	ts.stopServer()
	if e := wait(ch); e != nil {
		t.Fatal("Did not trigger ClosedCB correctly")
	}
}

func TestReconnectAllowedFlags(t *testing.T) {
	ts := startReconnectServer(t)
	ch := make(chan bool)
	opts := DefaultOptions
	opts.Url = "nats://localhost:22222"
	opts.AllowReconnect = true
	opts.ClosedCB = func(_ *Conn) {
		println("INSIDE CLOSED CB in TEST")
		ch <- true
		println("Exiting CB")
	}
	nc, err := opts.Connect();
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}

	ts.stopServer()
	if e := wait(ch); e == nil {
		t.Fatal("Triggered ClosedCB incorrectly")
	}
	// clear the CloseCB since ch will block
	nc.opts.ClosedCB = nil
	nc.Close()
}

var reconnectOpts =  Options{
	Url: "nats://localhost:22222",
	AllowReconnect: true,
	MaxReconnect:   10,
	ReconnectWait:  100 * time.Millisecond,
	Timeout:        DefaultTimeout,
}

func TestBasicReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)
	opts := reconnectOpts
	nc, _ := opts.Connect()
	ec, err := NewEncodedConn(nc, "default")
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	testString := "bar"
	received := 0
	ec.Subscribe("foo", func(s string) {
		if s != testString {
			t.Fatal("String don't match")
		}
		received += 1
	})
	ts.stopServer()
	// server is stopped here..
	ec.Publish("foo", testString)

	ts = startReconnectServer(t)
	defer ts.stopServer()

	ec.FlushTimeout(1 * time.Second)

	if received != 1 {
		t.Fatalf("Received != %d, equals %d\n", 1, received)
	}
}

func TestExtendedReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)
	cbCalled := false
	rcbCalled := false

	opts := reconnectOpts
	opts.DisconnectedCB = func(_ *Conn) {
		cbCalled = true
	}
	opts.ReconnectedCB = func(_ *Conn) {
		rcbCalled = true
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
	received := 0

	ec.Subscribe("foo", func(s string) {
		if s != testString {
			t.Fatal("String don't match")
		}
		received += 1
	})

	sub, _ := ec.Subscribe("foobar", func(s string) {
		if s != testString {
			t.Fatal("String don't match")
		}
		received += 1
	})

	ec.Publish("foo", testString)
	ec.Flush()

	ts.stopServer()
	// server is stopped here..

	// Sub while disconnected
	ec.Subscribe("bar", func(s string) {
		if s != testString {
			t.Fatal("String don't match")
		}
		received += 1
	})

	// Unsub while disconnected
	sub.Unsubscribe()

	if err = ec.Publish("foo", testString); err != nil {
		t.Fatalf("Got an error after disconnect: %v\n", err)
	}

	if err = ec.Publish("bar", testString); err != nil {
		t.Fatalf("Got an error after disconnect: %v\n", err)
	}

	ts = startReconnectServer(t)
	defer ts.stopServer()
	// server is restarted here..

	if err = ec.Publish("foobar", testString); err != nil {
		t.Fatalf("Got an error after server restarted: %v\n", err)
	}

	if err = ec.Publish("foo", testString); err != nil {
		t.Fatalf("Got an error after server restarted: %v\n", err)
	}

	if err = ec.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Got an error after flush: %v\n", err)
	}
	time.Sleep(100*time.Millisecond)
	if received != 4 {
		t.Fatalf("Received != %d, equals %d\n", 4, received)
	}
	if !cbCalled {
		t.Fatal("Did not have DisconnectedCB called")
	}
	if !rcbCalled {
		t.Fatal("Did not have ReconnectedCB called")
	}
}
