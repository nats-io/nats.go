
package nats

import (
	"testing"
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
}
