package test

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/auth"
	gnatsd "github.com/nats-io/gnatsd/test"
	"github.com/nats-io/nats"
)

func TestAuth(t *testing.T) {
	opts := gnatsd.DefaultTestOptions
	opts.Port = 8232
	s := RunServerWithOptions(opts)

	// Auth is pluggable, so need to set here..
	auth := &auth.Plain{
		Username: "derek",
		Password: "foo",
	}
	s.SetAuthMethod(auth)

	defer s.Shutdown()

	_, err := nats.Connect("nats://localhost:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	nc, err := nats.Connect("nats://derek:foo@localhost:8232")
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()
}

func TestAuthFailNoDisconnectCB(t *testing.T) {
	opts := gnatsd.DefaultTestOptions
	opts.Port = 8232
	s := RunServerWithOptions(opts)

	// Auth is pluggable, so need to set here..
	auth := &auth.Plain{
		Username: "derek",
		Password: "foo",
	}
	s.SetAuthMethod(auth)

	defer s.Shutdown()

	copts := nats.DefaultOptions
	copts.Url = "nats://localhost:8232"
	receivedDisconnectCB := int32(0)
	copts.DisconnectedCB = func(nc *nats.Conn) {
		atomic.AddInt32(&receivedDisconnectCB, 1)
	}

	_, err := copts.Connect()
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	if atomic.LoadInt32(&receivedDisconnectCB) > 0 {
		t.Fatal("Should not have received a disconnect callback on auth failure")
	}
}

func TestAuthFailAllowReconnect(t *testing.T) {

	ts := startReconnectServer(t)
	defer ts.Shutdown()

	var servers = []string{
		"nats://localhost:22222",
		"nats://localhost:22223",
		"nats://localhost:22224",
	}

	srvOpts := gnatsd.DefaultTestOptions
	srvOpts.Port = 22223
	ts2 := RunServerWithOptions(srvOpts)

	// Auth is pluggable, so need to set here..
	auth := &auth.Plain{
		Username: "ivan",
		Password: "foo",
	}
	ts2.SetAuthMethod(auth)
	defer ts2.Shutdown()

	ts3 := RunServerOnPort(22224)
	defer ts3.Shutdown()

	reconnectch := make(chan bool)

	opts := nats.DefaultOptions
	opts.Servers = servers
	opts.AllowReconnect = true
	opts.NoRandomize = true
	opts.MaxReconnect = 10
	opts.ReconnectWait = 100 * time.Millisecond

	opts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	// Stop the server
	ts.Shutdown()

	// The client will try to connect to the second server, and that
	// should fail. It should then try to connect to the third and succeed.

	// Wait for the reconnect CB.
	if e := Wait(reconnectch); e != nil {
		t.Fatal("Reconnect callback should have been triggered")
	}

	if nc.IsClosed() {
		t.Fatal("Should have reconnected")
	}

	if nc.ConnectedUrl() != servers[2] {
		t.Fatalf("Should have reconnected to %s, reconnected to %s instead", servers[2], nc.ConnectedUrl())
	}
}
