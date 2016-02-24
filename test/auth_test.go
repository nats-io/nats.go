package test

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/auth"
	"github.com/nats-io/nats"
)

func TestAuth(t *testing.T) {
	s := RunServerOnPort(8232)

	// Auth is pluggable, so need to set here..
	auth := &auth.Plain{
		Username: "derek",
		Password: "foo",
	}
	s.SetClientAuthMethod(auth)

	defer s.Shutdown()

	_, err := nats.Connect("nats://localhost:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	// This test may be a bit too strict for the future, but for now makes
	// sure that we correctly process the -ERR content on connect.
	if err.Error() != nats.ErrAuthorization.Error() {
		t.Fatalf("Expected error '%v', got '%v'", nats.ErrAuthorization, err)
	}

	nc, err := nats.Connect("nats://derek:foo@localhost:8232")
	if err != nil {
		t.Fatal("Should have connected successfully with a token")
	}
	nc.Close()
}

func TestAuthFailNoDisconnectCB(t *testing.T) {
	s := RunServerOnPort(8232)

	// Auth is pluggable, so need to set here..
	auth := &auth.Plain{
		Username: "derek",
		Password: "foo",
	}
	s.SetClientAuthMethod(auth)

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
	ts := RunServerOnPort(23232)
	defer ts.Shutdown()

	var servers = []string{
		"nats://localhost:23232",
		"nats://localhost:23233",
		"nats://localhost:23234",
	}

	ts2 := RunServerOnPort(23233)
	// Auth is pluggable, so need to set here..
	auth := &auth.Plain{
		Username: "ivan",
		Password: "foo",
	}
	ts2.SetClientAuthMethod(auth)
	defer ts2.Shutdown()

	ts3 := RunServerOnPort(23234)
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

func TestTokenAuth(t *testing.T) {
	s := RunServerOnPort(8232)

	secret := "S3Cr3T0k3n!"
	// Auth is pluggable, so need to set here..
	auth := &auth.Token{Token: secret}
	s.SetClientAuthMethod(auth)

	defer s.Shutdown()

	_, err := nats.Connect("nats://localhost:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	tokenUrl := fmt.Sprintf("nats://%s@localhost:8232", secret)
	nc, err := nats.Connect(tokenUrl)
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()
}
