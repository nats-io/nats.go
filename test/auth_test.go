package test

import (
	"sync/atomic"
	"testing"

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
