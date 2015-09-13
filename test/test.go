// Copyright 2015 Apcera Inc. All rights reserved.

package test

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/gnatsd/server"
	"github.com/nats-io/nats"

	gnatsd "github.com/nats-io/gnatsd/test"
)

// Dumb wait program to sync on callbacks, etc... Will timeout
func Wait(ch chan bool) error {
	return WaitTime(ch, 500*time.Millisecond)
}

func WaitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func NewDefaultConnection(t *testing.T) *nats.Conn {
	return NewConnection(t, nats.DefaultPort)
}

func NewConnection(t *testing.T, port int) *nats.Conn {
	url := fmt.Sprintf("nats://localhost:%d", port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v\n", err)
		return nil
	}
	return nc
}

func RunDefaultServer() *server.Server {
	return RunServerOnPort(nats.DefaultPort)
}

func RunServerOnPort(port int) *server.Server {
	opts := gnatsd.DefaultTestOptions
	opts.Port = port
	return gnatsd.RunServer(&opts)
}
