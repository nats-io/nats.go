package nats

import (
	"reflect"
	"testing"
)

var testServers = []string{
	"nats://localhost:1222",
	"nats://localhost:1223",
	"nats://localhost:1224",
	"nats://localhost:1225",
	"nats://localhost:1226",
	"nats://localhost:1227",
	"nats://localhost:1228",
}

func TestServersRandomize(t *testing.T) {
	opts := DefaultOptions
	opts.Servers = testServers
	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	// Build []string from srvPool
	clientServers := []string{}
	for _, s := range nc.srvPool {
		clientServers = append(clientServers, s.url.String())
	}
	// In theory this could happen..
	if reflect.DeepEqual(testServers, clientServers) {
		t.Fatalf("ServerPool list not randomized\n")
	}

	// Now test that we do not randomize if proper flag is set.
	opts = DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true
	nc = &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	// Build []string from srvPool
	clientServers = []string{}
	for _, s := range nc.srvPool {
		clientServers = append(clientServers, s.url.String())
	}
	if !reflect.DeepEqual(testServers, clientServers) {
		t.Fatalf("ServerPool list should not be randomized\n")
	}
}

func TestServersOption(t *testing.T) {
	opts := DefaultOptions
	opts.NoRandomize = true

	_, err := opts.Connect()
	if err != ErrNoServers {
		t.Fatalf("Wrong error: '%s'\n", err)
	}
	opts.Servers = testServers
	_, err = opts.Connect()
	if err == nil || err != ErrNoServers {
		t.Fatal("Did not receive proper error: %v", err)
	}

	// Make sure we can connect to first server if running
	s1 := startServer(t, 1222, "")
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Could not connect: %v\n", err)
	}
	if nc.ConnectedUrl() != "nats://localhost:1222" {
		t.Fatalf("Does not report correct connection: %s\n",
			nc.ConnectedUrl())
	}
	nc.Close()
	s1.stopServer()

	// Make sure we can connect to a non first server if running
	s2 := startServer(t, 1223, "")
	nc, err = opts.Connect()
	if err != nil {
		t.Fatalf("Could not connect: %v\n", err)
	}
	if nc.ConnectedUrl() != "nats://localhost:1223" {
		t.Fatalf("Does not report correct connection: %s\n",
			nc.ConnectedUrl())
	}
	nc.Close()
	s2.stopServer()
}
