package nats

import (
	"testing"
	"fmt"
	"net"
	"time"
	"strings"
	"bytes"
	"os/exec"
)

const natsServer = "nats-server"

type server struct {
	args []string
	cmd  *exec.Cmd
}

var s *server

func startServer(t *testing.T, port uint, other string) *server {
	var s server
	args := fmt.Sprintf("-p %d %s", port, other)
	s.args = strings.Split(args, " ")
	s.cmd = exec.Command(natsServer, s.args...)
	err := s.cmd.Start()
	if err != nil {
		s.cmd = nil
		t.Errorf("Could not start %s, is NATS installed and in path?", natsServer)
		return &s
	}
	// Give it time to start up
	start := time.Now()
	for {
		addr := fmt.Sprintf("localhost:%d", port)
		c, err := net.Dial("tcp", addr)
		if err != nil {
			time.Sleep(50 * time.Millisecond)
			if time.Since(start) > (5 * time.Second) {
				t.Fatalf("Timed out trying to connect to %s", natsServer)
				return nil
			}
		} else {
			c.Close()
			break
		}
	}
	return &s
}

func (s *server) stopServer() {
	if s.cmd != nil && s.cmd.Process != nil {
		s.cmd.Process.Kill()
	}
}

func newConnection(t *testing.T) *Conn {
	nc, err := Connect(DefaultURL)
	if err != nil {
		t.Fatal("Failed to create default connection", err)
		return nil
	}
	return nc
}

func TestDefaultConnection(t *testing.T) {
	s = startServer(t, DefaultPort, "")
	nc := newConnection(t)
	nc.Close()
}

func TestConnectionStatus(t *testing.T) {
	nc, err := Connect(DefaultURL)
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	if nc.status != CONNECTED {
		t.Fatal("Should have status set to CONNECTED")
	}
	nc.Close()
	if nc.status != CLOSED {
		t.Fatal("Should have status set to CLOSED")
	}
}

func TestConnClosedCB(t *testing.T) {
	cbCalled := false
	o := DefaultOptions
	o.Url = DefaultURL
	o.ClosedCB = func(_ *Conn) {
		cbCalled = true
	}
	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	nc.Close()
	if !cbCalled {
		t.Fatalf("Closed callback not triggered\n")
	}
}

func TestCloseDisconnectedCB(t *testing.T) {
	cbCalled := false
	o := DefaultOptions
	o.Url = DefaultURL
	o.DisconnectedCB = func(_ *Conn) {
		cbCalled = true
	}
	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	nc.Close()
	if !cbCalled {
		t.Fatalf("Disconnected callback not triggered\n")
	}
}

func TestServerStopDisconnectedCB(t *testing.T) {
	received := 0
	o := DefaultOptions
	o.Url = DefaultURL
	o.DisconnectedCB = func(nc *Conn) {
		if nc.status != DISCONNECTED {
			t.Fatal("Should have status set to DISCONNECTED")
		}
		received += 1
	}
	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}

	s.stopServer()

	// Wait for disconnect to be processed.
	time.Sleep(10 * time.Millisecond)

	if received == 0 {
		t.Fatalf("Disconnected callback not triggered\n")
	}
	if received > 1 {
		t.Fatalf("Disconnected callback called too many times: %d vs 1\n", received)
	}

	nc.Close()
}

func TestRestartServer(t *testing.T) {
	s = startServer(t, DefaultPort, "")
}

func TestServerSecureConnections(t *testing.T) {
	securePort := uint(2288)
	secureServer := startServer(t, securePort, "--ssl")
	defer secureServer.stopServer()
	secureUrl := fmt.Sprintf("nats://localhost:%d/", securePort)

	// Make sure this succeeds
	nc, err := SecureConnect(secureUrl)
	if err != nil {
		t.Fatal("Failed to create secure (TLS) connection", err)
	}
	omsg := []byte("Hello World")
	received := 0
	nc.Subscribe("foo", func(m *Msg) {
		received += 1
		if !bytes.Equal(m.Data, omsg) {
			t.Fatal("Message received does not match")
		}
	})
	err = nc.Publish("foo", omsg)
	if err != nil {
		t.Fatal("Failed to publish on secure (TLS) connection", err)
	}
	nc.Flush()
	nc.Close()

	// Test flag mismatch
	// Wanted but not available..
	nc, err = SecureConnect(DefaultURL)
	if err == nil || nc != nil || err != ErrSecureConnWanted {
		t.Fatalf("Should have failed to create connection: %v", err)
	}

	// Server required, but not requested.
	nc, err = Connect(secureUrl)
	if err == nil || nc != nil || err != ErrSecureConnRequired {
		t.Fatal("Should have failed to create secure (TLS) connection")
	}
}
