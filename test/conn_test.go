package test

import (
	"bytes"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/nats-io/nats"
)

func TestDefaultConnection(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	nc.Close()
}

func TestConnectionStatus(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	if nc.Status() != nats.CONNECTED {
		t.Fatal("Should have status set to CONNECTED")
	}
	nc.Close()
	if nc.Status() != nats.CLOSED {
		t.Fatal("Should have status set to CLOSED")
	}
}

func TestConnClosedCB(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	cbCalled := false
	o := nats.DefaultOptions
	o.Url = nats.DefaultURL
	o.ClosedCB = func(_ *nats.Conn) {
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
	s := RunDefaultServer()
	defer s.Shutdown()

	ch := make(chan bool)
	o := nats.DefaultOptions
	o.Url = nats.DefaultURL
	o.AllowReconnect = false
	o.DisconnectedCB = func(_ *nats.Conn) {
		ch <- true
	}
	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	nc.Close()
	if e := Wait(ch); e != nil {
		t.Fatal("Disconnected callback not triggered")
	}
}

func TestServerStopDisconnectedCB(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	ch := make(chan bool)
	o := nats.DefaultOptions
	o.Url = nats.DefaultURL
	o.AllowReconnect = false
	o.DisconnectedCB = func(nc *nats.Conn) {
		ch <- true
	}
	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	s.Shutdown()
	if e := Wait(ch); e != nil {
		t.Fatalf("Disconnected callback not triggered\n")
	}
}

func TestServerSecureConnections(t *testing.T) {
	s, opts := RunServerWithConfig("./configs/tls.conf")
	defer s.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	secureUrl := fmt.Sprintf("nats://%s:%s@%s/", opts.Username, opts.Password, endpoint)

	// Make sure this succeeds
	nc, err := nats.SecureConnect(secureUrl)
	if err != nil {
		t.Fatal("Failed to create secure (TLS) connection", err)
	}
	defer nc.Close()

	omsg := []byte("Hello World")
	received := 0
	nc.Subscribe("foo", func(m *nats.Msg) {
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

	// Server required, but not requested.
	nc, err = nats.Connect(secureUrl)
	if err == nil || nc != nil || err != nats.ErrSecureConnRequired {
		t.Fatal("Should have failed to create secure (TLS) connection")
	}

	// Test flag mismatch
	// Wanted but not available..
	ds := RunDefaultServer()
	defer ds.Shutdown()

	nc, err = nats.SecureConnect(nats.DefaultURL)
	if err == nil || nc != nil || err != nats.ErrSecureConnWanted {
		t.Fatalf("Should have failed to create connection: %v", err)
	}
}

func TestClosedConnections(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc := NewDefaultConnection(t)
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	if sub == nil {
		t.Fatal("Failed to create valid subscription")
	}

	// Test all API endpoints do the right thing with a closed connection.
	nc.Close()
	if err := nc.Publish("foo", nil); err != nats.ErrConnectionClosed {
		t.Fatalf("Publish on closed conn did not fail properly: %v\n", err)
	}
	if err := nc.PublishMsg(&nats.Msg{Subject: "foo"}); err != nats.ErrConnectionClosed {
		t.Fatalf("PublishMsg on closed conn did not fail properly: %v\n", err)
	}
	if err := nc.Flush(); err != nats.ErrConnectionClosed {
		t.Fatalf("Flush on closed conn did not fail properly: %v\n", err)
	}
	_, err := nc.Subscribe("foo", nil)
	if err != nats.ErrConnectionClosed {
		t.Fatalf("Subscribe on closed conn did not fail properly: %v\n", err)
	}
	_, err = nc.SubscribeSync("foo")
	if err != nats.ErrConnectionClosed {
		t.Fatalf("SubscribeSync on closed conn did not fail properly: %v\n", err)
	}
	_, err = nc.QueueSubscribe("foo", "bar", nil)
	if err != nats.ErrConnectionClosed {
		t.Fatalf("QueueSubscribe on closed conn did not fail properly: %v\n", err)
	}
	_, err = nc.Request("foo", []byte("help"), 10*time.Millisecond)
	if err != nats.ErrConnectionClosed {
		t.Fatalf("Request on closed conn did not fail properly: %v\n", err)
	}
	if _, err = sub.NextMsg(10); err != nats.ErrConnectionClosed {
		t.Fatalf("NextMessage on closed conn did not fail properly: %v\n", err)
	}
	if err = sub.Unsubscribe(); err != nats.ErrConnectionClosed {
		t.Fatalf("Unsubscribe on closed conn did not fail properly: %v\n", err)
	}
}

func TestErrOnConnectAndDeadlock(t *testing.T) {
	// We will hand run a fake server that will timeout and not return a proper
	// INFO proto. This is to test that we do not deadlock. Issue #18

	l, e := net.Listen("tcp", ":0")
	if e != nil {
		t.Fatal("Could not listen on an ephemeral port")
	}
	tl := l.(*net.TCPListener)
	defer tl.Close()

	addr := tl.Addr().(*net.TCPAddr)

	go func() {
		conn, err := l.Accept()
		if err != nil {
			t.Fatalf("Error accepting client connection: %v\n", err)
		}
		defer conn.Close()
		// Send back a mal-formed INFO.
		conn.Write([]byte("INFOZ \r\n"))
	}()

	// Used to synchronize
	ch := make(chan bool)

	go func() {
		natsUrl := fmt.Sprintf("nats://localhost:%d/", addr.Port)
		nc, err := nats.Connect(natsUrl)
		if err == nil {
			nc.Close()
			t.Fatal("Expected bad INFO err, got none")
		}
		ch <- true
	}()

	// Setup a timer to watch for deadlock
	select {
	case <-ch:
		break
	case <-time.After(time.Second):
		t.Fatalf("Connect took too long, deadlock?")
	}
}

func TestErrOnMaxPayloadLimit(t *testing.T) {
	expectedMaxPayload := int64(10)
	serverInfo := "INFO {\"server_id\":\"foobar\",\"version\":\"0.6.6\",\"go\":\"go1.5.1\",\"host\":\"%s\",\"port\":%d,\"auth_required\":false,\"ssl_required\":false,\"max_payload\":%d}\r\n"

	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal("Could not listen on an ephemeral port")
	}
	tl := l.(*net.TCPListener)
	defer tl.Close()

	addr := tl.Addr().(*net.TCPAddr)

	// Send back an INFO message with custom max payload size on connect.
	var conn net.Conn
	var err error

	go func() {
		conn, err = l.Accept()
		if err != nil {
			t.Fatalf("Error accepting client connection: %v\n", err)
		}
		defer conn.Close()
		info := fmt.Sprintf(serverInfo, addr.IP, addr.Port, expectedMaxPayload)
		conn.Write([]byte(info))

		// Read connect and ping commands sent from the client
		line := make([]byte, 111)
		_, err := conn.Read(line)
		if err != nil {
			t.Fatalf("Expected CONNECT and PING from client, got: %s", err)
		}
		conn.Write([]byte("PONG\r\n"))
		// Hang around a bit to not err on EOF in client.
		time.Sleep(250 * time.Millisecond)
	}()

	// Wait for server mock to start
	time.Sleep(100 * time.Millisecond)

	natsUrl := fmt.Sprintf("nats://%s:%d", addr.IP, addr.Port)
	opts := nats.DefaultOptions
	opts.Servers = []string{natsUrl}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Expected INFO message with custom max payload, got: %s", err)
	}
	defer nc.Close()

	got := nc.MaxPayload()
	if got != expectedMaxPayload {
		t.Fatalf("Expected MaxPayload to be %d, got: %d", expectedMaxPayload, got)
	}
	err = nc.Publish("hello", []byte("hello world"))
	if err != nats.ErrMaxPayload {
		t.Fatalf("Expected to fail trying to send more than max payload, got: %s", err)
	}
	err = nc.Publish("hello", []byte("a"))
	if err != nil {
		t.Fatalf("Expected to succeed trying to send less than max payload, got: %s", err)
	}
}

func TestConnectVerbose(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	o := nats.DefaultOptions
	o.Verbose = true

	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	nc.Close()
}
