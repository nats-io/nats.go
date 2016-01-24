package test

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"runtime"
	"strconv"
	"strings"
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

	ch := make(chan bool)
	o := nats.DefaultOptions
	o.Url = nats.DefaultURL
	o.ClosedCB = func(_ *nats.Conn) {
		ch <- true
	}
	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	nc.Close()
	if e := Wait(ch); e != nil {
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
	secureURL := fmt.Sprintf("nats://%s:%s@%s/", opts.Username, opts.Password, endpoint)

	// Make sure this succeeds
	nc, err := nats.Connect(secureURL, nats.Secure())
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
	nc, err = nats.Connect(secureURL)
	if err == nil || nc != nil || err != nats.ErrSecureConnRequired {
		t.Fatal("Should have failed to create secure (TLS) connection")
	}

	// Test flag mismatch
	// Wanted but not available..
	ds := RunDefaultServer()
	defer ds.Shutdown()

	nc, err = nats.Connect(nats.DefaultURL, nats.Secure())
	if err == nil || nc != nil || err != nats.ErrSecureConnWanted {
		t.Fatalf("Should have failed to create connection: %v", err)
	}

	// Let's be more TLS correct and verify servername, endpoint etc.
	// Now do more advanced checking, verifying servername and using rootCA.
	// Setup our own TLSConfig using RootCA from our self signed cert.
	rootPEM, err := ioutil.ReadFile("./configs/certs/ca.pem")
	if err != nil || rootPEM == nil {
		t.Fatalf("failed to read root certificate")
	}
	pool := x509.NewCertPool()
	ok := pool.AppendCertsFromPEM([]byte(rootPEM))
	if !ok {
		t.Fatalf("failed to parse root certificate")
	}

	tls := &tls.Config{
		ServerName: opts.Host,
		RootCAs:    pool,
		MinVersion: tls.VersionTLS12,
	}

	nc, err = nats.Connect(secureURL, nats.Secure(tls))
	if err != nil {
		t.Fatalf("Got an error on Connect with Secure Options: %+v\n", err)
	}
	defer nc.Close()
}

func TestServerTLSHintConnections(t *testing.T) {
	s, opts := RunServerWithConfig("./configs/tls.conf")
	defer s.Shutdown()

	endpoint := fmt.Sprintf("%s:%d", opts.Host, opts.Port)
	secureURL := fmt.Sprintf("tls://%s:%s@%s/", opts.Username, opts.Password, endpoint)

	nc, err := nats.Connect(secureURL, nats.RootCAs("./configs/certs/badca.pem"))
	if err == nil {
		t.Fatal("Expected an error from bad RootCA file")
	}

	nc, err = nats.Connect(secureURL, nats.RootCAs("./configs/certs/ca.pem"))
	if err != nil {
		t.Fatal("Failed to create secure (TLS) connection", err)
	}
	defer nc.Close()
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
		natsURL := fmt.Sprintf("nats://localhost:%d/", addr.Port)
		nc, err := nats.Connect(natsURL)
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

	natsURL := fmt.Sprintf("nats://%s:%d", addr.IP, addr.Port)
	opts := nats.DefaultOptions
	opts.Servers = []string{natsURL}
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

func isRunningInAsyncCBDispatcher() error {
	var stacks []byte

	stacksSize := 10000

	for {
		stacks = make([]byte, stacksSize)
		n := runtime.Stack(stacks, false)
		if n == stacksSize {
			stacksSize *= stacksSize
			continue
		}
		break
	}

	strStacks := string(stacks)

	if strings.Contains(strStacks, "asyncDispatch") {
		return nil
	}

	return errors.New(fmt.Sprintf("Callback not executed from dispatcher:\n %s\n", strStacks))
}

func TestCallbacksOrder(t *testing.T) {
	authS, authSOpts := RunServerWithConfig("./configs/tls.conf")
	defer authS.Shutdown()

	s := RunDefaultServer()
	defer s.Shutdown()

	firstDisconnect := true
	dtime1 := time.Time{}
	dtime2 := time.Time{}
	rtime := time.Time{}
	atime1 := time.Time{}
	atime2 := time.Time{}
	ctime := time.Time{}

	cbErrors := make(chan error, 20)

	reconnected := make(chan bool)
	closed := make(chan bool)
	asyncErr := make(chan bool, 2)
	recvCh := make(chan bool, 2)
	recvCh1 := make(chan bool)
	recvCh2 := make(chan bool)

	dch := func(nc *nats.Conn) {
		if err := isRunningInAsyncCBDispatcher(); err != nil {
			cbErrors <- err
			return
		}
		time.Sleep(100 * time.Millisecond)
		if firstDisconnect {
			firstDisconnect = false
			dtime1 = time.Now()
		} else {
			dtime2 = time.Now()
		}
	}

	rch := func(nc *nats.Conn) {
		if err := isRunningInAsyncCBDispatcher(); err != nil {
			cbErrors <- err
			reconnected <- true
			return
		}
		time.Sleep(50 * time.Millisecond)
		rtime = time.Now()
		reconnected <- true
	}

	ech := func(nc *nats.Conn, sub *nats.Subscription, err error) {
		if err := isRunningInAsyncCBDispatcher(); err != nil {
			cbErrors <- err
			asyncErr <- true
			return
		}
		if sub.Subject == "foo" {
			time.Sleep(20 * time.Millisecond)
			atime1 = time.Now()
		} else {
			atime2 = time.Now()
		}
		asyncErr <- true
	}

	cch := func(nc *nats.Conn) {
		if err := isRunningInAsyncCBDispatcher(); err != nil {
			cbErrors <- err
			closed <- true
			return
		}
		ctime = time.Now()
		closed <- true
	}

	url := net.JoinHostPort(authSOpts.Host, strconv.Itoa(authSOpts.Port))
	url = "nats://" + url + "," + nats.DefaultURL

	nc, err := nats.Connect(url,
		nats.DisconnectHandler(dch),
		nats.ReconnectHandler(rch),
		nats.ClosedHandler(cch),
		nats.ErrorHandler(ech),
		nats.ReconnectWait(50*time.Millisecond),
		nats.DontRandomize())
	if err != nil {
		t.Fatalf("Unable to connect: %v\n", err)
	}
	defer nc.Close()

	ncp, err := nats.Connect(nats.DefaultURL,
		nats.ReconnectWait(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Unable to connect: %v\n", err)
	}
	defer ncp.Close()

	// Wait to make sure that if we have closed (incorrectly) the
	// asyncCBDispatcher during the connect process, this is caught here.
	time.Sleep(time.Second)

	s.Shutdown()

	s = RunDefaultServer()
	defer s.Shutdown()

	if err := Wait(reconnected); err != nil {
		t.Fatal("Did not get the reconnected callback")
	}

	var sub1 *nats.Subscription
	var sub2 *nats.Subscription

	recv := func(m *nats.Msg) {
		// Signal that one message is received
		recvCh <- true

		// We will now block
		if m.Subject == "foo" {
			<-recvCh1
		} else {
			<-recvCh2
		}
		m.Sub.Unsubscribe()
	}

	sub1, err = nc.Subscribe("foo", recv)
	if err != nil {
		t.Fatalf("Unable to create subscription: %v\n", err)
	}
	sub1.SetPendingLimits(1, 100000)

	sub2, err = nc.Subscribe("bar", recv)
	if err != nil {
		t.Fatalf("Unable to create subscription: %v\n", err)
	}
	sub2.SetPendingLimits(1, 100000)

	nc.Flush()

	ncp.Publish("foo", []byte("test"))
	ncp.Publish("bar", []byte("test"))
	ncp.Flush()

	// Wait notification that message were received
	err = Wait(recvCh)
	if err == nil {
		err = Wait(recvCh)
	}
	if err != nil {
		t.Fatal("Did not receive message")
	}

	for i := 0; i < 2; i++ {
		ncp.Publish("foo", []byte("test"))
		ncp.Publish("bar", []byte("test"))
	}
	ncp.Flush()

	if err := Wait(asyncErr); err != nil {
		t.Fatal("Did not get the async callback")
	}
	if err := Wait(asyncErr); err != nil {
		t.Fatal("Did not get the async callback")
	}

	close(recvCh1)
	close(recvCh2)

	nc.Close()

	if err := Wait(closed); err != nil {
		t.Fatal("Did not get the close callback")
	}

	if len(cbErrors) > 0 {
		t.Fatalf("%v", <-cbErrors)
	}

	if (dtime1 == time.Time{}) || (dtime2 == time.Time{}) || (rtime == time.Time{}) || (atime1 == time.Time{}) || (atime2 == time.Time{}) || (ctime == time.Time{}) {
		t.Fatalf("Some callbacks did not fire:\n%v\n%v\n%v\n%v\n%v\n%v", dtime1, rtime, atime1, atime2, dtime2, ctime)
	}

	if rtime.Before(dtime1) || dtime2.Before(rtime) || atime2.Before(atime1) || ctime.Before(atime2) {
		t.Fatalf("Wrong callback order:\n%v\n%v\n%v\n%v\n%v\n%v", dtime1, rtime, atime1, atime2, dtime2, ctime)
	}
}
