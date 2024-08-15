// Copyright 2013-2023 The NATS Authors
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package test

import (
	"errors"
	"fmt"
	"net"
	"net/url"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func startReconnectServer(t *testing.T) *server.Server {
	return RunServerOnPort(TEST_PORT)
}

func TestReconnectTotalTime(t *testing.T) {
	opts := nats.GetDefaultOptions()
	totalReconnectTime := time.Duration(opts.MaxReconnect) * opts.ReconnectWait
	if totalReconnectTime < (2 * time.Minute) {
		t.Fatalf("Total reconnect time should be at least 2 mins: Currently %v\n",
			totalReconnectTime)
	}
}

func TestDefaultReconnectJitter(t *testing.T) {
	opts := nats.GetDefaultOptions()
	if opts.ReconnectJitter != nats.DefaultReconnectJitter {
		t.Fatalf("Expected default jitter for non TLS to be %v, got %v", nats.DefaultReconnectJitter, opts.ReconnectJitter)
	}
	if opts.ReconnectJitterTLS != nats.DefaultReconnectJitterTLS {
		t.Fatalf("Expected default jitter for TLS to be %v, got %v", nats.DefaultReconnectJitterTLS, opts.ReconnectJitterTLS)
	}
}

func TestReconnectDisallowedFlags(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	ch := make(chan bool)
	opts := nats.GetDefaultOptions()
	opts.Url = fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	opts.AllowReconnect = false
	opts.ClosedCB = func(_ *nats.Conn) {
		ch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	ts.Shutdown()

	if e := Wait(ch); e != nil {
		t.Fatal("Did not trigger ClosedCB correctly")
	}
}

func TestReconnectAllowedFlags(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()
	ch := make(chan bool)
	dch := make(chan bool)
	opts := nats.GetDefaultOptions()
	opts.Url = fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	opts.AllowReconnect = true
	opts.MaxReconnect = 2
	opts.ReconnectWait = 1 * time.Second
	nats.ReconnectJitter(0, 0)(&opts)

	opts.ClosedCB = func(_ *nats.Conn) {
		ch <- true
	}
	opts.DisconnectedErrCB = func(_ *nats.Conn, _ error) {
		dch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	ts.Shutdown()

	// We want wait to timeout here, and the connection
	// should not trigger the Close CB.
	if e := WaitTime(ch, 500*time.Millisecond); e == nil {
		t.Fatal("Triggered ClosedCB incorrectly")
	}

	// We should wait to get the disconnected callback to ensure
	// that we are in the process of reconnecting.
	if e := Wait(dch); e != nil {
		t.Fatal("DisconnectedErrCB should have been triggered")
	}

	if !nc.IsReconnecting() {
		t.Fatal("Expected to be in a reconnecting state")
	}

	// clear the CloseCB since ch will block
	nc.Opts.ClosedCB = nil
}

func TestConnCloseBreaksReconnectLoop(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	cch := make(chan bool)

	opts := reconnectOpts
	// Bump the max reconnect attempts
	opts.MaxReconnect = 100
	opts.ClosedCB = func(_ *nats.Conn) {
		cch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()
	nc.Flush()

	// Shutdown the server
	ts.Shutdown()

	// Wait a second, then close the connection
	time.Sleep(time.Second)

	// Close the connection, this should break the reconnect loop.
	// Do this in a go routine since the issue was that Close()
	// would block until the reconnect loop is done.
	go nc.Close()

	// Even on Windows (where a createConn takes more than a second)
	// we should be able to break the reconnect loop with the following
	// timeout.
	if err := WaitTime(cch, 3*time.Second); err != nil {
		t.Fatal("Did not get a closed callback")
	}
}

func TestBasicReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	ch := make(chan bool)
	dch := make(chan bool, 2)

	opts := reconnectOpts

	opts.DisconnectedErrCB = func(_ *nats.Conn, _ error) {
		dch <- true
	}

	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v\n", err)
	}
	defer nc.Close()

	testString := "bar"
	nc.Subscribe("foo", func(m *nats.Msg) {
		if string(m.Data) != testString {
			t.Fatal("String doesn't match")
		}
		ch <- true
	})
	nc.Flush()

	ts.Shutdown()
	// server is stopped here...

	if err := Wait(dch); err != nil {
		t.Fatalf("Did not get the disconnected callback on time\n")
	}

	if err := nc.Publish("foo", []byte("bar")); err != nil {
		t.Fatalf("Failed to publish message: %v\n", err)
	}

	ts = startReconnectServer(t)
	defer ts.Shutdown()

	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}

	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive our message")
	}

	expectedReconnectCount := uint64(1)
	reconnectCount := nc.Stats().Reconnects

	if reconnectCount != expectedReconnectCount {
		t.Fatalf("Reconnect count incorrect: %d vs %d\n",
			reconnectCount, expectedReconnectCount)
	}
}

func TestExtendedReconnectFunctionality(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	opts := reconnectOpts
	dch := make(chan bool, 2)
	opts.DisconnectedErrCB = func(_ *nats.Conn, _ error) {
		dch <- true
	}
	rch := make(chan bool, 1)
	opts.ReconnectedCB = func(_ *nats.Conn) {
		rch <- true
	}
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	testString := "bar"
	received := int32(0)

	nc.Subscribe("foo", func(*nats.Msg) {
		atomic.AddInt32(&received, 1)
	})

	sub, _ := nc.Subscribe("foobar", func(*nats.Msg) {
		atomic.AddInt32(&received, 1)
	})

	nc.Publish("foo", []byte(testString))
	nc.Flush()

	ts.Shutdown()
	// server is stopped here..

	// wait for disconnect
	if e := WaitTime(dch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	// Sub while disconnected
	nc.Subscribe("bar", func(*nats.Msg) {
		atomic.AddInt32(&received, 1)
	})

	// Unsub foobar while disconnected
	sub.Unsubscribe()

	if err = nc.Publish("foo", []byte(testString)); err != nil {
		t.Fatalf("Received an error after disconnect: %v\n", err)
	}

	if err = nc.Publish("bar", []byte(testString)); err != nil {
		t.Fatalf("Received an error after disconnect: %v\n", err)
	}

	ts = startReconnectServer(t)
	defer ts.Shutdown()

	// server is restarted here..
	// wait for reconnect
	if e := WaitTime(rch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a reconnect callback message")
	}

	if err = nc.Publish("foobar", []byte(testString)); err != nil {
		t.Fatalf("Received an error after server restarted: %v\n", err)
	}

	if err = nc.Publish("foo", []byte(testString)); err != nil {
		t.Fatalf("Received an error after server restarted: %v\n", err)
	}

	ch := make(chan bool)
	nc.Subscribe("done", func(*nats.Msg) {
		ch <- true
	})
	nc.Publish("done", nil)

	if e := Wait(ch); e != nil {
		t.Fatal("Did not receive our message")
	}

	// Sleep a bit to guarantee scheduler runs and process all subs.
	time.Sleep(50 * time.Millisecond)

	if atomic.LoadInt32(&received) != 4 {
		t.Fatalf("Received != %d, equals %d\n", 4, received)
	}
}

func TestQueueSubsOnReconnect(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	opts := reconnectOpts

	// Allow us to block on reconnect complete.
	reconnectsDone := make(chan bool)
	opts.ReconnectedCB = func(nc *nats.Conn) {
		reconnectsDone <- true
	}

	// Create connection
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v\n", err)
	}
	defer nc.Close()

	// To hold results.
	results := make(map[int]int)
	var mu sync.Mutex

	// Make sure we got what we needed, 1 msg only and all seqnos accounted for..
	checkResults := func(numSent int) {
		mu.Lock()
		defer mu.Unlock()

		for i := 0; i < numSent; i++ {
			if results[i] != 1 {
				t.Fatalf("Received incorrect number of messages, [%d] for seq: %d\n", results[i], i)
			}
		}

		// Auto reset results map
		results = make(map[int]int)
	}

	subj := "foo.bar"
	qgroup := "workers"

	cb := func(m *nats.Msg) {
		mu.Lock()
		defer mu.Unlock()
		seqno, err := strconv.Atoi(string(m.Data))
		if err != nil {
			t.Fatalf("Received an invalid sequence number: %v\n", err)
		}
		results[seqno] = results[seqno] + 1
	}

	// Create Queue Subscribers
	nc.QueueSubscribe(subj, qgroup, cb)
	nc.QueueSubscribe(subj, qgroup, cb)

	nc.Flush()

	// Helper function to send messages and check results.
	sendAndCheckMsgs := func(numToSend int) {
		for i := 0; i < numToSend; i++ {
			nc.Publish(subj, []byte(fmt.Sprint(i)))
		}
		// Wait for processing.
		nc.Flush()
		time.Sleep(50 * time.Millisecond)

		// Check Results
		checkResults(numToSend)
	}

	// Base Test
	sendAndCheckMsgs(10)

	// Stop and restart server
	ts.Shutdown()
	ts = startReconnectServer(t)
	defer ts.Shutdown()

	if err := Wait(reconnectsDone); err != nil {
		t.Fatal("Did not get the ReconnectedCB!")
	}

	// Reconnect Base Test
	sendAndCheckMsgs(10)
}

func TestIsClosed(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	nc := NewConnection(t, TEST_PORT)
	defer nc.Close()

	if nc.IsClosed() {
		t.Fatalf("IsClosed returned true when the connection is still open.")
	}
	ts.Shutdown()
	if nc.IsClosed() {
		t.Fatalf("IsClosed returned true when the connection is still open.")
	}
	ts = startReconnectServer(t)
	defer ts.Shutdown()
	if nc.IsClosed() {
		t.Fatalf("IsClosed returned true when the connection is still open.")
	}
	nc.Close()
	if !nc.IsClosed() {
		t.Fatalf("IsClosed returned false after Close() was called.")
	}
}

func TestIsReconnectingAndStatus(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	disconnectedch := make(chan bool, 3)
	reconnectch := make(chan bool, 2)
	opts := nats.GetDefaultOptions()
	opts.Url = fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	opts.AllowReconnect = true
	opts.MaxReconnect = 10000
	opts.ReconnectWait = 100 * time.Millisecond
	nats.ReconnectJitter(0, 0)(&opts)

	opts.DisconnectedErrCB = func(_ *nats.Conn, _ error) {
		disconnectedch <- true
	}
	opts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect, verify initial reconnecting state check, then stop the server
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	if nc.IsReconnecting() {
		t.Fatalf("IsReconnecting returned true when the connection is still open.")
	}
	if status := nc.Status(); status != nats.CONNECTED {
		t.Fatalf("Status returned %d when connected instead of CONNECTED", status)
	}
	ts.Shutdown()

	// Wait until we get the disconnected callback
	if e := Wait(disconnectedch); e != nil {
		t.Fatalf("Disconnect callback wasn't triggered: %v", e)
	}
	if !nc.IsReconnecting() {
		t.Fatalf("IsReconnecting returned false when the client is reconnecting.")
	}
	if status := nc.Status(); status != nats.RECONNECTING {
		t.Fatalf("Status returned %d when reconnecting instead of CONNECTED", status)
	}

	ts = startReconnectServer(t)
	defer ts.Shutdown()

	// Wait until we get the reconnect callback
	if e := Wait(reconnectch); e != nil {
		t.Fatalf("Reconnect callback wasn't triggered: %v", e)
	}
	if nc.IsReconnecting() {
		t.Fatalf("IsReconnecting returned true after the connection was reconnected.")
	}
	if status := nc.Status(); status != nats.CONNECTED {
		t.Fatalf("Status returned %d when reconnected instead of CONNECTED", status)
	}

	// Close the connection, reconnecting should still be false
	nc.Close()
	if nc.IsReconnecting() {
		t.Fatalf("IsReconnecting returned true after Close() was called.")
	}
	if status := nc.Status(); status != nats.CLOSED {
		t.Fatalf("Status returned %d after Close() was called instead of CLOSED", status)
	}
}

func TestFullFlushChanDuringReconnect(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	reconnectch := make(chan bool, 2)

	opts := nats.GetDefaultOptions()
	opts.Url = fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	opts.AllowReconnect = true
	opts.MaxReconnect = 10000
	opts.ReconnectWait = 100 * time.Millisecond
	nats.ReconnectJitter(0, 0)(&opts)

	opts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	// Channel used to make the go routine sending messages to stop.
	stop := make(chan bool)

	// While connected, publish as fast as we can
	go func() {
		for i := 0; ; i++ {
			_ = nc.Publish("foo", []byte("hello"))

			// Make sure we are sending at least flushChanSize (1024) messages
			// before potentially pausing.
			if i%2000 == 0 {
				select {
				case <-stop:
					return
				default:
					time.Sleep(100 * time.Millisecond)
				}
			}
		}
	}()

	// Send a bit...
	time.Sleep(500 * time.Millisecond)

	// Shut down the server
	ts.Shutdown()

	// Continue sending while we are disconnected
	time.Sleep(time.Second)

	// Restart the server
	ts = startReconnectServer(t)
	defer ts.Shutdown()

	// Wait for the reconnect CB to be invoked (but not for too long)
	if e := WaitTime(reconnectch, 5*time.Second); e != nil {
		t.Fatalf("Reconnect callback wasn't triggered: %v", e)
	}
	close(stop)
}

func TestReconnectVerbose(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	o := nats.GetDefaultOptions()
	o.ReconnectWait = 50 * time.Millisecond
	o.Verbose = true
	rch := make(chan bool)
	o.ReconnectedCB = func(_ *nats.Conn) {
		rch <- true
	}

	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	err = nc.Flush()
	if err != nil {
		t.Fatalf("Error during flush: %v", err)
	}

	s.Shutdown()
	s = RunDefaultServer()
	defer s.Shutdown()

	if e := Wait(rch); e != nil {
		t.Fatal("Should have reconnected ok")
	}

	err = nc.Flush()
	if err != nil {
		t.Fatalf("Error during flush: %v", err)
	}
}

func TestReconnectBufSizeOption(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect("nats://127.0.0.1:4222", nats.ReconnectBufSize(32))
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	if nc.Opts.ReconnectBufSize != 32 {
		t.Fatalf("ReconnectBufSize should be 32 but it is %d", nc.Opts.ReconnectBufSize)
	}
}

func TestReconnectBufSize(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	o := nats.GetDefaultOptions()
	o.ReconnectBufSize = 32 // 32 bytes

	dch := make(chan bool)
	o.DisconnectedErrCB = func(_ *nats.Conn, _ error) {
		dch <- true
	}

	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	err = nc.Flush()
	if err != nil {
		t.Fatalf("Error during flush: %v", err)
	}

	// Force disconnected state.
	s.Shutdown()

	if e := Wait(dch); e != nil {
		t.Fatal("DisconnectedErrCB should have been triggered")
	}

	msg := []byte("food") // 4 bytes paylaod, total proto is 16 bytes
	// These should work, 2X16 = 32
	if err := nc.Publish("foo", msg); err != nil {
		t.Fatalf("Failed to publish message: %v\n", err)
	}
	if err := nc.Publish("foo", msg); err != nil {
		t.Fatalf("Failed to publish message: %v\n", err)
	}

	// This should fail since we have exhausted the backing buffer.
	if err := nc.Publish("foo", msg); err == nil {
		t.Fatalf("Expected to fail to publish message: got no error\n")
	}
	nc.Buffered()
}

// When a cluster is fronted by a single DNS name (desired) but communicates IPs to clients (also desired),
// and we use TLS, we want to make sure we do the right thing connecting to an IP directly for TLS to work.
// The reason this may happen is that the cluster has a single DNS name and a single certificate, but the cluster
// wants to vend out IPs and not wait on DNS for topology changes and failover.
func TestReconnectTLSHostNoIP(t *testing.T) {
	sa, optsA := RunServerWithConfig("./configs/tls_noip_a.conf")
	defer sa.Shutdown()
	sb, optsB := RunServerWithConfig("./configs/tls_noip_b.conf")
	defer sb.Shutdown()

	// Wait for cluster to form.
	wait := time.Now().Add(2 * time.Second)
	for time.Now().Before(wait) {
		sanr := sa.NumRoutes()
		sbnr := sb.NumRoutes()
		if sanr == 1 && sbnr == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	endpoint := fmt.Sprintf("%s:%d", optsA.Host, optsA.Port)
	secureURL := fmt.Sprintf("tls://%s:%s@%s/", optsA.Username, optsA.Password, endpoint)

	dch := make(chan bool, 2)
	dcb := func(_ *nats.Conn, _ error) { dch <- true }
	rch := make(chan bool)
	rcb := func(_ *nats.Conn) { rch <- true }

	nc, err := nats.Connect(secureURL,
		nats.RootCAs("./configs/certs/ca.pem"),
		nats.DisconnectErrHandler(dcb),
		nats.ReconnectHandler(rcb))
	if err != nil {
		t.Fatalf("Failed to create secure (TLS) connection: %v", err)
	}
	defer nc.Close()

	// Wait for DiscoveredServers() to be 1.
	wait = time.Now().Add(2 * time.Second)
	for time.Now().Before(wait) {
		if len(nc.DiscoveredServers()) == 1 {
			break
		}
	}
	// Make sure this is the server B info, and that it is an IP.
	expectedDiscoverURL := fmt.Sprintf("tls://%s:%d", optsB.Host, optsB.Port)
	eurl, err := url.Parse(expectedDiscoverURL)
	if err != nil {
		t.Fatalf("Expected to parse discovered server URL: %v", err)
	}
	if addr := net.ParseIP(eurl.Hostname()); addr == nil {
		t.Fatalf("Expected the discovered server to be an IP, got %v", eurl.Hostname())
	}
	ds := nc.DiscoveredServers()
	if ds[0] != expectedDiscoverURL {
		t.Fatalf("Expected %q, got %q", expectedDiscoverURL, ds[0])
	}

	// Force us to switch servers.
	sa.Shutdown()

	if e := Wait(dch); e != nil {
		t.Fatal("DisconnectedErrCB should have been triggered")
	}
	if e := WaitTime(rch, time.Second); e != nil {
		t.Fatalf("ReconnectedCB should have been triggered: %v", nc.LastError())
	}
}

var reconnectOpts = nats.Options{
	Url:            fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT),
	AllowReconnect: true,
	MaxReconnect:   10,
	ReconnectWait:  100 * time.Millisecond,
	Timeout:        nats.DefaultTimeout,
}

func TestConnCloseNoCallback(t *testing.T) {
	ts := startReconnectServer(t)
	defer ts.Shutdown()

	// create a connection that manually sets the options
	var conns []*nats.Conn
	cch := make(chan string, 2)
	opts := reconnectOpts
	opts.ClosedCB = func(_ *nats.Conn) {
		cch <- "manual"
	}
	opts.NoCallbacksAfterClientClose = true
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	conns = append(conns, nc)

	// and another connection that uses the option
	nc2, err := nats.Connect(reconnectOpts.Url, nats.NoCallbacksAfterClientClose(),
		nats.ClosedHandler(func(_ *nats.Conn) {
			cch <- "opts"
		}))
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	conns = append(conns, nc2)

	// defer close() for safety, flush() and close()
	for _, c := range conns {
		defer c.Close()
		c.Flush()

		// Close the connection, we don't expect to get a notification
		c.Close()
	}

	// if the timeout happens we didn't get data from the channel
	// if we get a value from the channel that connection type failed.
	select {
	case <-time.After(500 * time.Millisecond):
		// test passed - we timed so no callback was called
	case what := <-cch:
		t.Fatalf("%s issued a callback and it shouldn't have", what)
	}
}

func TestReconnectBufSizeDisable(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	o := nats.GetDefaultOptions()

	// Disable buffering to always get a synchronous error when publish fails.
	o.ReconnectBufSize = -1

	dch := make(chan bool)
	o.DisconnectedErrCB = func(_ *nats.Conn, _ error) {
		dch <- true
	}

	nc, err := o.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	err = nc.Flush()
	if err != nil {
		t.Fatalf("Error during flush: %v", err)
	}

	// Force disconnected state.
	s.Shutdown()

	if e := Wait(dch); e != nil {
		t.Fatal("DisconnectedErrCB should have been triggered")
	}

	msg := []byte("food")
	if err := nc.Publish("foo", msg); err != nats.ErrReconnectBufExceeded {
		t.Fatalf("Unexpected error: %v\n", err)
	}
	got, _ := nc.Buffered()
	if got != 0 {
		t.Errorf("Unexpected buffered bytes: %v", got)
	}
}

func TestAuthExpiredReconnect(t *testing.T) {
	ts := runTrustServer()
	defer ts.Shutdown()

	_, err := nats.Connect(ts.ClientURL())
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}
	ukp, err := nkeys.FromSeed(uSeed)
	if err != nil {
		t.Fatalf("Error creating user key pair: %v", err)
	}
	upub, err := ukp.PublicKey()
	if err != nil {
		t.Fatalf("Error getting user public key: %v", err)
	}
	akp, err := nkeys.FromSeed(aSeed)
	if err != nil {
		t.Fatalf("Error creating account key pair: %v", err)
	}

	jwtCB := func() (string, error) {
		claims := jwt.NewUserClaims("test")
		claims.Expires = time.Now().Add(time.Second).Unix()
		claims.Subject = upub
		jwt, err := claims.Encode(akp)
		if err != nil {
			return "", err
		}
		return jwt, nil
	}
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, _ := nkeys.FromSeed(uSeed)
		sig, _ := kp.Sign(nonce)
		return sig, nil
	}

	errCh := make(chan error, 1)
	nc, err := nats.Connect(ts.ClientURL(), nats.UserJWT(jwtCB, sigCB), nats.ReconnectWait(100*time.Millisecond),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errCh <- err
		}))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	stasusCh := nc.StatusChanged(nats.RECONNECTING, nats.CONNECTED)
	select {
	case err := <-errCh:
		if !errors.Is(err, nats.ErrAuthExpired) {
			t.Fatalf("Expected auth expired error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Did not get the auth expired error")
	}
	WaitOnChannel(t, stasusCh, nats.RECONNECTING)
	WaitOnChannel(t, stasusCh, nats.CONNECTED)
	nc.Close()
}

func TestForceReconnect(t *testing.T) {
	s := RunDefaultServer()

	nc, err := nats.Connect(s.ClientURL(), nats.ReconnectWait(10*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}

	statusCh := nc.StatusChanged(nats.RECONNECTING, nats.CONNECTED)
	defer close(statusCh)
	newStatus := make(chan nats.Status, 10)
	// non-blocking channel, so we need to be constantly listening
	go func() {
		for {
			s, ok := <-statusCh
			if !ok {
				return
			}
			newStatus <- s
		}
	}()

	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	_, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	// Force a reconnect
	err = nc.ForceReconnect()
	if err != nil {
		t.Fatalf("Unexpected error on reconnect: %v", err)
	}

	WaitOnChannel(t, newStatus, nats.RECONNECTING)
	WaitOnChannel(t, newStatus, nats.CONNECTED)

	if err := nc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	_, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	// shutdown server and then force a reconnect
	s.Shutdown()
	WaitOnChannel(t, newStatus, nats.RECONNECTING)
	_, err = sub.NextMsg(100 * time.Millisecond)
	if err == nil {
		t.Fatal("Expected error getting message")
	}

	// restart server
	s = RunDefaultServer()
	defer s.Shutdown()

	if err := nc.ForceReconnect(); err != nil {
		t.Fatalf("Unexpected error on reconnect: %v", err)
	}
	// wait for the reconnect
	// because the connection has long ReconnectWait,
	// if force reconnect does not work, the test will timeout
	WaitOnChannel(t, newStatus, nats.CONNECTED)

	if err := nc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	_, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	nc.Close()
}

func TestForceReconnectDisallowReconnect(t *testing.T) {
	s := RunDefaultServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.NoReconnect())
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()

	statusCh := nc.StatusChanged(nats.RECONNECTING, nats.CONNECTED)
	defer close(statusCh)
	newStatus := make(chan nats.Status, 10)
	// non-blocking channel, so we need to be constantly listening
	go func() {
		for {
			s, ok := <-statusCh
			if !ok {
				return
			}
			newStatus <- s
		}
	}()

	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := nc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	_, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	// Force a reconnect
	err = nc.ForceReconnect()
	if err != nil {
		t.Fatalf("Unexpected error on reconnect: %v", err)
	}

	WaitOnChannel(t, newStatus, nats.RECONNECTING)
	WaitOnChannel(t, newStatus, nats.CONNECTED)

	if err := nc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	_, err = sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

}

func TestAuthExpiredForceReconnect(t *testing.T) {
	ts := runTrustServer()
	defer ts.Shutdown()

	_, err := nats.Connect(ts.ClientURL())
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}
	ukp, err := nkeys.FromSeed(uSeed)
	if err != nil {
		t.Fatalf("Error creating user key pair: %v", err)
	}
	upub, err := ukp.PublicKey()
	if err != nil {
		t.Fatalf("Error getting user public key: %v", err)
	}
	akp, err := nkeys.FromSeed(aSeed)
	if err != nil {
		t.Fatalf("Error creating account key pair: %v", err)
	}

	jwtCB := func() (string, error) {
		claims := jwt.NewUserClaims("test")
		claims.Expires = time.Now().Add(time.Second).Unix()
		claims.Subject = upub
		jwt, err := claims.Encode(akp)
		if err != nil {
			return "", err
		}
		return jwt, nil
	}
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, _ := nkeys.FromSeed(uSeed)
		sig, _ := kp.Sign(nonce)
		return sig, nil
	}

	errCh := make(chan error, 1)
	nc, err := nats.Connect(ts.ClientURL(), nats.UserJWT(jwtCB, sigCB), nats.ReconnectWait(10*time.Second),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errCh <- err
		}))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc.Close()
	statusCh := nc.StatusChanged(nats.RECONNECTING, nats.CONNECTED)
	defer close(statusCh)
	newStatus := make(chan nats.Status, 10)
	// non-blocking channel, so we need to be constantly listening
	go func() {
		for {
			s, ok := <-statusCh
			if !ok {
				return
			}
			newStatus <- s
		}
	}()
	time.Sleep(100 * time.Millisecond)
	select {
	case err := <-errCh:
		if !errors.Is(err, nats.ErrAuthExpired) {
			t.Fatalf("Expected auth expired error, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Did not get the auth expired error")
	}
	if err := nc.ForceReconnect(); err != nil {
		t.Fatalf("Unexpected error on reconnect: %v", err)
	}
	WaitOnChannel(t, newStatus, nats.RECONNECTING)
	WaitOnChannel(t, newStatus, nats.CONNECTED)
}
