// Copyright 2013-2026 The NATS Authors
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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
	"github.com/nats-io/nkeys"
)

// Seeds and account JWT used by the trust-server reconnect tests. These mirror
// the constants in nats_test.go (which is gated by !testservice) so the
// testservice build can stand on its own.
var (
	tsOSeed = []byte("SOAL7GTNI66CTVVNXBNQMG6V2HTDRWC3HGEP7D2OUTWNWSNYZDXWFOX4SU")
	tsASeed = []byte("SAAASUPRY3ONU4GJR7J5RUVYRUFZXG56F4WEXELLLORQ65AEPSMIFTOJGE")
	tsUSeed = []byte("SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY")

	tsAJWT = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJLWjZIUVRXRlY3WkRZSFo3NklRNUhPM0pINDVRNUdJS0JNMzJTSENQVUJNNk5PNkU3TUhRIiwiaWF0IjoxNTQ0MDcxODg5LCJpc3MiOiJPRDJXMkk0TVZSQTVUR1pMWjJBRzZaSEdWTDNPVEtGV1FKRklYNFROQkVSMjNFNlA0NlMzNDVZWSIsInN1YiI6IkFBUFFKUVVQS1ZYR1c1Q1pINUcySEZKVUxZU0tERUxBWlJWV0pBMjZWRFpPN1dTQlVOSVlSRk5RIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6eyJzdWJzIjotMSwiY29ubiI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ3aWxkY2FyZHMiOnRydWV9fX0.8o35JPQgvhgFT84Bi2Z-zAeSiLrzzEZn34sgr1DIBEDTwa-EEiMhvTeos9cvXxoZVCCadqZxAWVwS6paAMj8Bg"
)

// trustServerOpts returns the testservice options needed to render a
// trusted-keys server with a MEM resolver pre-loaded with the test account
// JWT — the testservice equivalent of runTrustServer.
func trustServerOpts(t *testing.T) []testservice.CreateOption {
	t.Helper()
	okp, err := nkeys.FromSeed(tsOSeed)
	if err != nil {
		t.Fatalf("Error creating operator key pair: %v", err)
	}
	opub, err := okp.PublicKey()
	if err != nil {
		t.Fatalf("Error getting operator public key: %v", err)
	}
	akp, err := nkeys.FromSeed(tsASeed)
	if err != nil {
		t.Fatalf("Error creating account key pair: %v", err)
	}
	apub, err := akp.PublicKey()
	if err != nil {
		t.Fatalf("Error getting account public key: %v", err)
	}
	body := fmt.Sprintf(`
trusted_keys: [%q]
resolver: MEM
resolver_preload: {
  %q: %q
}
`, opub, apub, tsAJWT)
	return []testservice.CreateOption{
		testservice.WithTopLevel(body),
		// The default rendered config has an accounts block, an authorization
		// block with no_auth_user, and a system_account — all of which
		// conflict with operator-mode trust. Strip all three.
		testservice.WithAccounts(`# accounts intentionally empty; using operator/account JWT trust`),
		testservice.WithAuthorization(`# authorization intentionally empty; using operator/account JWT trust`),
		testservice.WithSystemAccount(`# system_account intentionally unset`),
	}
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
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		ch := make(chan bool)
		opts := nats.GetDefaultOptions()
		opts.Url = inst.Servers[0].URL
		opts.AllowReconnect = false
		opts.ClosedCB = func(_ *nats.Conn) {
			ch <- true
		}
		nc, err := opts.Connect()
		if err != nil {
			t.Fatalf("Should have connected ok: %v", err)
		}
		defer nc.Close()

		inst.StopServer(t, inst.Servers[0])

		if e := Wait(ch); e != nil {
			t.Fatal("Did not trigger ClosedCB correctly")
		}
	})
}

func TestReconnectAllowedFlags(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		ch := make(chan bool)
		dch := make(chan bool)
		opts := nats.GetDefaultOptions()
		opts.Url = inst.Servers[0].URL
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

		inst.StopServer(t, inst.Servers[0])

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
	})
}

// reconnectOptsFor returns a reconnectOpts equivalent dialed against the given
// instance URL.
func reconnectOptsFor(url string) nats.Options {
	return nats.Options{
		Url:            url,
		AllowReconnect: true,
		MaxReconnect:   10,
		ReconnectWait:  100 * time.Millisecond,
		Timeout:        nats.DefaultTimeout,
	}
}

func TestConnCloseBreaksReconnectLoop(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		cch := make(chan bool)

		opts := reconnectOptsFor(inst.Servers[0].URL)
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
		inst.StopServer(t, inst.Servers[0])

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
	})
}

func TestBasicReconnectFunctionality(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		ch := make(chan bool)
		dch := make(chan bool, 2)

		opts := reconnectOptsFor(inst.Servers[0].URL)
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

		inst.StopServer(t, inst.Servers[0])
		// server is stopped here...

		if err := Wait(dch); err != nil {
			t.Fatalf("Did not get the disconnected callback on time\n")
		}

		if err := nc.Publish("foo", []byte("bar")); err != nil {
			t.Fatalf("Failed to publish message: %v\n", err)
		}

		inst.StartServer(t, inst.Servers[0])

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
	})
}

func TestExtendedReconnectFunctionality(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		opts := reconnectOptsFor(inst.Servers[0].URL)
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

		inst.StopServer(t, inst.Servers[0])
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

		inst.StartServer(t, inst.Servers[0])

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
	})
}

func TestQueueSubsOnReconnect(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		opts := reconnectOptsFor(inst.Servers[0].URL)

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

			for i := range numSent {
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
			for i := range numToSend {
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
		inst.StopServer(t, inst.Servers[0])
		inst.StartServer(t, inst.Servers[0])

		if err := Wait(reconnectsDone); err != nil {
			t.Fatal("Did not get the ReconnectedCB!")
		}

		// Reconnect Base Test
		sendAndCheckMsgs(10)
	})
}

func TestIsClosed(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.MaxReconnects(-1), nats.ReconnectWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Should have connected ok: %v", err)
		}
		defer nc.Close()

		if nc.IsClosed() {
			t.Fatalf("IsClosed returned true when the connection is still open.")
		}
		inst.StopServer(t, inst.Servers[0])
		if nc.IsClosed() {
			t.Fatalf("IsClosed returned true when the connection is still open.")
		}
		inst.StartServer(t, inst.Servers[0])
		if nc.IsClosed() {
			t.Fatalf("IsClosed returned true when the connection is still open.")
		}
		nc.Close()
		if !nc.IsClosed() {
			t.Fatalf("IsClosed returned false after Close() was called.")
		}
	})
}

func TestIsReconnectingAndStatus(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		disconnectedch := make(chan bool, 3)
		reconnectch := make(chan bool, 2)
		opts := nats.GetDefaultOptions()
		opts.Url = inst.Servers[0].URL
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
		inst.StopServer(t, inst.Servers[0])

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

		inst.StartServer(t, inst.Servers[0])

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
	})
}

func TestFullFlushChanDuringReconnect(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		reconnectch := make(chan bool, 2)

		opts := nats.GetDefaultOptions()
		opts.Url = inst.Servers[0].URL
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
		inst.StopServer(t, inst.Servers[0])

		// Continue sending while we are disconnected
		time.Sleep(time.Second)

		// Restart the server
		inst.StartServer(t, inst.Servers[0])

		// Wait for the reconnect CB to be invoked (but not for too long)
		if e := WaitTime(reconnectch, 5*time.Second); e != nil {
			t.Fatalf("Reconnect callback wasn't triggered: %v", e)
		}
		close(stop)
	})
}

func TestReconnectVerbose(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		o := nats.GetDefaultOptions()
		o.Url = inst.Servers[0].URL
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

		inst.StopServer(t, inst.Servers[0])
		inst.StartServer(t, inst.Servers[0])

		if e := Wait(rch); e != nil {
			t.Fatal("Should have reconnected ok")
		}

		err = nc.Flush()
		if err != nil {
			t.Fatalf("Error during flush: %v", err)
		}
	})
}

func TestReconnectBufSizeOption(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.ReconnectBufSize(32))
		if err != nil {
			t.Fatalf("Should have connected ok: %v", err)
		}
		defer nc.Close()

		if nc.Opts.ReconnectBufSize != 32 {
			t.Fatalf("ReconnectBufSize should be 32 but it is %d", nc.Opts.ReconnectBufSize)
		}
	})
}

func TestReconnectBufSize(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		o := nats.GetDefaultOptions()
		o.Url = inst.Servers[0].URL
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
		inst.StopServer(t, inst.Servers[0])

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
	})
}

func TestReconnectTLSHostNoIP(t *testing.T) {
	t.Skip("DIVERGENCE: the original test asymmetrically configures two clustered servers — A listens on `localhost:5222`, B on `127.0.0.1:5224` — so the cluster gossips an IP-only URL to a client that connected via hostname, exercising TLS reconnect with a cert that has no IP SANs. testservice's `CreateCluster` gives every server identical config, so the hostname-vs-IP asymmetry the test relies on cannot be reproduced without per-server config overrides upstream. The 14-line nats.go invariant this test guards (preserving the dialed hostname as tlsName when a gossiped IP URL is added to a secure pool) is now covered by the white-box test TestParseServerURLPreservesTLSName in the root nats package's nats_test.go, which exercises the same code path at parseServerURL with no integration scaffolding.")
	// Original embedded-server body preserved verbatim below for future
	// re-port (will not compile against the testservice-only branch — kept as
	// a comment so re-enabling the test does not require git archeology).
	/*
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
	*/
}

func TestConnCloseNoCallback(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		serverURL := inst.Servers[0].URL

		// create a connection that manually sets the options
		var conns []*nats.Conn
		cch := make(chan string, 2)
		opts := reconnectOptsFor(serverURL)
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
		nc2, err := nats.Connect(serverURL, nats.NoCallbacksAfterClientClose(),
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
	})
}

func TestReconnectBufSizeDisable(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		o := nats.GetDefaultOptions()
		o.Url = inst.Servers[0].URL

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
		inst.StopServer(t, inst.Servers[0])

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
	})
}

func TestAuthExpiredReconnect(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, trustServerOpts(t)...)
	t.Cleanup(func() { inst.Destroy(t) })

	clientURL := inst.Servers[0].URL

	_, err := nats.Connect(clientURL)
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}
	ukp, err := nkeys.FromSeed(tsUSeed)
	if err != nil {
		t.Fatalf("Error creating user key pair: %v", err)
	}
	upub, err := ukp.PublicKey()
	if err != nil {
		t.Fatalf("Error getting user public key: %v", err)
	}
	akp, err := nkeys.FromSeed(tsASeed)
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
		kp, _ := nkeys.FromSeed(tsUSeed)
		sig, _ := kp.Sign(nonce)
		return sig, nil
	}

	errCh := make(chan error, 1)
	nc, err := nats.Connect(clientURL, nats.UserJWT(jwtCB, sigCB), nats.ReconnectWait(100*time.Millisecond),
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
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.ReconnectWait(10*time.Second))
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
		inst.StopServer(t, inst.Servers[0])
		WaitOnChannel(t, newStatus, nats.RECONNECTING)
		_, err = sub.NextMsg(100 * time.Millisecond)
		if err == nil {
			t.Fatal("Expected error getting message")
		}

		// restart server
		inst.StartServer(t, inst.Servers[0])

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
	})
}

func TestForceReconnectDisallowReconnect(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.NoReconnect())
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
	})
}

func TestForceReconnectSubsequentCalls(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.ReconnectWait(10*time.Second))
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

		for range 10 {
			err = nc.ForceReconnect()
			if err != nil {
				t.Fatalf("Unexpected error on reconnect: %v", err)
			}
		}

		WaitOnChannel(t, newStatus, nats.RECONNECTING)
		WaitOnChannel(t, newStatus, nats.CONNECTED)

		// check that we did not try to reconnect again
		select {
		case <-newStatus:
			t.Fatal("Should not have received a new status")
		case <-time.After(200 * time.Millisecond):
		}

		// now force a reconnect again
		if err := nc.ForceReconnect(); err != nil {
			t.Fatalf("Unexpected error on reconnect: %v", err)
		}
		WaitOnChannel(t, newStatus, nats.RECONNECTING)
		WaitOnChannel(t, newStatus, nats.CONNECTED)
	})
}

func TestAuthExpiredForceReconnect(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, trustServerOpts(t)...)
	t.Cleanup(func() { inst.Destroy(t) })

	clientURL := inst.Servers[0].URL

	_, err := nats.Connect(clientURL)
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}
	ukp, err := nkeys.FromSeed(tsUSeed)
	if err != nil {
		t.Fatalf("Error creating user key pair: %v", err)
	}
	upub, err := ukp.PublicKey()
	if err != nil {
		t.Fatalf("Error getting user public key: %v", err)
	}
	akp, err := nkeys.FromSeed(tsASeed)
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
		kp, _ := nkeys.FromSeed(tsUSeed)
		sig, _ := kp.Sign(nonce)
		return sig, nil
	}

	errCh := make(chan error, 1)
	nc, err := nats.Connect(clientURL, nats.UserJWT(jwtCB, sigCB), nats.ReconnectWait(10*time.Second),
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

func TestAlwaysReconnectOnMaxConnectionsExceededErr(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, testservice.WithTopLevel("max_connections: 2"))
	t.Cleanup(func() { inst.Destroy(t) })

	srvURL := inst.Servers[0].URL

	clientOpts := func(name string) ([]nats.Option, chan error, chan struct{}, chan struct{}) {
		disconnectCh := make(chan error, 10)
		reconnectCh := make(chan struct{}, 10)
		closedCh := make(chan struct{}, 10)
		return []nats.Option{
			nats.MaxReconnects(-1),
			nats.ReconnectWait(10 * time.Millisecond),
			nats.Timeout(200 * time.Millisecond),
			nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
				clientID, _ := c.GetClientID()
				t.Logf("[%v] Disconnected: %v: %v", name, clientID, err)
				if err != nil {
					disconnectCh <- err
				}
			}),
			nats.ReconnectHandler(func(c *nats.Conn) {
				clientID, _ := c.GetClientID()
				t.Logf("[%v] Reconnected: %v", name, clientID)
				reconnectCh <- struct{}{}
			}),
			nats.ClosedHandler(func(c *nats.Conn) {
				clientID, _ := c.GetClientID()
				t.Logf("[%v] Closed: %v", name, clientID)
				closedCh <- struct{}{}
			}),
			nats.Name(name),
		}, disconnectCh, reconnectCh, closedCh
	}

	opts1, errCh1, reconnectCh1, _ := clientOpts("A")
	nc1, err := nats.Connect(srvURL, opts1...)
	if err != nil {
		t.Fatalf("Failed to connect first client: %v", err)
	}
	defer nc1.Close()

	opts2, errCh2, reconnectCh2, _ := clientOpts("B")
	nc2, err := nats.Connect(srvURL, opts2...)
	if err != nil {
		t.Fatalf("Failed to connect second client: %v", err)
	}
	defer nc2.Close()

	if err := nc1.Flush(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	if err := nc2.Flush(); err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	// Reload to lower max_connections.
	inst.UpdateServer(t, inst.Servers[0], testservice.WithTopLevel("max_connections: 1"))
	inst.ReloadServer(t, inst.Servers[0])

	select {
	case err := <-errCh1:
		if err != nats.ErrMaxConnectionsExceeded {
			t.Fatalf("Expected ErrMaxConnectionsExceeded, got %v", err)
		}
	case err := <-errCh2:
		if err != nats.ErrMaxConnectionsExceeded {
			t.Fatalf("Expected ErrMaxConnectionsExceeded, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for disconnect event")
	}
	if nc1.IsConnected() && nc2.IsConnected() {
		t.Fatalf("Expected a client to be disconnected")
	}

	// Reload to increase max_connections.
	inst.UpdateServer(t, inst.Servers[0], testservice.WithTopLevel("max_connections: 10"))
	inst.ReloadServer(t, inst.Servers[0])

	select {
	case <-reconnectCh1:
	case <-reconnectCh2:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for reconnect event")
	}
	if !(nc1.IsConnected() || nc2.IsConnected()) {
		t.Fatal("At least one client should have reconnected successfully")
	}
}

func TestAlwaysReconnectOnAccountMaxConnectionsExceededErr(t *testing.T) {
	c := newTester(t)
	initialAccounts := `accounts: {
  ACCT1: {
    users: [ {user: "user1", password: "pass1"} ]
    limits: { max_conn: 1 }
  },
  ACCT2: {
    users: [ {user: "user2", password: "pass2"} ]
    limits: { max_conn: 2 }
  }
}`
	inst := c.CreateServer(t, false,
		testservice.WithAccounts(initialAccounts),
		testservice.WithAuthorization("# no_auth_user intentionally unset"),
		testservice.WithSystemAccount(noSystemAccountBody()),
	)
	t.Cleanup(func() { inst.Destroy(t) })

	srvURL := inst.Servers[0].URL

	clientOpts := func(name string) ([]nats.Option, chan error, chan struct{}, chan struct{}) {
		disconnectCh := make(chan error, 10)
		reconnectCh := make(chan struct{}, 10)
		closedCh := make(chan struct{}, 10)
		return []nats.Option{
			nats.MaxReconnects(-1),
			nats.ReconnectWait(200 * time.Millisecond),
			nats.Timeout(200 * time.Millisecond),
			nats.DisconnectErrHandler(func(c *nats.Conn, err error) {
				clientID, _ := c.GetClientID()
				t.Logf("[%v] Disconnected: %v: %v", name, clientID, err)
				if err != nil {
					disconnectCh <- err
				}
			}),
			nats.ReconnectHandler(func(c *nats.Conn) {
				clientID, _ := c.GetClientID()
				t.Logf("[%v] Reconnected: %v", name, clientID)
				reconnectCh <- struct{}{}
			}),
			nats.ClosedHandler(func(c *nats.Conn) {
				clientID, _ := c.GetClientID()
				t.Logf("[%v] Closed: %v", name, clientID)
				closedCh <- struct{}{}
			}),
			nats.Name(name),
		}, disconnectCh, reconnectCh, closedCh
	}

	opts1, errCh1, reconnectCh1, _ := clientOpts("ACCT1:C:1")
	opts1 = append(opts1, nats.UserInfo("user1", "pass1"))
	nc1, err := nats.Connect(srvURL, opts1...)
	if err != nil {
		t.Fatalf("Failed to connect first client: %v", err)
	}
	defer nc1.Close()

	// Second connection to same account fails due to limit.
	opts2, _, _, _ := clientOpts("ACCT1:C:2")
	opts2 = append(opts2, nats.UserInfo("user1", "pass1"))
	_, err = nats.Connect(srvURL, opts2...)
	if err == nil {
		t.Fatal("Expected connection to fail due to account max connections limit")
	}
	if !strings.Contains(err.Error(), `maximum account active connections exceeded`) {
		t.Fatalf("Expected second connection to be properly rejected: %v", err)
	}

	// Loosen account limits via reload so a second client can join.
	loosenedAccounts := `accounts: {
  ACCT1: {
    users: [ {user: "user1", password: "pass1"} ]
    limits: { max_conn: 10 }
  },
  ACCT2: {
    users: [ {user: "user2", password: "pass2"} ]
    limits: { max_conn: 2 }
  }
}`
	inst.UpdateServer(t, inst.Servers[0],
		testservice.WithAccounts(loosenedAccounts),
		testservice.WithAuthorization("# no_auth_user intentionally unset"),
		testservice.WithSystemAccount(noSystemAccountBody()),
	)
	inst.ReloadServer(t, inst.Servers[0])

	if err := nc1.Flush(); err != nil {
		t.Fatalf("Failed to flush first connection: %v", err)
	}

	opts3, errCh3, reconnectCh3, _ := clientOpts("ACCT1:C:3")
	opts3 = append(opts3, nats.UserInfo("user1", "pass1"))
	nc3, err := nats.Connect(srvURL, opts3...)
	if err != nil {
		t.Fatalf("Failed to connect client after limit increase: %v", err)
	}
	defer nc3.Close()
	if err = nc3.Flush(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if !(nc1.IsConnected() && nc3.IsConnected()) {
		t.Errorf("Expected both clients to be connected")
	}

	// Tighten limits — server should kick one of the two active ACCT1 clients
	// with an account-limit error; the kicked client must NOT give up (we set
	// MaxReconnects(-1) above) — that's the always-reconnect semantic this
	// test is named for.
	tightenedAccounts := `accounts: {
  ACCT1: {
    users: [ {user: "user1", password: "pass1"} ]
    limits: { max_conn: 1 }
  },
  ACCT2: {
    users: [ {user: "user2", password: "pass2"} ]
    limits: { max_conn: 2 }
  }
}`
	inst.UpdateServer(t, inst.Servers[0],
		testservice.WithAccounts(tightenedAccounts),
		testservice.WithAuthorization("# no_auth_user intentionally unset"),
		testservice.WithSystemAccount(noSystemAccountBody()),
	)
	inst.ReloadServer(t, inst.Servers[0])

	select {
	case err := <-errCh1:
		if !strings.Contains(err.Error(), "maximum account active connections exceeded") {
			t.Fatalf("Expected account-limit error on nc1, got %v", err)
		}
	case err := <-errCh3:
		if !strings.Contains(err.Error(), "maximum account active connections exceeded") {
			t.Fatalf("Expected account-limit error on nc3, got %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for disconnect event after tighten")
	}
	if nc1.IsConnected() && nc3.IsConnected() {
		t.Fatal("Expected one ACCT1 client to be kicked after tighten")
	}

	// Re-loosen so the kicked client can reconnect. If MaxReconnects(-1)
	// honored the account-limit error correctly, the kicked client kept
	// retrying and now succeeds.
	inst.UpdateServer(t, inst.Servers[0],
		testservice.WithAccounts(loosenedAccounts),
		testservice.WithAuthorization("# no_auth_user intentionally unset"),
		testservice.WithSystemAccount(noSystemAccountBody()),
	)
	inst.ReloadServer(t, inst.Servers[0])

	select {
	case <-reconnectCh1:
	case <-reconnectCh3:
	case <-time.After(3 * time.Second):
		t.Fatal("Timed out waiting for reconnect event after re-loosen")
	}
	if !(nc1.IsConnected() || nc3.IsConnected()) {
		t.Fatal("At least one ACCT1 client should have reconnected successfully")
	}
}

func TestReconnectToServerCallback(t *testing.T) {
	t.Run("select custom server", func(t *testing.T) {
		c := newTester(t)
		srv1Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv1Inst.Destroy(t) })
		srv2Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv2Inst.Destroy(t) })

		url1 := srv1Inst.Servers[0].URL
		url2 := srv2Inst.Servers[0].URL

		var callbackCalled atomic.Int32
		reconnectCh := make(chan bool, 1)

		nc, err := nats.Connect(
			url1+","+url2,
			nats.ReconnectWait(100*time.Millisecond),
			nats.MaxReconnects(10),
			nats.DontRandomize(),
			nats.ReconnectToServer(func(servers []nats.Server, info nats.ServerInfo) (*nats.Server, time.Duration) {
				callbackCalled.Add(1)
				for i := range servers {
					if servers[i].URL.String() == url2 {
						return &servers[i], 0
					}
				}
				return &servers[0], 0
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				select {
				case reconnectCh <- true:
				default:
				}
			}),
		)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer nc.Close()

		if nc.ConnectedUrl() != url1 {
			t.Fatalf("Expected initial connection to %s, got %s", url1, nc.ConnectedUrl())
		}

		// try reconnecting a few times, we should end up on url2 each time
		for i := range 5 {
			if err := nc.ForceReconnect(); err != nil {
				t.Fatalf("Failed to force reconnect: %v", err)
			}

			select {
			case <-reconnectCh:
			case <-time.After(2 * time.Second):
				t.Fatal("Timed out waiting for reconnect")
			}

			if callbackCalled.Load() != int32(i+1) {
				t.Fatal("Expected ReconnectToServer callback to be called on each reconnect")
			}

			if nc.ConnectedUrl() != url2 {
				t.Fatalf("Expected connection to %s after callback selection, got %s", url2, nc.ConnectedUrl())
			}
		}
	})

	t.Run("wait before reconnect", func(t *testing.T) {
		c := newTester(t)
		srv1Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv1Inst.Destroy(t) })
		srv2Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv2Inst.Destroy(t) })

		url1 := srv1Inst.Servers[0].URL
		url2 := srv2Inst.Servers[0].URL

		reconnectCh := make(chan bool, 1)

		nc, err := nats.Connect(
			url1+","+url2,
			nats.ReconnectWait(2*time.Second),
			nats.MaxReconnects(10),
			nats.DontRandomize(),
			nats.ReconnectToServer(func(servers []nats.Server, info nats.ServerInfo) (*nats.Server, time.Duration) {
				for i := range servers {
					if servers[i].URL.String() == url2 {
						return &servers[i], 500 * time.Millisecond
					}
				}
				return &servers[0], 500 * time.Millisecond
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				select {
				case reconnectCh <- true:
				default:
				}
			}),
		)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer nc.Close()

		if nc.ConnectedUrl() != url1 {
			t.Fatalf("Expected initial connection to %s, got %s", url1, nc.ConnectedUrl())
		}
		now := time.Now()
		if err := nc.ForceReconnect(); err != nil {
			t.Fatalf("Failed to force reconnect: %v", err)
		}

		select {
		case <-reconnectCh:
			elapsed := time.Since(now)
			if elapsed < 500*time.Millisecond {
				t.Fatalf("Reconnect occurred too quickly, expected ~500ms, got %v", elapsed)
			}
			if elapsed > 1500*time.Millisecond {
				t.Fatalf("Reconnect took too long (used ReconnectWait instead of callback delay?), got %v", elapsed)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("Timed out waiting for reconnect")
		}
	})

	t.Run("server not in pool fires error callback", func(t *testing.T) {
		c := newTester(t)
		srv1Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv1Inst.Destroy(t) })
		srv2Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv2Inst.Destroy(t) })

		url1 := srv1Inst.Servers[0].URL
		url2 := srv2Inst.Servers[0].URL

		reconnectCh := make(chan bool, 1)
		var reconnectErr atomic.Value

		nc, err := nats.Connect(
			url1+","+url2,
			nats.ReconnectWait(50*time.Millisecond),
			nats.MaxReconnects(10),
			nats.DontRandomize(),
			nats.ReconnectToServer(func(servers []nats.Server, info nats.ServerInfo) (*nats.Server, time.Duration) {
				return &nats.Server{URL: &url.URL{Scheme: "nats", Host: "127.0.0.1:9999"}}, 0
			}),
			nats.ReconnectErrHandler(func(_ *nats.Conn, err error) {
				reconnectErr.Store(err)
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				select {
				case reconnectCh <- true:
				default:
				}
			}),
		)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer nc.Close()

		srv1Inst.StopServer(t, srv1Inst.Servers[0])

		select {
		case <-reconnectCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Timed out waiting for reconnect")
		}

		if nc.ConnectedUrl() != url2 {
			t.Fatalf("Expected connection to %s via default fallback, got %s", url2, nc.ConnectedUrl())
		}

		if storedErr, ok := reconnectErr.Load().(error); !ok {
			t.Fatalf("Expected an error in ReconnectErrHandler, got %v", storedErr)
		} else if !errors.Is(storedErr, nats.ErrServerNotInPool) {
			t.Fatalf("Expected ErrServerNotInPool, got %v", storedErr)
		}
	})

	t.Run("nil server falls back to default selection", func(t *testing.T) {
		c := newTester(t)
		srv1Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv1Inst.Destroy(t) })
		srv2Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv2Inst.Destroy(t) })

		url1 := srv1Inst.Servers[0].URL
		url2 := srv2Inst.Servers[0].URL

		reconnectCh := make(chan bool, 1)

		nc, err := nats.Connect(
			url1+","+url2,
			nats.ReconnectWait(50*time.Millisecond),
			nats.MaxReconnects(10),
			nats.DontRandomize(),
			nats.ReconnectToServer(func(servers []nats.Server, info nats.ServerInfo) (*nats.Server, time.Duration) {
				return nil, 0
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				select {
				case reconnectCh <- true:
				default:
				}
			}),
		)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer nc.Close()

		srv1Inst.StopServer(t, srv1Inst.Servers[0])

		select {
		case <-reconnectCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Timed out waiting for reconnect")
		}

		if nc.ConnectedUrl() != url2 {
			t.Fatalf("Expected connection to %s via default fallback, got %s", url2, nc.ConnectedUrl())
		}
	})
}

func TestSetServerPool(t *testing.T) {
	t.Run("reconnect to server from new pool", func(t *testing.T) {
		c := newTester(t)
		srv1Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv1Inst.Destroy(t) })
		srv2Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv2Inst.Destroy(t) })

		url1 := srv1Inst.Servers[0].URL
		url2 := srv2Inst.Servers[0].URL

		reconnectCh := make(chan bool, 1)
		disconnectCh := make(chan bool, 1)

		nc, err := nats.Connect(
			url1,
			nats.ReconnectWait(50*time.Millisecond),
			nats.MaxReconnects(10),
			nats.DisconnectErrHandler(func(_ *nats.Conn, _ error) {
				select {
				case disconnectCh <- true:
				default:
				}
			}),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				select {
				case reconnectCh <- true:
				default:
				}
			}),
		)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer nc.Close()

		if nc.ConnectedUrl() != url1 {
			t.Fatalf("Expected initial connection to %s, got %s", url1, nc.ConnectedUrl())
		}

		srv3Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv3Inst.Destroy(t) })
		url3 := srv3Inst.Servers[0].URL

		if err := nc.SetServerPool([]string{url2, url3}); err != nil {
			t.Fatalf("Failed to set server pool: %v", err)
		}

		srv1Inst.StopServer(t, srv1Inst.Servers[0])

		select {
		case <-disconnectCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Timed out waiting for disconnect")
		}

		select {
		case <-reconnectCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Timed out waiting for reconnect")
		}

		connectedURL := nc.ConnectedUrl()
		if connectedURL != url2 && connectedURL != url3 {
			t.Fatalf("Expected connection to %s or %s, got %s", url2, url3, connectedURL)
		}
	})

	t.Run("atomic operation - partial failure doesn't modify pool", func(t *testing.T) {
		withServer(t, func(t *testing.T, nc *nats.Conn) {
			originalPool := nc.ServerPool()
			if len(originalPool) != 1 {
				t.Fatalf("Expected 1 server in original pool, got %d", len(originalPool))
			}

			err := nc.SetServerPool([]string{
				"nats://localhost:4222",
				"invalid://bad url with spaces",
				"nats://localhost:4223",
			})
			if err == nil {
				t.Fatal("Expected error from invalid URL, got nil")
			}

			currentPool := nc.ServerPool()
			if len(currentPool) != len(originalPool) {
				t.Fatalf("Pool was modified on error: expected %d servers, got %d", len(originalPool), len(currentPool))
			}
			if currentPool[0].URL.String() != originalPool[0].URL.String() {
				t.Fatalf("Pool was modified on error: expected %s, got %s", originalPool[0].URL.String(), currentPool[0].URL.String())
			}
		})
	})

	t.Run("websocket URLs rejected for non-websocket connection", func(t *testing.T) {
		withServer(t, func(t *testing.T, nc *nats.Conn) {
			originalPool := nc.ServerPool()

			err := nc.SetServerPool([]string{"ws://localhost:8080"})
			if !errors.Is(err, nats.ErrMixingWebsocketSchemes) {
				t.Fatal("Expected error from websocket URL on non-websocket connection")
			}

			currentPool := nc.ServerPool()
			if len(currentPool) != len(originalPool) {
				t.Fatalf("Pool was modified on error: expected %d servers, got %d", len(originalPool), len(currentPool))
			}
		})
	})

	t.Run("preserves reconnect count for existing servers", func(t *testing.T) {
		c := newTester(t)
		srv1Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv1Inst.Destroy(t) })
		srv2Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv2Inst.Destroy(t) })

		url1 := srv1Inst.Servers[0].URL
		url2 := srv2Inst.Servers[0].URL

		reconnectCh := make(chan struct{}, 1)

		nc, err := nats.Connect(
			url1+","+url2,
			nats.ReconnectWait(50*time.Millisecond),
			nats.MaxReconnects(10),
			nats.DontRandomize(),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				reconnectCh <- struct{}{}
			}),
		)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		defer nc.Close()

		// Shut down srv2 so it fails when attempted during reconnect.
		// Then ForceReconnect: client tries url2 (fails, Reconnects++),
		// then url1 (succeeds, Reconnects=0). So url2 has non-zero count.
		srv2Inst.StopServer(t, srv2Inst.Servers[0])

		if err := nc.ForceReconnect(); err != nil {
			t.Fatalf("Failed to force reconnect: %v", err)
		}

		select {
		case <-reconnectCh:
		case <-time.After(2 * time.Second):
			t.Fatal("Timed out waiting for reconnect")
		}

		pool := nc.ServerPool()
		var srv2Reconnects int
		for _, s := range pool {
			if s.URL.String() == url2 {
				srv2Reconnects = s.Reconnects
				break
			}
		}
		if srv2Reconnects != 1 {
			t.Fatal("Expected one reconnect for srv2")
		}

		srv3Inst := c.CreateServer(t, false)
		t.Cleanup(func() { srv3Inst.Destroy(t) })
		url3 := srv3Inst.Servers[0].URL

		if err := nc.SetServerPool([]string{url1, url2, url3}); err != nil {
			t.Fatalf("Failed to set server pool: %v", err)
		}

		pool = nc.ServerPool()
		for _, s := range pool {
			if s.URL.String() == url2 {
				if s.Reconnects != srv2Reconnects {
					t.Fatalf("Expected reconnect count %d to be preserved, got %d", srv2Reconnects, s.Reconnects)
				}
				return
			}
		}
		t.Fatal("srv2 not found in pool after SetServerPool")
	})

	t.Run("server pool returns deep copy", func(t *testing.T) {
		withServer(t, func(t *testing.T, nc *nats.Conn) {
			pool := nc.ServerPool()
			originalURL := pool[0].URL.String()

			pool[0].URL.Host = "modified:9999"

			pool2 := nc.ServerPool()
			if pool2[0].URL.String() != originalURL {
				t.Fatalf("Internal pool was modified: expected %s, got %s", originalURL, pool2[0].URL.String())
			}
		})
	})
}
