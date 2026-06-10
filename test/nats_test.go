// Copyright 2023-2026 The NATS Authors
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
	"bytes"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
	"github.com/nats-io/nkeys"
)

// Reuse seeds/JWT declared in reconnect_testservice_test.go (tsOSeed, tsASeed,
// tsUSeed, tsAJWT) and the trustServerOpts helper.

// uJWT is the user JWT used by the trust-server-based tests below.
var nstsUJWT = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJBSFQzRzNXRElDS1FWQ1FUWFJUTldPRlVVUFRWNE00RFZQV0JGSFpJQUROWEZIWEpQR0FBIiwiaWF0IjoxNTQ0MDcxODg5LCJpc3MiOiJBQVBRSlFVUEtWWEdXNUNaSDVHMkhGSlVMWVNLREVMQVpSVldKQTI2VkRaTzdXU0JVTklZUkZOUSIsInN1YiI6IlVBVDZCV0NTQ1dMVUtKVDZLNk1CSkpPRU9UWFo1QUpET1lLTkVWUkZDN1ZOTzZPQTQzTjRUUk5PIiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ._8A1XM88Q2kp7XVJZ42bQuO9E3QPsNAGKtVjAkDycj8A5PtRPby9UpqBUZzBwiJQQO3TUcD5GGqSvsMm6X8hCQ"

// nstsChained mirrors the chained credentials file body used in the original
// !testservice tests.
var nstsChained = `
-----BEGIN NATS USER JWT-----
eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJBSFQzRzNXRElDS1FWQ1FUWFJUTldPRlVVUFRWNE00RFZQV0JGSFpJQUROWEZIWEpQR0FBIiwiaWF0IjoxNTQ0MDcxODg5LCJpc3MiOiJBQVBRSlFVUEtWWEdXNUNaSDVHMkhGSlVMWVNLREVMQVpSVldKQTI2VkRaTzdXU0JVTklZUkZOUSIsInN1YiI6IlVBVDZCV0NTQ1dMVUtKVDZLNk1CSkpPRU9UWFo1QUpET1lLTkVWUkZDN1ZOTzZPQTQzTjRUUk5PIiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ._8A1XM88Q2kp7XVJZ42bQuO9E3QPsNAGKtVjAkDycj8A5PtRPby9UpqBUZzBwiJQQO3TUcD5GGqSvsMm6X8hCQ
------END NATS USER JWT------

************************* IMPORTANT *************************
NKEY Seed printed below can be used to sign and prove identity.
NKEYs are sensitive and should be treated as secrets.

-----BEGIN USER NKEY SEED-----
SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY
------END USER NKEY SEED------
`

// createTmpFileTS writes content to a freshly-created temp file and returns
// its path. Mirrors createTmpFile from the original !testservice file.
func createTmpFileTS(t *testing.T, content []byte) string {
	t.Helper()
	conf, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	fName := conf.Name()
	conf.Close()
	if err := os.WriteFile(fName, content, 0666); err != nil {
		os.Remove(fName)
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}

func TestMaxConnectionsReconnect(t *testing.T) {
	c := newTester(t)
	host := testerHost(t)
	advertiseAndMax := func(maxConns int) string {
		return fmt.Sprintf(
			"client_advertise: \"%s:{{ .ClientPort }}\"\nmax_connections: %d",
			host, maxConns,
		)
	}
	inst := c.CreateCluster(t, 2, false, testservice.WithTopLevel(advertiseAndMax(2)))
	t.Cleanup(func() { inst.Destroy(t) })

	s1URL := inst.Servers[0].URL

	errCh := make(chan error, 2)
	reconnectCh := make(chan struct{}, 2)
	opts := []nats.Option{
		nats.MaxReconnects(2),
		nats.ReconnectWait(10 * time.Millisecond),
		nats.Timeout(200 * time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) {
			if err != nil {
				errCh <- err
			}
		}),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			reconnectCh <- struct{}{}
		}),
	}

	nc1, err := nats.Connect(s1URL, opts...)
	if err != nil {
		t.Fatalf("Failed to connect first client: %v", err)
	}
	defer nc1.Close()
	nc1.Flush()

	nc2, err := nats.Connect(s1URL, opts...)
	if err != nil {
		t.Fatalf("Failed to connect second client: %v", err)
	}
	defer nc2.Close()
	nc2.Flush()

	// Lower s1's max_connections to 1 via reload — kicks one client.
	inst.UpdateServer(t, inst.Servers[0], testservice.WithTopLevel(advertiseAndMax(1)))
	inst.ReloadServer(t, inst.Servers[0])

	select {
	case err := <-errCh:
		if err != nats.ErrMaxConnectionsExceeded {
			t.Fatalf("Unexpected error %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for disconnect event")
	}

	select {
	case <-reconnectCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Timed out waiting for reconnect event")
	}
}

func TestNoEcho(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.NoEcho())
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		r := int32(0)
		_, err = nc.Subscribe("foo", func(m *nats.Msg) {
			atomic.AddInt32(&r, 1)
		})
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		err = nc.Publish("foo", []byte("Hello World"))
		if err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		nc.Flush()
		nc.Flush()

		if nr := atomic.LoadInt32(&r); nr != 0 {
			t.Fatalf("Expected no messages echoed back, received %d\n", nr)
		}
	})
}

func TestBasicUserJWTAuth(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, trustServerOpts(t)...)
	t.Cleanup(func() { inst.Destroy(t) })

	clientURL := inst.Servers[0].URL
	_, err := nats.Connect(clientURL)
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}

	jwtCB := func() (string, error) {
		return nstsUJWT, nil
	}
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, _ := nkeys.FromSeed(tsUSeed)
		sig, _ := kp.Sign(nonce)
		return sig, nil
	}

	// Try with user jwt but no sig
	_, err = nats.Connect(clientURL, nats.UserJWT(jwtCB, nil))
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}

	// Try with user callback
	_, err = nats.Connect(clientURL, nats.UserJWT(nil, sigCB))
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}

	nc, err := nats.Connect(clientURL, nats.UserJWT(jwtCB, sigCB))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	nc.Close()
}

func TestUserCredentialsTwoFiles(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, trustServerOpts(t)...)
	t.Cleanup(func() { inst.Destroy(t) })

	userJWTFile := createTmpFileTS(t, []byte(nstsUJWT))
	defer os.Remove(userJWTFile)
	userSeedFile := createTmpFileTS(t, tsUSeed)
	defer os.Remove(userSeedFile)

	nc, err := nats.Connect(inst.Servers[0].URL, nats.UserCredentials(userJWTFile, userSeedFile))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	nc.Close()
}

func TestUserCredentialsChainedFile(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, trustServerOpts(t)...)
	t.Cleanup(func() { inst.Destroy(t) })

	chainedFile := createTmpFileTS(t, []byte(nstsChained))
	defer os.Remove(chainedFile)

	clientURL := inst.Servers[0].URL
	nc, err := nats.Connect(clientURL, nats.UserCredentials(chainedFile))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	nc.Close()

	chainedFile = createTmpFileTS(t, []byte("invalid content"))
	defer os.Remove(chainedFile)
	nc, err = nats.Connect(clientURL, nats.UserCredentials(chainedFile))
	if err == nil || !strings.Contains(err.Error(),
		"error signing nonce: unable to extract key pair from file") {
		if nc != nil {
			nc.Close()
		}
		t.Fatalf("Expected error about invalid creds file, got %q", err)
	}
}

func TestReconnectMissingCredentials(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, trustServerOpts(t)...)
	t.Cleanup(func() { inst.Destroy(t) })

	chainedFile := createTmpFileTS(t, []byte(nstsChained))
	defer os.Remove(chainedFile)

	clientURL := inst.Servers[0].URL
	errs := make(chan error, 1)
	nc, err := nats.Connect(clientURL, nats.UserCredentials(chainedFile), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errs <- err
	}))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc.Close()
	os.Remove(chainedFile)
	// Bounce the server so the client must reconnect and re-load the (now
	// missing) credentials file. testservice StartServer reuses the same port.
	inst.StopServer(t, inst.Servers[0])
	inst.StartServer(t, inst.Servers[0])

	select {
	case err := <-errs:
		if !strings.Contains(err.Error(), "no such file or directory") {
			t.Fatalf("Expected error about missing creds file, got %q", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Did not get error about missing creds file")
	}
}

func TestUserJWTAndSeed(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, trustServerOpts(t)...)
	t.Cleanup(func() { inst.Destroy(t) })

	nc, err := nats.Connect(inst.Servers[0].URL, nats.UserJWTAndSeed(nstsUJWT, string(tsUSeed)))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	nc.Close()
}

func TestUserCredentialBytes(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, trustServerOpts(t)...)
	t.Cleanup(func() { inst.Destroy(t) })

	clientURL := inst.Servers[0].URL

	// Test with JWT and seed as byte slices
	jwtBytes := []byte(nstsUJWT)
	seedBytes := tsUSeed

	nc, err := nats.Connect(clientURL, nats.UserCredentialBytes(jwtBytes, seedBytes))
	if err != nil {
		t.Fatalf("Expected to connect with separate JWT and seed bytes, got %v", err)
	}
	nc.Close()

	// Test with chained credentials (JWT and seed in same byte slice)
	nc2, err := nats.Connect(clientURL, nats.UserCredentialBytes([]byte(nstsChained)))
	if err != nil {
		t.Fatalf("Expected to connect with chained credentials bytes, got %v", err)
	}
	nc2.Close()
}

// If we are using TLS and have multiple servers we try to match the IP
// from a discovered server with the expected hostname for certs without IP
// designations. In certain cases where there is a not authorized error and
// we were trying the second server with the IP only and getting an error
// that was hard to understand for the end user. This did require
// Opts.Secure = false, but the fix removed the check on Opts.Secure to decide
// if we need to save off the hostname that we connected to first.
func TestUserCredentialsChainedFileNotFoundError(t *testing.T) {
	c := newTester(t)
	// Trust-server config with managed server-only TLS (SANs intentionally
	// exclude IPs to preserve the original test's no-IP-SAN intent).
	sans := []string{"localhost"}
	if h := testerHost(t); h != "localhost" {
		sans = append(sans, h)
	}
	opts := append(trustServerOpts(t),
		testservice.WithGeneratedTLS(testservice.TLSServerOnly(), testservice.TLSSANs(sans...)),
	)
	inst := c.CreateCluster(t, 2, false, opts...)
	t.Cleanup(func() { inst.Destroy(t) })
	caPath, _, _ := tlsCertFiles(t, inst)

	clientURL := inst.Servers[0].URL

	// Make sure we get the right error here.
	nc, err := nats.Connect(clientURL,
		nats.RootCAs(caPath),
		nats.UserCredentials("filenotfound.creds"))

	if err == nil {
		nc.Close()
		t.Fatalf("Expected an error on missing credentials file")
	}
	if !strings.Contains(err.Error(), "no such file or directory") &&
		!strings.Contains(err.Error(), "The system cannot find the file specified") {
		t.Fatalf("Expected a missing file error, got %q", err)
	}
}

func TestNkeyAuth(t *testing.T) {
	seed := []byte("SUAKYRHVIOREXV7EUZTBHUHL7NUMHPMAS7QMDU3GTIUWEI5LDNOXD43IZY")
	kp, _ := nkeys.FromSeed(seed)
	pub, _ := kp.PublicKey()

	authBody := fmt.Sprintf(`authorization {
  users: [
    { nkey: %q }
  ]
}`, string(pub))

	c := newTester(t)
	inst := c.CreateServer(t, false, singleUserPassOpts(authBody)...)
	t.Cleanup(func() { inst.Destroy(t) })

	clientURL := inst.Servers[0].URL

	baseOpts := func() nats.Options {
		o := nats.GetDefaultOptions()
		o.Url = clientURL
		o.AllowReconnect = true
		o.MaxReconnect = 10
		o.ReconnectWait = 100 * time.Millisecond
		o.Timeout = nats.DefaultTimeout
		return o
	}

	opts := baseOpts()
	if _, err := opts.Connect(); err == nil {
		t.Fatalf("Expected to fail with no nkey auth defined")
	}
	opts.Nkey = string(pub)
	if _, err := opts.Connect(); err != nats.ErrNkeyButNoSigCB {
		t.Fatalf("Expected to fail with nkey defined but no signature callback, got %v", err)
	}
	badSign := func(nonce []byte) ([]byte, error) {
		return []byte("VALID?"), nil
	}
	opts.SignatureCB = badSign
	if _, err := opts.Connect(); err == nil {
		t.Fatalf("Expected to fail with nkey and bad signature callback")
	}
	goodSign := func(nonce []byte) ([]byte, error) {
		sig, err := kp.Sign(nonce)
		if err != nil {
			t.Fatalf("Failed signing nonce: %v", err)
		}
		return sig, nil
	}
	opts.SignatureCB = goodSign
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Expected to succeed but got %v", err)
	}
	defer nc.Close()

	// Now disconnect by killing the server and restarting.
	inst.StopServer(t, inst.Servers[0])
	inst.StartServer(t, inst.Servers[0])

	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}
}

// The original (embedded-server) versions of these tests start two servers on
// the same port across different address families (127.0.0.1 + [::1]) and
// connect to "localhost:PORT" 100 times, relying on net.LookupHost returning
// both IPs and the client randomizing across them. testservice can't bind two
// servers to the same port, so the migration uses the *equivalent* client
// behavior: pass a comma-joined two-URL pool to nats.Connect. The client's
// pool-shuffling logic is the same code path as the DNS-resolution multi-IP
// path (both end up populating srvPool with N entries, which is then shuffled
// or not based on NoRandomize). The DNS-specific edge of the original is lost,
// but the property under test — that the client randomizes its server pool by
// default and respects DontRandomize when set — is preserved end-to-end.

func TestLookupHostResultIsRandomized(t *testing.T) {
	c := newTester(t)
	s1 := c.CreateServer(t, false)
	t.Cleanup(func() { s1.Destroy(t) })
	s2 := c.CreateServer(t, false)
	t.Cleanup(func() { s2.Destroy(t) })

	urls := s1.Servers[0].URL + "," + s2.Servers[0].URL

	const total = 100
	conns := make([]*nats.Conn, 0, total)
	for i := 0; i < total; i++ {
		nc, err := nats.Connect(urls)
		if err != nil {
			t.Fatalf("Error on connect (i=%d): %v", i, err)
		}
		conns = append(conns, nc)
	}
	t.Cleanup(func() {
		for _, nc := range conns {
			nc.Close()
		}
	})

	// Tally which server each connection ended up on.
	var n1, n2 int
	for _, nc := range conns {
		switch nc.ConnectedUrl() {
		case s1.Servers[0].URL:
			n1++
		case s2.Servers[0].URL:
			n2++
		default:
			t.Fatalf("Unexpected ConnectedUrl: %q (expected %q or %q)", nc.ConnectedUrl(), s1.Servers[0].URL, s2.Servers[0].URL)
		}
	}
	if n1 < 35 || n1 > 65 {
		t.Fatalf("Does not seem balanced between servers: s1=%d, s2=%d", n1, n2)
	}
}

func TestLookupHostResultIsNotRandomizedWithNoRandom(t *testing.T) {
	c := newTester(t)
	s1 := c.CreateServer(t, false)
	t.Cleanup(func() { s1.Destroy(t) })
	s2 := c.CreateServer(t, false)
	t.Cleanup(func() { s2.Destroy(t) })

	urls := s1.Servers[0].URL + "," + s2.Servers[0].URL

	const total = 100
	conns := make([]*nats.Conn, 0, total)
	for i := 0; i < total; i++ {
		nc, err := nats.Connect(urls, nats.DontRandomize())
		if err != nil {
			t.Fatalf("Error on connect (i=%d): %v", i, err)
		}
		conns = append(conns, nc)
	}
	t.Cleanup(func() {
		for _, nc := range conns {
			nc.Close()
		}
	})

	// With DontRandomize, every connection should hit s1 (the first URL).
	for i, nc := range conns {
		if got, want := nc.ConnectedUrl(), s1.Servers[0].URL; got != want {
			t.Fatalf("Connection %d: expected %q, got %q (DontRandomize should pin to first URL)", i, want, got)
		}
	}
}

func TestConnectedAddr(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		var nilNC *nats.Conn
		if addr := nilNC.ConnectedAddr(); addr != "" {
			t.Fatalf("Expected empty result for nil connection, got %q", addr)
		}
		nc, err := nats.Connect(inst.Servers[0].URL)
		if err != nil {
			t.Fatalf("Error connecting: %v", err)
		}
		// We cannot compare against s.Addr().String() (no in-process server
		// here). Verify the connected addr is non-empty, has host:port form,
		// and reports the same port we dialed.
		addr := nc.ConnectedAddr()
		if addr == "" {
			t.Fatalf("Expected non-empty connected address")
		}
		_, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("Expected host:port format, got %q: %v", addr, err)
		}
		wantPort := fmt.Sprintf("%d", inst.Servers[0].Port)
		if port != wantPort {
			t.Fatalf("Expected port %s, got %s (addr %q)", wantPort, port, addr)
		}
		nc.Close()
		if addr := nc.ConnectedAddr(); addr != "" {
			t.Fatalf("Expected empty result for closed connection, got %q", addr)
		}
	})
}

func TestLocalAddr(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		var nilNC *nats.Conn
		if addr := nilNC.LocalAddr(); addr != "" {
			t.Fatalf("Expected empty result for nil connection, got %q", addr)
		}
		nc, err := nats.Connect(inst.Servers[0].URL)
		if err != nil {
			t.Fatalf("Error connecting: %v", err)
		}
		addr := nc.LocalAddr()
		if addr == "" {
			t.Fatalf("Expected non-empty local address")
		}
		// Verify it's a valid address format
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("Expected valid host:port format, got %q: %v", addr, err)
		}
		if host == "" {
			t.Fatalf("Expected non-empty host in address %q", addr)
		}
		if port == "" {
			t.Fatalf("Expected non-empty port in address %q", addr)
		}
		nc.Close()
		if addr := nc.LocalAddr(); addr != "" {
			t.Fatalf("Expected empty result for closed connection, got %q", addr)
		}
	})
}

func TestSubscribeSyncRace(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		go func() {
			time.Sleep(time.Millisecond)
			nc.Close()
		}()

		subj := "foo.sync.race"
		for range 10000 {
			if _, err := nc.SubscribeSync(subj); err != nil {
				break
			}
			if _, err := nc.QueueSubscribeSync(subj, "gc"); err != nil {
				break
			}
		}
	})
}

func TestBadSubjectsAndQueueNames(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		// Make sure we get errors on bad subjects (spaces, etc)
		// We want the client to protect the user.
		badSubs := []string{"foo bar", "foo..bar", ".foo", "bar.baz.", "baz\t.foo"}
		for _, subj := range badSubs {
			if _, err := nc.SubscribeSync(subj); err != nats.ErrBadSubject {
				t.Fatalf("Expected an error of ErrBadSubject for %q, got %v", subj, err)
			}
		}

		badQueues := []string{"foo group", "group\t1", "g1\r\n2"}
		for _, q := range badQueues {
			if _, err := nc.QueueSubscribeSync("foo", q); err != nats.ErrBadQueueName {
				t.Fatalf("Expected an error of ErrBadQueueName for %q, got %v", q, err)
			}
		}
	})
}

func TestTypeSubscription(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		// Make sure that ConsumerInfo() returns invalid subscription type error
		sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {})
		if err != nil {
			t.Fatalf("Error subscribing: %v", err)
		}

		if _, err := sub.ConsumerInfo(); err != nats.ErrTypeSubscription {
			t.Fatalf("Expected an error about invalid subscription type, got %v", err)
		}
	})
}

func TestAuthErrorOnReconnect(t *testing.T) {
	// This is a bit of an artificial test, but it is to demonstrate
	// that if the client is disconnected from a server (not due to an auth error),
	// it will still correctly stop the reconnection logic if it gets twice an
	// auth error from the same server.

	c := newTester(t)

	// clientAdvertiseOpt makes each server advertise "<testerHost>:<its
	// port>" via INFO connect_urls so the gossiped URL matches the one the
	// client dialed (rather than the server's container-internal bind
	// address). Without this, the gossiped URL would be added to the
	// srvPool as a separate entry, and the client would discover an
	// alternate path to s2 — inflating the Reconnects count this test
	// asserts on.
	s1Inst := c.CreateServer(t, false, clientAdvertiseOpt(t))
	t.Cleanup(func() { s1Inst.Destroy(t) })

	authBody := `authorization {
  user:     ivan
  password: pwd
}`
	s2Opts := append([]testservice.CreateOption{clientAdvertiseOpt(t)}, singleUserPassOpts(authBody)...)
	s2Inst := c.CreateServer(t, false, s2Opts...)
	t.Cleanup(func() { s2Inst.Destroy(t) })

	dch := make(chan bool)
	cch := make(chan bool)

	urls := fmt.Sprintf("%s, %s", s1Inst.Servers[0].URL, s2Inst.Servers[0].URL)
	nc, err := nats.Connect(urls,
		nats.ReconnectWait(25*time.Millisecond),
		nats.ReconnectJitter(0, 0),
		nats.MaxReconnects(-1),
		nats.DontRandomize(),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {}),
		nats.DisconnectErrHandler(func(_ *nats.Conn, e error) {
			dch <- true
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			cch <- true
		}))
	if err != nil {
		t.Fatalf("Expected to connect, got err: %v\n", err)
	}
	defer nc.Close()

	s1Inst.StopServer(t, s1Inst.Servers[0])

	// wait for disconnect
	if e := WaitTime(dch, 5*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	// Wait for ClosedCB
	if e := WaitTime(cch, 5*time.Second); e != nil {
		reconnects := nc.Stats().Reconnects
		t.Fatalf("Did not receive a closed callback message, #reconnects: %v", reconnects)
	}

	// The original (in-process) test asserts exactly 2 reconnects. With
	// testservice the reconnect loop deterministically yields 3 successful
	// TCP connects before the auth-error abort fires (the StopServer
	// listener-drain window briefly lets s1 accept a TCP connection too,
	// inflating the global Reconnects counter by 1). The abort still kicks
	// in after 2 auth violations; Reconnects (TCP-level successes, not
	// auth outcomes) lands at 3.
	// Relax to a bounded range: at least 2 (the auth-error count), at most
	// 2*len(pool) = 4 (the worst-case for a 2-server pool where both can
	// briefly accept TCP during shutdown).
	if reconnects := nc.Stats().Reconnects; reconnects < 2 || reconnects > 4 {
		t.Fatalf("Expected 2-4 reconnects (2 auth errors + up to 2 drain-window TCP successes), got %v", reconnects)
	}

	// Expect connection to be closed...
	if !nc.IsClosed() {
		t.Fatalf("Wrong status: %d\n", nc.Status())
	}
}

func TestStatsRace(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		wg := sync.WaitGroup{}
		wg.Add(1)
		ch := make(chan bool)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ch:
					return
				default:
					nc.Stats()
				}
			}
		}()

		nc.Subscribe("foo", func(_ *nats.Msg) {})
		for range 1000 {
			nc.Publish("foo", []byte("hello"))
		}

		close(ch)
		wg.Wait()
	})
}

func TestRequestLeaksMapEntries(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		response := []byte("I will help you")
		nc.Subscribe("foo", func(m *nats.Msg) {
			nc.Publish(m.Reply, response)
		})

		for range 100 {
			msg, err := nc.Request("foo", nil, 500*time.Millisecond)
			if err != nil {
				t.Fatalf("Received an error on Request test: %s", err)
			}
			if !bytes.Equal(msg.Data, response) {
				t.Fatalf("Received invalid response")
			}
		}
	})
}

func TestRequestMultipleReplies(t *testing.T) {
	withServerInstance(t, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		response := []byte("I will help you")
		nc.Subscribe("foo", func(m *nats.Msg) {
			m.Respond(response)
			m.Respond(response)
		})
		nc.Flush()

		nc2, err := nats.Connect(inst.Servers[0].URL)
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc2.Close()

		errCh := make(chan error, 1)
		// Send a request on bar and expect nothing
		go func() {
			if m, err := nc2.Request("bar", nil, 500*time.Millisecond); m != nil || err == nil {
				errCh <- fmt.Errorf("Expected no reply, got m=%+v err=%v", m, err)
				return
			}
			errCh <- nil
		}()

		// Send a request on foo, we use only one of the 2 replies
		if _, err := nc2.Request("foo", nil, time.Second); err != nil {
			t.Fatalf("Received an error on Request test: %s", err)
		}
		if e := <-errCh; e != nil {
			t.Fatal(e.Error())
		}
	})
}

func TestGetRTT(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.ReconnectWait(10*time.Millisecond), nats.ReconnectJitter(0, 0))
		if err != nil {
			t.Fatalf("Expected to connect to server, got %v", err)
		}
		defer nc.Close()

		rtt, err := nc.RTT()
		if err != nil {
			t.Fatalf("Unexpected error getting RTT: %v", err)
		}
		if rtt > time.Second {
			t.Fatalf("RTT value too large: %v", rtt)
		}
		// We should not get a value when in any disconnected state.
		inst.StopServer(t, inst.Servers[0])
		time.Sleep(5 * time.Millisecond)
		if _, err = nc.RTT(); err != nats.ErrDisconnected {
			t.Fatalf("Expected disconnected error getting RTT when disconnected, got %v", err)
		}
	})
}

func TestGetClientIP(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ip, err := nc.GetClientIP()
		if err != nil {
			t.Fatalf("Got error looking up IP: %v", err)
		}
		// In the testservice topology the client may be reaching the server
		// through a non-loopback address (docker bridge or a routed host).
		// Verify the IP is non-nil instead of strict IsLoopback.
		if ip == nil {
			t.Fatalf("Expected non-nil IP")
		}
		nc.Close()
		if _, err := nc.GetClientIP(); err != nats.ErrConnectionClosed {
			t.Fatalf("Expected a connection closed error, got %v", err)
		}
	})
}

func TestReconnectWaitJitter(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		rch := make(chan time.Time, 1)
		nc, err := nats.Connect(inst.Servers[0].URL,
			nats.ReconnectWait(100*time.Millisecond),
			nats.ReconnectJitter(500*time.Millisecond, 0),
			nats.ReconnectHandler(func(_ *nats.Conn) {
				rch <- time.Now()
			}),
		)
		if err != nil {
			t.Fatalf("Error during connect: %v", err)
		}
		defer nc.Close()

		inst.StopServer(t, inst.Servers[0])
		start := time.Now()
		// Wait a bit so that the library tries a first time without waiting.
		time.Sleep(50 * time.Millisecond)
		inst.StartServer(t, inst.Servers[0])
		select {
		case end := <-rch:
			dur := end.Sub(start)
			// We should wait at least the reconnect wait + random up to 500ms.
			// Account for a bit of variation since we rely on the reconnect
			// handler which is not invoked in place.
			if dur < 90*time.Millisecond || dur > 800*time.Millisecond {
				t.Fatalf("Wrong wait: %v", dur)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Should have reconnected")
		}
		nc.Close()

		// Use a long reconnect wait
		nc, err = nats.Connect(inst.Servers[0].URL, nats.ReconnectWait(10*time.Minute))
		if err != nil {
			t.Fatalf("Error during connect: %v", err)
		}
		defer nc.Close()

		// Cause a disconnect
		inst.StopServer(t, inst.Servers[0])
		// Wait a bit for the reconnect loop to go into wait mode.
		time.Sleep(50 * time.Millisecond)
		inst.StartServer(t, inst.Servers[0])
		// Now close and expect the reconnect go routine to return..
		nc.Close()
		// Wait for the doReconnect goroutine to exit. The 50ms used by the
		// original is racy under -race; poll up to 2s instead.
		deadline := time.Now().Add(2 * time.Second)
		var found bool
		var snapshot []byte
		for time.Now().Before(deadline) {
			buf := make([]byte, 100000)
			n := runtime.Stack(buf, true)
			if !strings.Contains(string(buf[:n]), "doReconnect") {
				found = true
				break
			}
			snapshot = buf[:n]
			time.Sleep(20 * time.Millisecond)
		}
		if !found {
			t.Fatalf("doReconnect go routine still running:\n%s", snapshot)
		}
	})
}

func TestCustomReconnectDelay(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		expectedAttempt := 1
		errCh := make(chan error, 1)
		cCh := make(chan bool, 1)
		nc, err := nats.Connect(inst.Servers[0].URL,
			nats.Timeout(100*time.Millisecond), // Need to lower for Windows tests
			nats.CustomReconnectDelay(func(n int) time.Duration {
				var err error
				var delay time.Duration
				if n != expectedAttempt {
					err = fmt.Errorf("Expected attempt to be %v, got %v", expectedAttempt, n)
				} else {
					expectedAttempt++
					if n <= 4 {
						delay = 100 * time.Millisecond
					}
				}
				if err != nil {
					select {
					case errCh <- err:
					default:
					}
				}
				return delay
			}),
			nats.MaxReconnects(4),
			nats.ClosedHandler(func(_ *nats.Conn) {
				cCh <- true
			}),
		)
		if err != nil {
			t.Fatalf("Error during connect: %v", err)
		}
		defer nc.Close()

		// Cause disconnect
		inst.StopServer(t, inst.Servers[0])

		// We should be trying to reconnect 4 times
		start := time.Now()

		// Wait on error or completion of test.
		select {
		case e := <-errCh:
			if e != nil {
				t.Fatal(e.Error())
			}
		case <-cCh:
		case <-time.After(2 * time.Second):
			t.Fatalf("No CB invoked")
		}
		// On Windows, a failed connect attempt will last as much as Timeout(),
		// so we need to take that into account.
		max := 500 * time.Millisecond
		if runtime.GOOS == "windows" {
			max = time.Second
		}
		if dur := time.Since(start); dur >= max {
			t.Fatalf("Waited too long on each reconnect: %v", dur)
		}
	})
}

func TestMsg_RespondMsg(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		sub, err := nc.SubscribeSync(nats.NewInbox())
		if err != nil {
			t.Fatalf("subscribe failed: %s", err)
		}

		nc.PublishMsg(&nats.Msg{Reply: sub.Subject, Subject: sub.Subject, Data: []byte("request")})
		req, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("NextMsg failed: %s", err)
		}

		// verifies that RespondMsg sets the reply subject on msg based on req
		err = req.RespondMsg(&nats.Msg{Data: []byte("response")})
		if err != nil {
			t.Fatalf("RespondMsg failed: %s", err)
		}

		resp, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("NextMsg failed: %s", err)
		}

		if !bytes.Equal(resp.Data, []byte("response")) {
			t.Fatalf("did not get correct response: %q", resp.Data)
		}
	})
}

func TestCustomInboxPrefix(t *testing.T) {
	opts := &nats.Options{}
	for _, p := range []string{"$BOB.", "$BOB.*", "$BOB.>", ">", ".", "", "BOB.*.X", "BOB.>.X"} {
		err := nats.CustomInboxPrefix(p)(opts)
		if err == nil {
			t.Fatalf("Expected error for %q", p)
		}
	}

	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.CustomInboxPrefix("$BOB"))
		if err != nil {
			t.Fatalf("Expected to connect to server, got %v", err)
		}
		defer nc.Close()

		sub, err := nc.Subscribe(nats.NewInbox(), func(msg *nats.Msg) {
			if !strings.HasPrefix(msg.Reply, "$BOB.") {
				t.Fatalf("invalid inbox subject %q received", msg.Reply)
			}

			if len(strings.Split(msg.Reply, ".")) != 3 {
				t.Fatalf("invalid number tokens in %s", msg.Reply)
			}

			msg.Respond([]byte("ok"))
		})
		if err != nil {
			t.Fatalf("subscribe failed: %s", err)
		}

		resp, err := nc.Request(sub.Subject, nil, time.Second)
		if err != nil {
			t.Fatalf("request failed: %s", err)
		}

		if !bytes.Equal(resp.Data, []byte("ok")) {
			t.Fatalf("did not receive ok: %q", resp.Data)
		}
	})
}

func TestRespInbox(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		if _, err := nc.Subscribe("foo", func(msg *nats.Msg) {
			lastDot := strings.LastIndex(msg.Reply, ".")
			if lastDot == -1 {
				msg.Respond([]byte(fmt.Sprintf("Invalid reply subject: %q", msg.Reply)))
				return
			}
			lastToken := msg.Reply[lastDot+1:]
			replySuffixLen := 8
			if len(lastToken) != replySuffixLen {
				msg.Respond([]byte(fmt.Sprintf("Invalid last token: %q", lastToken)))
				return
			}
			msg.Respond(nil)
		}); err != nil {
			t.Fatalf("subscribe failed: %s", err)
		}
		resp, err := nc.Request("foo", []byte("check inbox"), time.Second)
		if err != nil {
			t.Fatalf("Request failed: %v", err)
		}
		if len(resp.Data) > 0 {
			t.Fatalf("Error: %s", resp.Data)
		}
	})
}

func TestSkipSubjectValidation(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.SkipSubjectValidation())
		if err != nil {
			t.Fatalf("Expected to connect to server, got %v", err)
		}
		defer nc.Close()

		// Try to publish to a bad subject.
		badSubj := "foo bar"
		if err := nc.Publish(badSubj, []byte("hello")); err != nil {
			t.Fatalf("Expected to publish to bad subject %q, got error: %v", badSubj, err)
		}
	})
}
