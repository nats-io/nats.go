// Copyright 2023 The NATS Authors
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
	"net/url"
	"os"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

func TestMaxConnectionsReconnect(t *testing.T) {
	// Start first server
	s1Opts := natsserver.DefaultTestOptions
	s1Opts.Port = -1
	s1Opts.MaxConn = 2
	s1Opts.Cluster = server.ClusterOpts{Name: "test", Host: "127.0.0.1", Port: -1}
	s1 := RunServerWithOptions(&s1Opts)
	defer s1.Shutdown()

	// Start second server
	s2Opts := natsserver.DefaultTestOptions
	s2Opts.Port = -1
	s2Opts.MaxConn = 2
	s2Opts.Cluster = server.ClusterOpts{Name: "test", Host: "127.0.0.1", Port: -1}
	s2Opts.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s1Opts.Cluster.Port))
	s2 := RunServerWithOptions(&s2Opts)
	defer s2.Shutdown()

	errCh := make(chan error, 2)
	reconnectCh := make(chan struct{})
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

	// Create two connections (the current max) to first server
	nc1, _ := nats.Connect(s1.ClientURL(), opts...)
	defer nc1.Close()
	nc1.Flush()

	nc2, _ := nats.Connect(s1.ClientURL(), opts...)
	defer nc2.Close()
	nc2.Flush()

	if s1.NumClients() != 2 {
		t.Fatalf("Expected 2 client connections to first server. Got %d", s1.NumClients())
	}

	if s2.NumClients() > 0 {
		t.Fatalf("Expected 0 client connections to second server. Got %d", s2.NumClients())
	}

	// Kick one of our two server connections off first server. One client should reconnect to second server
	newS1Opts := s1Opts
	newS1Opts.MaxConn = 1
	err := s1.ReloadOptions(&newS1Opts)
	if err != nil {
		t.Fatalf("Unexpected error changing max_connections [%s]", err)
	}

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

	if s2.NumClients() <= 0 || s1.NumClients() > 1 {
		t.Fatalf("Expected client reconnection to second server")
	}
}

func TestNoEcho(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	url := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)

	nc, err := nats.Connect(url, nats.NoEcho())
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
}

// Trust Server Tests

var (
	oSeed = []byte("SOAL7GTNI66CTVVNXBNQMG6V2HTDRWC3HGEP7D2OUTWNWSNYZDXWFOX4SU")
	aSeed = []byte("SAAASUPRY3ONU4GJR7J5RUVYRUFZXG56F4WEXELLLORQ65AEPSMIFTOJGE")
	uSeed = []byte("SUAMK2FG4MI6UE3ACF3FK3OIQBCEIEZV7NSWFFEW63UXMRLFM2XLAXK4GY")

	aJWT = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJLWjZIUVRXRlY3WkRZSFo3NklRNUhPM0pINDVRNUdJS0JNMzJTSENQVUJNNk5PNkU3TUhRIiwiaWF0IjoxNTQ0MDcxODg5LCJpc3MiOiJPRDJXMkk0TVZSQTVUR1pMWjJBRzZaSEdWTDNPVEtGV1FKRklYNFROQkVSMjNFNlA0NlMzNDVZWSIsInN1YiI6IkFBUFFKUVVQS1ZYR1c1Q1pINUcySEZKVUxZU0tERUxBWlJWV0pBMjZWRFpPN1dTQlVOSVlSRk5RIiwidHlwZSI6ImFjY291bnQiLCJuYXRzIjp7ImxpbWl0cyI6eyJzdWJzIjotMSwiY29ubiI6LTEsImltcG9ydHMiOi0xLCJleHBvcnRzIjotMSwiZGF0YSI6LTEsInBheWxvYWQiOi0xLCJ3aWxkY2FyZHMiOnRydWV9fX0.8o35JPQgvhgFT84Bi2Z-zAeSiLrzzEZn34sgr1DIBEDTwa-EEiMhvTeos9cvXxoZVCCadqZxAWVwS6paAMj8Bg"

	uJWT = "eyJ0eXAiOiJqd3QiLCJhbGciOiJlZDI1NTE5In0.eyJqdGkiOiJBSFQzRzNXRElDS1FWQ1FUWFJUTldPRlVVUFRWNE00RFZQV0JGSFpJQUROWEZIWEpQR0FBIiwiaWF0IjoxNTQ0MDcxODg5LCJpc3MiOiJBQVBRSlFVUEtWWEdXNUNaSDVHMkhGSlVMWVNLREVMQVpSVldKQTI2VkRaTzdXU0JVTklZUkZOUSIsInN1YiI6IlVBVDZCV0NTQ1dMVUtKVDZLNk1CSkpPRU9UWFo1QUpET1lLTkVWUkZDN1ZOTzZPQTQzTjRUUk5PIiwidHlwZSI6InVzZXIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e319fQ._8A1XM88Q2kp7XVJZ42bQuO9E3QPsNAGKtVjAkDycj8A5PtRPby9UpqBUZzBwiJQQO3TUcD5GGqSvsMm6X8hCQ"

	chained = `
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
)

func runTrustServer() *server.Server {
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts := natsserver.DefaultTestOptions
	opts.Port = TEST_PORT
	opts.TrustedKeys = []string{string(pub)}
	s := RunServerWithOptions(&opts)
	mr := &server.MemAccResolver{}
	akp, _ := nkeys.FromSeed(aSeed)
	apub, _ := akp.PublicKey()
	mr.Store(string(apub), aJWT)
	s.SetAccountResolver(mr)
	return s
}

func createTmpFile(t *testing.T, content []byte) string {
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

func TestBasicUserJWTAuth(t *testing.T) {
	if server.VERSION[0] == '1' {
		t.Skip()
	}
	ts := runTrustServer()
	defer ts.Shutdown()

	url := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	_, err := nats.Connect(url)
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}

	jwtCB := func() (string, error) {
		return uJWT, nil
	}
	sigCB := func(nonce []byte) ([]byte, error) {
		kp, _ := nkeys.FromSeed(uSeed)
		sig, _ := kp.Sign(nonce)
		return sig, nil
	}

	// Try with user jwt but no sig
	_, err = nats.Connect(url, nats.UserJWT(jwtCB, nil))
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}

	// Try with user callback
	_, err = nats.Connect(url, nats.UserJWT(nil, sigCB))
	if err == nil {
		t.Fatalf("Expecting an error on connect")
	}

	nc, err := nats.Connect(url, nats.UserJWT(jwtCB, sigCB))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	nc.Close()
}

func TestUserCredentialsTwoFiles(t *testing.T) {
	if server.VERSION[0] == '1' {
		t.Skip()
	}
	ts := runTrustServer()
	defer ts.Shutdown()

	userJWTFile := createTmpFile(t, []byte(uJWT))
	defer os.Remove(userJWTFile)
	userSeedFile := createTmpFile(t, uSeed)
	defer os.Remove(userSeedFile)

	url := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	nc, err := nats.Connect(url, nats.UserCredentials(userJWTFile, userSeedFile))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	nc.Close()
}

func TestUserCredentialsChainedFile(t *testing.T) {
	if server.VERSION[0] == '1' {
		t.Skip()
	}
	ts := runTrustServer()
	defer ts.Shutdown()

	chainedFile := createTmpFile(t, []byte(chained))
	defer os.Remove(chainedFile)

	url := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	nc, err := nats.Connect(url, nats.UserCredentials(chainedFile))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	nc.Close()

	chainedFile = createTmpFile(t, []byte("invalid content"))
	defer os.Remove(chainedFile)
	nc, err = nats.Connect(url, nats.UserCredentials(chainedFile))
	if err == nil || !strings.Contains(err.Error(),
		"error signing nonce: unable to extract key pair from file") {
		if nc != nil {
			nc.Close()
		}
		t.Fatalf("Expected error about invalid creds file, got %q", err)
	}
}

func TestReconnectMissingCredentials(t *testing.T) {
	ts := runTrustServer()
	defer ts.Shutdown()

	chainedFile := createTmpFile(t, []byte(chained))
	defer os.Remove(chainedFile)

	url := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	errs := make(chan error, 1)
	nc, err := nats.Connect(url, nats.UserCredentials(chainedFile), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errs <- err
	}))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	defer nc.Close()
	os.Remove(chainedFile)
	ts.Shutdown()

	ts = runTrustServer()
	defer ts.Shutdown()

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
	if server.VERSION[0] == '1' {
		t.Skip()
	}
	ts := runTrustServer()
	defer ts.Shutdown()

	url := fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT)
	nc, err := nats.Connect(url, nats.UserJWTAndSeed(uJWT, string(uSeed)))
	if err != nil {
		t.Fatalf("Expected to connect, got %v", err)
	}
	nc.Close()
}

// If we are using TLS and have multiple servers we try to match the IP
// from a discovered server with the expected hostname for certs without IP
// designations. In certain cases where there is a not authorized error and
// we were trying the second server with the IP only and getting an error
// that was hard to understand for the end user. This did require
// Opts.Secure = false, but the fix removed the check on Opts.Secure to decide
// if we need to save off the hostname that we connected to first.
func TestUserCredentialsChainedFileNotFoundError(t *testing.T) {
	if server.VERSION[0] == '1' {
		t.Skip()
	}
	// Setup opts for both servers.
	kp, _ := nkeys.FromSeed(oSeed)
	pub, _ := kp.PublicKey()
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.Cluster.Port = -1
	opts.TrustedKeys = []string{string(pub)}
	tc := &server.TLSConfigOpts{
		CertFile: "./configs/certs/server_noip.pem",
		KeyFile:  "./configs/certs/key_noip.pem",
	}
	var err error
	if opts.TLSConfig, err = server.GenTLSConfig(tc); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}

	// copy the opts for the second server.
	opts2 := opts

	sa := RunServerWithOptions(&opts)
	defer sa.Shutdown()

	routeAddr := fmt.Sprintf("nats-route://%s:%d", opts.Cluster.Host, opts.Cluster.Port)
	rurl, _ := url.Parse(routeAddr)
	opts2.Routes = []*url.URL{rurl}

	sb := RunServerWithOptions(&opts2)
	defer sb.Shutdown()

	wait := time.Now().Add(2 * time.Second)
	for time.Now().Before(wait) {
		sanr := sa.NumRoutes()
		sbnr := sb.NumRoutes()
		if sanr == 1 && sbnr == 1 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	// Make sure we get the right error here.
	nc, err := nats.Connect(fmt.Sprintf("nats://localhost:%d", opts.Port),
		nats.RootCAs("./configs/certs/ca.pem"),
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

var natsReconnectOpts = nats.Options{
	Url:            fmt.Sprintf("nats://127.0.0.1:%d", TEST_PORT),
	AllowReconnect: true,
	MaxReconnect:   10,
	ReconnectWait:  100 * time.Millisecond,
	Timeout:        nats.DefaultTimeout,
}

func TestNkeyAuth(t *testing.T) {
	if server.VERSION[0] == '1' {
		t.Skip()
	}

	seed := []byte("SUAKYRHVIOREXV7EUZTBHUHL7NUMHPMAS7QMDU3GTIUWEI5LDNOXD43IZY")
	kp, _ := nkeys.FromSeed(seed)
	pub, _ := kp.PublicKey()

	sopts := natsserver.DefaultTestOptions
	sopts.Port = TEST_PORT
	sopts.Nkeys = []*server.NkeyUser{{Nkey: string(pub)}}
	ts := RunServerWithOptions(&sopts)
	defer ts.Shutdown()

	opts := natsReconnectOpts
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
	ts.Shutdown()
	ts = RunServerWithOptions(&sopts)
	defer ts.Shutdown()

	if err := nc.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}
}

func TestLookupHostResultIsRandomized(t *testing.T) {
	orgAddrs, err := net.LookupHost("localhost")
	if err != nil {
		t.Fatalf("Error looking up host: %v", err)
	}

	// We actually want the IPv4 and IPv6 addresses, so lets make sure.
	if !reflect.DeepEqual(orgAddrs, []string{"::1", "127.0.0.1"}) {
		t.Skip("Was looking for IPv4 and IPv6 addresses for localhost to perform test")
	}

	opts := natsserver.DefaultTestOptions
	opts.Host = "127.0.0.1"
	opts.Port = TEST_PORT
	s1 := RunServerWithOptions(&opts)
	defer s1.Shutdown()

	opts.Host = "::1"
	s2 := RunServerWithOptions(&opts)
	defer s2.Shutdown()

	for i := 0; i < 100; i++ {
		nc, err := nats.Connect(fmt.Sprintf("localhost:%d", TEST_PORT))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
	}

	if ncls := s1.NumClients(); ncls < 35 || ncls > 65 {
		t.Fatalf("Does not seem balanced between multiple servers: s1:%d, s2:%d", s1.NumClients(), s2.NumClients())
	}
}

func TestLookupHostResultIsNotRandomizedWithNoRandom(t *testing.T) {
	orgAddrs, err := net.LookupHost("localhost")
	if err != nil {
		t.Fatalf("Error looking up host: %v", err)
	}

	// We actually want the IPv4 and IPv6 addresses, so lets make sure.
	if !reflect.DeepEqual(orgAddrs, []string{"::1", "127.0.0.1"}) {
		t.Skip("Was looking for IPv4 and IPv6 addresses for localhost to perform test")
	}

	opts := natsserver.DefaultTestOptions
	opts.Host = orgAddrs[0]
	opts.Port = TEST_PORT
	s1 := RunServerWithOptions(&opts)
	defer s1.Shutdown()

	opts.Host = orgAddrs[1]
	s2 := RunServerWithOptions(&opts)
	defer s2.Shutdown()

	for i := 0; i < 100; i++ {
		nc, err := nats.Connect(fmt.Sprintf("localhost:%d", TEST_PORT), nats.DontRandomize())
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()
	}

	if ncls := s1.NumClients(); ncls != 100 {
		t.Fatalf("Expected all clients on first server, only got %d of 100", ncls)
	}
}

func TestConnectedAddr(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	var nc *nats.Conn
	if addr := nc.ConnectedAddr(); addr != "" {
		t.Fatalf("Expected empty result for nil connection, got %q", addr)
	}
	nc, err := nats.Connect(fmt.Sprintf("localhost:%d", TEST_PORT))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	expected := s.Addr().String()
	if addr := nc.ConnectedAddr(); addr != expected {
		t.Fatalf("Expected address %q, got %q", expected, addr)
	}
	nc.Close()
	if addr := nc.ConnectedAddr(); addr != "" {
		t.Fatalf("Expected empty result for closed connection, got %q", addr)
	}
}

func TestSubscribeSyncRace(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("127.0.0.1:%d", TEST_PORT))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	go func() {
		time.Sleep(time.Millisecond)
		nc.Close()
	}()

	subj := "foo.sync.race"
	for i := 0; i < 10000; i++ {
		if _, err := nc.SubscribeSync(subj); err != nil {
			break
		}
		if _, err := nc.QueueSubscribeSync(subj, "gc"); err != nil {
			break
		}
	}
}

func TestBadSubjectsAndQueueNames(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("127.0.0.1:%d", TEST_PORT))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer nc.Close()

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
}

func BenchmarkNextMsgNoTimeout(b *testing.B) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	ncp, err := nats.Connect(fmt.Sprintf("127.0.0.1:%d", TEST_PORT))
	if err != nil {
		b.Fatalf("Error connecting: %v", err)
	}
	ncs, err := nats.Connect(fmt.Sprintf("127.0.0.1:%d", TEST_PORT), nats.SyncQueueLen(b.N))
	if err != nil {
		b.Fatalf("Error connecting: %v", err)
	}

	// Test processing speed so no long subject or payloads.
	subj := "a"

	sub, err := ncs.SubscribeSync(subj)
	if err != nil {
		b.Fatalf("Error subscribing: %v", err)
	}
	ncs.Flush()

	// Set it up so we can internally queue all the messages.
	sub.SetPendingLimits(b.N, b.N*1000)

	for i := 0; i < b.N; i++ {
		ncp.Publish(subj, nil)
	}
	ncp.Flush()

	// Wait for them to all be queued up, testing NextMsg not server here.
	// Only wait at most one second.
	wait := time.Now().Add(time.Second)
	for time.Now().Before(wait) {
		nm, _, err := sub.Pending()
		if err != nil {
			b.Fatalf("Error on Pending() - %v", err)
		}
		if nm >= b.N {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := sub.NextMsg(10 * time.Millisecond); err != nil {
			b.Fatalf("Error getting message[%d]: %v", i, err)
		}
	}
}

func TestAuthErrorOnReconnect(t *testing.T) {
	// This is a bit of an artificial test, but it is to demonstrate
	// that if the client is disconnected from a server (not due to an auth error),
	// it will still correctly stop the reconnection logic if it gets twice an
	// auth error from the same server.

	o1 := natsserver.DefaultTestOptions
	o1.Port = -1
	s1 := RunServerWithOptions(&o1)
	defer s1.Shutdown()

	o2 := natsserver.DefaultTestOptions
	o2.Port = -1
	o2.Username = "ivan"
	o2.Password = "pwd"
	s2 := RunServerWithOptions(&o2)
	defer s2.Shutdown()

	dch := make(chan bool)
	cch := make(chan bool)

	urls := fmt.Sprintf("nats://%s:%d, nats://%s:%d", o1.Host, o1.Port, o2.Host, o2.Port)
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

	s1.Shutdown()

	// wait for disconnect
	if e := WaitTime(dch, 5*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	// Wait for ClosedCB
	if e := WaitTime(cch, 5*time.Second); e != nil {
		reconnects := nc.Stats().Reconnects
		t.Fatalf("Did not receive a closed callback message, #reconnects: %v", reconnects)
	}

	// We should have stopped after 2 reconnects.
	if reconnects := nc.Stats().Reconnects; reconnects != 2 {
		t.Fatalf("Expected 2 reconnects, got %v", reconnects)
	}

	// Expect connection to be closed...
	if !nc.IsClosed() {
		t.Fatalf("Wrong status: %d\n", nc.Status())
	}
}

func TestStatsRace(t *testing.T) {
	o := natsserver.DefaultTestOptions
	o.Port = -1
	s := RunServerWithOptions(&o)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", o.Host, o.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

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
	for i := 0; i < 1000; i++ {
		nc.Publish("foo", []byte("hello"))
	}

	close(ch)
	wg.Wait()
}

func TestRequestLeaksMapEntries(t *testing.T) {
	o := natsserver.DefaultTestOptions
	o.Port = -1
	s := RunServerWithOptions(&o)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", o.Host, o.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	response := []byte("I will help you")
	nc.Subscribe("foo", func(m *nats.Msg) {
		nc.Publish(m.Reply, response)
	})

	for i := 0; i < 100; i++ {
		msg, err := nc.Request("foo", nil, 500*time.Millisecond)
		if err != nil {
			t.Fatalf("Received an error on Request test: %s", err)
		}
		if !bytes.Equal(msg.Data, response) {
			t.Fatalf("Received invalid response")
		}
	}
}

func TestRequestMultipleReplies(t *testing.T) {
	o := natsserver.DefaultTestOptions
	o.Port = -1
	s := RunServerWithOptions(&o)
	defer s.Shutdown()

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", o.Host, o.Port))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	response := []byte("I will help you")
	nc.Subscribe("foo", func(m *nats.Msg) {
		m.Respond(response)
		m.Respond(response)
	})
	nc.Flush()

	nc2, err := nats.Connect(fmt.Sprintf("nats://%s:%d", o.Host, o.Port))
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
}

func TestGetRTT(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.ReconnectWait(10*time.Millisecond), nats.ReconnectJitter(0, 0))
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
	s.Shutdown()
	time.Sleep(5 * time.Millisecond)
	if _, err = nc.RTT(); err != nats.ErrDisconnected {
		t.Fatalf("Expected disconnected error getting RTT when disconnected, got %v", err)
	}
}

func TestGetClientIP(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	ip, err := nc.GetClientIP()
	if err != nil {
		t.Fatalf("Got error looking up IP: %v", err)
	}
	if !ip.IsLoopback() {
		t.Fatalf("Expected a loopback IP, got %v", ip)
	}
	nc.Close()
	if _, err := nc.GetClientIP(); err != nats.ErrConnectionClosed {
		t.Fatalf("Expected a connection closed error, got %v", err)
	}
}

func TestReconnectWaitJitter(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	rch := make(chan time.Time, 1)
	nc, err := nats.Connect(s.ClientURL(),
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

	s.Shutdown()
	start := time.Now()
	// Wait a bit so that the library tries a first time without waiting.
	time.Sleep(50 * time.Millisecond)
	s = RunServerOnPort(TEST_PORT)
	defer s.Shutdown()
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
	nc, err = nats.Connect(s.ClientURL(), nats.ReconnectWait(10*time.Minute))
	if err != nil {
		t.Fatalf("Error during connect: %v", err)
	}
	defer nc.Close()

	// Cause a disconnect
	s.Shutdown()
	// Wait a bit for the reconnect loop to go into wait mode.
	time.Sleep(50 * time.Millisecond)
	s = RunServerOnPort(TEST_PORT)
	defer s.Shutdown()
	// Now close and expect the reconnect go routine to return..
	nc.Close()
	// Wait a bit to give a chance for the go routine to exit.
	time.Sleep(50 * time.Millisecond)
	buf := make([]byte, 100000)
	n := runtime.Stack(buf, true)
	if strings.Contains(string(buf[:n]), "doReconnect") {
		t.Fatalf("doReconnect go routine still running:\n%s", buf[:n])
	}
}

func TestCustomReconnectDelay(t *testing.T) {
	s := RunServerOnPort(TEST_PORT)
	defer s.Shutdown()

	expectedAttempt := 1
	errCh := make(chan error, 1)
	cCh := make(chan bool, 1)
	nc, err := nats.Connect(s.ClientURL(),
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
	s.Shutdown()

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
}

func TestMsg_RespondMsg(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

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
}

func TestCustomInboxPrefix(t *testing.T) {
	opts := &nats.Options{}
	for _, p := range []string{"$BOB.", "$BOB.*", "$BOB.>", ">", ".", "", "BOB.*.X", "BOB.>.X"} {
		err := nats.CustomInboxPrefix(p)(opts)
		if err == nil {
			t.Fatalf("Expected error for %q", p)
		}
	}

	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL(), nats.CustomInboxPrefix("$BOB"))
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
}

func TestRespInbox(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

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
}

func TestInProcessConn(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect("", nats.InProcessServer(s))
	if err != nil {
		t.Fatal(err)
	}

	defer nc.Close()

	// Status should be connected.
	if nc.Status() != nats.CONNECTED {
		t.Fatal("should be status CONNECTED")
	}

	// The server should respond to a request.
	if _, err := nc.RTT(); err != nil {
		t.Fatal(err)
	}
}
