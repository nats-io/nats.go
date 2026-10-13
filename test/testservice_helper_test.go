// Copyright 2026 The NATS Authors
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
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
)

// testerProbe caches a one-time reachability check of the tester service so
// that when it is down, every test fails fast with one actionable message
// instead of its own raw dial error.
var testerProbe struct {
	once sync.Once
	err  error
}

// testerURL is set by TestMain: TESTER_NATS_URL when set, otherwise the
// in-process tester it started.
var testerURL string

// newTester returns a tester Client connected to the tester service. Close is
// registered with t.Cleanup.
func newTester(t testing.TB) *testservice.Client {
	t.Helper()
	testerProbe.once.Do(func() {
		nc, err := nats.Connect(testerURL)
		if err != nil {
			testerProbe.err = fmt.Errorf("cannot reach the tester at %s (unset TESTER_NATS_URL to use the in-process one, or start it with 'make tester-up-host'): %w", testerURL, err)
			return
		}
		fmt.Fprintf(os.Stderr, "tester at %s: nats-server %s\n", testerURL, nc.ConnectedServerVersion())
		nc.Close()
	})
	if testerProbe.err != nil {
		t.Fatal(testerProbe.err)
	}
	c := testservice.New(t, testerURL)
	t.Cleanup(func() { c.Close(t) })
	return c
}

// testerHost returns the hostname clients use to reach the tester (and the
// servers it spawns). This is "localhost" for the host-side dev workflow and
// the in-process tester, and the tester's docker service name (e.g. "nats")
// in CI.
func testerHost(t *testing.T) string {
	t.Helper()
	u, err := url.Parse(testerURL)
	if err != nil {
		t.Fatalf("could not parse tester URL %q: %v", testerURL, err)
	}
	return u.Hostname()
}

// withServer creates a non-JetStream server and dials it. Cleanup via t.Cleanup.
func withServer(t *testing.T, fn func(*testing.T, *nats.Conn), opts ...testservice.CreateOption) {
	t.Helper()
	withServerInstance(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		fn(t, nc)
	}, opts...)
}

// withServerInstance is withServer plus the *testservice.Instance so tests can
// stop/start the server, inspect ports, or open additional connections.
func withServerInstance(t *testing.T, fn func(*testing.T, *nats.Conn, *testservice.Instance), opts ...testservice.CreateOption) {
	t.Helper()
	c := newTester(t)
	inst := c.CreateServer(t, false, opts...)
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	fn(t, nc, inst)
}

// withJSServer creates a JetStream-enabled server, dials it, and waits for
// JetStream to be ready. The callback receives the connection; tests in the
// `test` package use the legacy nc.JetStream() API to obtain a context.
// Cleanup via t.Cleanup.
func withJSServer(t *testing.T, fn func(*testing.T, *nats.Conn), opts ...testservice.CreateOption) {
	t.Helper()
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		fn(t, nc)
	}, opts...)
}

// withJSServerInstance is withJSServer plus the *testservice.Instance.
func withJSServerInstance(t *testing.T, fn func(*testing.T, *nats.Conn, *testservice.Instance), opts ...testservice.CreateOption) {
	t.Helper()
	c := newTester(t)
	inst := c.CreateServer(t, true, opts...)
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	c.WaitForJetStream(t, nc)
	fn(t, nc, inst)
}

// waitForJSCluster blocks until the meta leader is elected. WaitForJetStream
// only checks the transport error, so a leaderless cluster answering
// JSClusterNotAvail satisfies it; AccountInfo parses the response body.
func waitForJSCluster(t *testing.T, nc *nats.Conn) {
	t.Helper()
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("nc.JetStream: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err := js.AccountInfo()
		if err == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("jetstream cluster not ready: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// dialInstance returns a connection that lists every server URL in inst, so
// reconnect survives any single node going down. nats.MaxReconnects(-1) is
// always set; additional connect options (e.g. credentials) may be passed.
// Tests that need custom dial behavior call CreateServer/CreateCluster
// themselves and then dialInstance directly. Accepts testing.TB so benchmarks
// can share the helper.
func dialInstance(t testing.TB, inst *testservice.Instance, opts ...nats.Option) *nats.Conn {
	t.Helper()
	urls := make([]string, len(inst.Servers))
	for i, s := range inst.Servers {
		urls[i] = s.URL
	}
	connectOpts := append([]nats.Option{nats.MaxReconnects(-1)}, opts...)
	nc, err := nats.Connect(strings.Join(urls, ","), connectOpts...)
	if err != nil {
		t.Fatalf("nats.Connect: %v", err)
	}
	t.Cleanup(nc.Close)
	return nc
}

// managedTLSOpts wraps WithGeneratedTLS so the generated server cert covers
// the host clients actually dial — testerHost(t) — in addition to the upstream
// defaults (localhost, 127.0.0.1, ::1). Locally testerHost is "localhost"
// (already covered); in sibling-container / CI it's the docker service name
// ("nats"), which the default cert does not include and would fail x509
// verification on. Pass additional TLSOpts (TLSServerOnly, TLSHandshakeFirst,
// etc.) through varargs.
func managedTLSOpts(t *testing.T, opts ...testservice.TLSOpt) testservice.CreateOption {
	t.Helper()
	sans := []string{"localhost", "127.0.0.1", "::1"}
	if h := testerHost(t); h != "" && h != "localhost" {
		sans = append(sans, h)
	}
	return testservice.WithGeneratedTLS(append([]testservice.TLSOpt{testservice.TLSSANs(sans...)}, opts...)...)
}

// tlsCertFiles materializes the managed TLS material on inst to files under
// t.TempDir() and returns their paths. caPath is always set; clientCertPath
// and clientKeyPath are "" when the instance is not mutual TLS. Use this when
// the test exercises file-based nats client options (RootCAs, ClientCert,
// UserCredentials chained-files, etc.) — pure in-memory uses can call
// testservice.TLSConfig(inst) directly instead.
func tlsCertFiles(t *testing.T, inst *testservice.Instance) (caPath, clientCertPath, clientKeyPath string) {
	t.Helper()
	if inst == nil || inst.TLS == nil {
		t.Fatal("instance has no managed TLS material; was WithGeneratedTLS set?")
	}
	dir := t.TempDir()
	caPath = filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caPath, []byte(inst.TLS.CAPEM), 0o600); err != nil {
		t.Fatalf("could not write CA pem: %v", err)
	}
	if inst.TLS.ClientCertPEM != "" {
		clientCertPath = filepath.Join(dir, "client-cert.pem")
		clientKeyPath = filepath.Join(dir, "client-key.pem")
		if err := os.WriteFile(clientCertPath, []byte(inst.TLS.ClientCertPEM), 0o600); err != nil {
			t.Fatalf("could not write client cert: %v", err)
		}
		if err := os.WriteFile(clientKeyPath, []byte(inst.TLS.ClientKeyPEM), 0o600); err != nil {
			t.Fatalf("could not write client key: %v", err)
		}
	}
	return
}

// withServerB is the benchmark-flavored variant of withServer. Benchmarks use
// *testing.B so the with*-style wrappers (whose callbacks take *testing.T)
// don't fit; this helper inlines the same shape against testing.TB.
func withServerB(b *testing.B, fn func(*testing.B, *nats.Conn), opts ...testservice.CreateOption) {
	b.Helper()
	c := newTester(b)
	inst := c.CreateServer(b, false, opts...)
	b.Cleanup(func() { inst.Destroy(b) })
	nc := dialInstance(b, inst)
	fn(b, nc)
}
