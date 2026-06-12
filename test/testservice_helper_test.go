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
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

// newTester returns a tester Client connected to the service at TESTER_NATS_URL.
// Tests skip when the env var is unset so a -tags=testservice run does not
// fail on machines without docker. Close is registered with t.Cleanup. Accepts
// any testing.TB so benchmarks (which use *testing.B) can share the helper.
func newTester(t testing.TB) *testservice.Client {
	t.Helper()
	url := os.Getenv("TESTER_NATS_URL")
	if url == "" {
		t.Skip("TESTER_NATS_URL not set; skipping testservice test")
	}
	c := testservice.New(t, url)
	t.Cleanup(func() { c.Close(t) })
	return c
}

// testerHost returns the hostname clients use to reach the tester (and the
// servers it spawns), parsed from TESTER_NATS_URL. This is "localhost" for the
// host-side dev workflow and the tester's docker service name (e.g. "nats")
// in CI. Skips the test if TESTER_NATS_URL is unset.
func testerHost(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("TESTER_NATS_URL")
	if raw == "" {
		t.Skip("TESTER_NATS_URL not set; skipping testservice test")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("could not parse TESTER_NATS_URL %q: %v", raw, err)
	}
	return u.Hostname()
}

// clientAdvertiseOpt returns a CreateOption that makes every server in the
// instance advertise "<testerHost>:<its client port>" to clients via INFO
// connect_urls, instead of the server's own bind address (which inside docker
// is a container-internal IP unreachable from the test process). The host is
// resolved at test time from TESTER_NATS_URL; the per-server port is filled by
// the tester's template engine via .ClientPort. This makes gossiped pool URLs
// match the inst.Servers[i].URL values the client dialed, so pool-membership
// assertions hold in both the host-side and CI (sibling-container) topologies.
func clientAdvertiseOpt(t *testing.T) testservice.CreateOption {
	t.Helper()
	host := testerHost(t)
	return testservice.WithTopLevel(fmt.Sprintf("client_advertise: \"%s:{{ .ClientPort }}\"", host))
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
