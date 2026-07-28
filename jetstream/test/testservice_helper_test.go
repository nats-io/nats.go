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
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
)

// testserviceHost returns the hostname clients use to reach the tester (and
// the servers it spawns), parsed from TESTER_NATS_URL. Used by cross-domain
// leafnode tests that need to embed the hub host:port in the leaf's remotes.
func testserviceHost(t *testing.T) string {
	t.Helper()
	raw := os.Getenv("TESTER_NATS_URL")
	if raw == "" {
		t.Skip("TESTER_NATS_URL not set; skipping testservice test (see 'make tester-up-host')")
	}
	u, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("could not parse TESTER_NATS_URL %q: %v", raw, err)
	}
	return u.Hostname()
}

// emptyAccountsBody clears the built-in USERS1..USERS5 / $SYS accounts block.
func emptyAccountsBody() string {
	return `# accounts block intentionally empty`
}

// noSystemAccountBody clears the built-in `system_account: "$SYS"` line.
func noSystemAccountBody() string {
	return `# system_account intentionally unset`
}

// testerProbe caches a one-time reachability check of the tester service so
// that when it is down, every test fails fast with one actionable message
// instead of its own raw dial error.
var testerProbe struct {
	once sync.Once
	err  error
}

// newTester returns a tester Client connected to the service at TESTER_NATS_URL.
// Tests skip when the env var is unset so a plain `go test` run passes on
// machines without docker. Close is registered with t.Cleanup. Accepts any
// testing.TB so benchmarks (which use *testing.B) can share the helper.
func newTester(t testing.TB) *testservice.Client {
	t.Helper()
	url := os.Getenv("TESTER_NATS_URL")
	if url == "" {
		t.Skip("TESTER_NATS_URL not set; skipping testservice test (see 'make tester-up-host')")
	}
	testerProbe.once.Do(func() {
		nc, err := nats.Connect(url)
		if err != nil {
			testerProbe.err = fmt.Errorf("cannot reach the tester at %s (is it running? see 'make tester-up-host'): %w", url, err)
			return
		}
		nc.Close()
	})
	if testerProbe.err != nil {
		t.Fatal(testerProbe.err)
	}
	c := testservice.New(t, url)
	t.Cleanup(func() { c.Close(t) })
	return c
}

// newTesterCtx returns a context with the given timeout; cancel is registered
// with t.Cleanup.
func newTesterCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
}

// withJSServer creates a single JetStream server, dials it, and invokes fn with
// a connection and a jetstream.JetStream handle. Cleanup is via t.Cleanup.
func withJSServer(t *testing.T, fn func(*testing.T, *nats.Conn, jetstream.JetStream), opts ...testservice.CreateOption) {
	t.Helper()
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream, _ *testservice.Instance) {
		fn(t, nc, js)
	}, opts...)
}

// withJSServerInstance is withJSServer plus the *testservice.Instance handle,
// so tests can stop/start the server or inspect Ports.
func withJSServerInstance(t *testing.T, fn func(*testing.T, *nats.Conn, jetstream.JetStream, *testservice.Instance), opts ...testservice.CreateOption) {
	t.Helper()
	c := newTester(t)
	inst := c.CreateServer(t, true, opts...)
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	js := newJetStream(t, nc)
	fn(t, nc, js, inst)
}

// withJSCluster creates a JetStream cluster of `size` servers, dials all member
// URLs (so reconnect survives any single node going down), and invokes fn with
// connection, JetStream handle, and *testservice.Instance — cluster tests
// effectively always want the instance, so we pass it unconditionally.
func withJSCluster(t *testing.T, size int, fn func(*testing.T, *nats.Conn, jetstream.JetStream, *testservice.Instance), opts ...testservice.CreateOption) {
	t.Helper()
	c := newTester(t)
	inst := c.CreateCluster(t, size, true, opts...)
	t.Cleanup(func() { inst.Destroy(t) })

	if got := len(inst.Servers); got != size {
		t.Fatalf("expected %d servers in cluster, got %d", size, got)
	}

	nc := dialInstance(t, inst)
	c.WaitForJetStream(t, nc)
	js := newJetStream(t, nc)
	fn(t, nc, js, inst)
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

func newJetStream(t *testing.T, nc *nats.Conn) jetstream.JetStream {
	t.Helper()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return js
}
