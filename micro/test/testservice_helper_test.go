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

package micro_test

import (
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"

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

// withServer creates a non-JetStream server and dials it. Cleanup via t.Cleanup.
func withServer(t *testing.T, fn func(*testing.T, *nats.Conn), opts ...testservice.CreateOption) {
	t.Helper()
	withServerInstance(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		fn(t, nc)
	}, opts...)
}

// withServerInstance is withServer plus the *testservice.Instance.
func withServerInstance(t *testing.T, fn func(*testing.T, *nats.Conn, *testservice.Instance), opts ...testservice.CreateOption) {
	t.Helper()
	c := newTester(t)
	inst := c.CreateServer(t, false, opts...)
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	fn(t, nc, inst)
}

// dialInstance returns a connection that lists every server URL in inst, so
// reconnect survives any single node going down. nats.MaxReconnects(-1) is
// always set; additional connect options (e.g. credentials) may be passed.
// Accepts testing.TB so benchmarks can share the helper.
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
