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
