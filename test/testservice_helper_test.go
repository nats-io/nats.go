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

//go:build testservice

package test

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

// testConfigsMount is the absolute path inside the tester container at which
// the host's test/configs directory is mounted (see Makefile and
// .github/workflows/ci.yaml). Tests use containerPath to address files within
// it — e.g. containerPath("certs/server.pem") returns
// "/test-configs/certs/server.pem", suitable for embedding into a server config
// snippet via testservice.WithTLS / WithTopLevel.
const testConfigsMount = "/test-configs"

// containerPath returns the absolute path inside the tester container for a
// file relative to test/configs/.
func containerPath(rel string) string {
	return testConfigsMount + "/" + rel
}

// newTester returns a tester Client connected to the service at TESTER_NATS_URL.
// Tests skip when the env var is unset so a -tags=testservice run does not
// fail on machines without docker. Close is registered with t.Cleanup.
func newTester(t *testing.T) *testservice.Client {
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

// newTesterCtx returns a context with the given timeout; cancel is registered
// with t.Cleanup.
func newTesterCtx(t *testing.T, d time.Duration) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), d)
	t.Cleanup(cancel)
	return ctx
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

// dialInstance returns a connection that lists every server URL in inst, so
// reconnect survives any single node going down. nats.MaxReconnects(-1) is
// always set; additional connect options (e.g. credentials) may be passed.
// Tests that need custom dial behavior call CreateServer/CreateCluster
// themselves and then dialInstance directly.
func dialInstance(t *testing.T, inst *testservice.Instance, opts ...nats.Option) *nats.Conn {
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
