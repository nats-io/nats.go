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
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
	"github.com/nats-io/nats.go/jetstream"
)

// newTester returns a Client connected to the tester service at the URL given
// by TESTER_NATS_URL. Tests skip when the env var is unset so a `go test
// -tags=testservice ./...` run does not fail on developer machines without
// docker.
func newTester(t *testing.T) *testservice.Client {
	t.Helper()
	url := os.Getenv("TESTER_NATS_URL")
	if url == "" {
		t.Skip("TESTER_NATS_URL not set; skipping testservice PoC")
	}
	return testservice.New(t, url)
}

// withTesterJSServer creates a single JetStream server via the tester, opens
// a connection and a jetstream.JetStream handle, runs the callback, and
// destroys the instance on return.
func withTesterJSServer(t *testing.T, fn func(t *testing.T, nc *nats.Conn, js jetstream.JetStream)) {
	t.Helper()
	c := newTester(t)
	c.WithJetStreamServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("jetstream.New: %v", err)
		}
		fn(t, nc, js)
	})
}

// withTesterJSCluster creates a JetStream cluster of `size` servers, opens a
// connection to a random server plus a jetstream.JetStream handle, runs the
// callback (which receives the Instance handle so it can stop/start servers),
// and destroys on return.
func withTesterJSCluster(t *testing.T, size int, fn func(t *testing.T, nc *nats.Conn, js jetstream.JetStream, inst *testservice.Instance)) {
	t.Helper()
	c := newTester(t)
	c.WithJetStreamCluster(t, size, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("jetstream.New: %v", err)
		}
		fn(t, nc, js, inst)
	})
}

// withTesterCtx returns a 30s context. Most JS calls in tests take a context;
// this keeps PoC tests terse.
func withTesterCtx(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), 30*time.Second)
}
