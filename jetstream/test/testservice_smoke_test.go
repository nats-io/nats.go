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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
	"github.com/nats-io/nats.go/jetstream"
)

// TestTestserviceSmokeJSServer exercises withJSServer end-to-end.
func TestTestserviceSmokeJSServer(t *testing.T) {
	withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 10*time.Second)

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     "SMOKE",
			Subjects: []string{"smoke.*"},
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}

		for i := range 3 {
			if _, err := js.Publish(ctx, "smoke.x", []byte{byte('a' + i)}); err != nil {
				t.Fatalf("Publish %d: %v", i, err)
			}
		}

		info, err := s.Info(ctx)
		if err != nil {
			t.Fatalf("Stream Info: %v", err)
		}
		if info.State.Msgs != 3 {
			t.Fatalf("expected 3 msgs, got %d", info.State.Msgs)
		}
	})
}

// TestTestserviceSmokeJSCluster exercises withJSCluster end-to-end with
// replicas across all nodes.
func TestTestserviceSmokeJSCluster(t *testing.T) {
	withJSCluster(t, 3, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream, inst *testservice.Instance) {
		ctx := newTesterCtx(t, 15*time.Second)

		if got := len(inst.Servers); got != 3 {
			t.Fatalf("expected 3 servers in cluster, got %d", got)
		}

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     "SMOKE_CL",
			Subjects: []string{"smokecl.*"},
			Replicas: 3,
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}
		if _, err := js.Publish(ctx, "smokecl.x", []byte("ping")); err != nil {
			t.Fatalf("Publish: %v", err)
		}

		info, err := s.Info(ctx)
		if err != nil {
			t.Fatalf("Stream Info: %v", err)
		}
		if info.State.Msgs != 1 {
			t.Fatalf("expected 1 msg, got %d", info.State.Msgs)
		}
	})
}

// TestTestserviceSmokeJSSuperCluster is intentionally skipped: super-cluster
// JetStream meta-leader does not elect against synadia/server-tester:2.14.0
// (validated deterministically across 4 runs: CreateStream returns 10008
// "JetStream system temporarily unavailable" indefinitely). Tracking upstream
// via synadia-labs/testing.go; re-enable when the gateway-formation issue is
// resolved. The withJSSuperCluster helper is kept in the helper file because
// it costs nothing and will be exercised once super-cluster works.
func TestTestserviceSmokeJSSuperCluster(t *testing.T) {
	t.Skip("super-cluster meta-layer not ready in synadia/server-tester:2.14.0; see migration design D5 finding")
}
