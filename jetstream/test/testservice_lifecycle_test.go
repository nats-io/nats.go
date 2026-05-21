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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTesterServerLifecycle(t *testing.T) {
	withTesterJSCluster(t, 3, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream, inst *testservice.Instance) {
		ctx := withTesterCtx(t)

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     "POC3",
			Subjects: []string{"poc3.*"},
			Replicas: 3,
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}

		victim := inst.Servers[0]
		inst.StopServer(t, victim)

		// Give the cluster a moment to elect / settle without the victim.
		time.Sleep(2 * time.Second)

		if _, err := js.Publish(ctx, "poc3.x", []byte("after-stop")); err != nil {
			t.Fatalf("Publish after stop: %v", err)
		}

		inst.StartServer(t, victim)

		// Allow the restarted node to catch up.
		time.Sleep(3 * time.Second)

		info, err := s.Info(ctx)
		if err != nil {
			t.Fatalf("Stream Info after restart: %v", err)
		}
		if info.State.Msgs != 1 {
			t.Fatalf("expected 1 msg after restart, got %d", info.State.Msgs)
		}
	})
}
