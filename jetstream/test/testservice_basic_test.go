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

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestTesterJSBasic(t *testing.T) {
	withTesterJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
		ctx := withTesterCtx(t)

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{
			Name:     "POC1",
			Subjects: []string{"poc1.*"},
		})
		if err != nil {
			t.Fatalf("CreateStream: %v", err)
		}

		for i := range 5 {
			if _, err := js.Publish(ctx, "poc1.x", []byte{byte('a' + i)}); err != nil {
				t.Fatalf("Publish %d: %v", i, err)
			}
		}

		info, err := s.Info(ctx)
		if err != nil {
			t.Fatalf("Stream Info: %v", err)
		}
		if info.State.Msgs != 5 {
			t.Fatalf("expected 5 msgs, got %d", info.State.Msgs)
		}

		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:   "poc1c",
			AckPolicy: jetstream.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("CreateOrUpdateConsumer: %v", err)
		}

		msgs, err := c.Fetch(5)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		got := 0
		for msg := range msgs.Messages() {
			if err := msg.Ack(); err != nil {
				t.Fatalf("Ack: %v", err)
			}
			got++
		}
		if msgs.Error() != nil {
			t.Fatalf("Fetch error: %v", msgs.Error())
		}
		if got != 5 {
			t.Fatalf("expected 5 fetched msgs, got %d", got)
		}
	})
}
