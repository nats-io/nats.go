// Copyright 2023 The NATS Authors
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
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestConsumerInfo(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	t.Run("get consumer info, ok", func(t *testing.T) {
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:     "cons",
			AckPolicy:   jetstream.AckExplicitPolicy,
			Description: "test consumer",
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		info, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if info.Stream != "foo" {
			t.Fatalf("Invalid stream name; expected: 'foo'; got: %s", info.Stream)
		}
		if info.Config.Description != "test consumer" {
			t.Fatalf("Invalid consumer description; expected: 'test consumer'; got: %s", info.Config.Description)
		}

		// update consumer and see if info is updated
		_, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
			Durable:     "cons",
			AckPolicy:   jetstream.AckExplicitPolicy,
			Description: "updated consumer",
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		info, err = c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if info.Stream != "foo" {
			t.Fatalf("Invalid stream name; expected: 'foo'; got: %s", info.Stream)
		}
		if info.Config.Description != "updated consumer" {
			t.Fatalf("Invalid consumer description; expected: 'updated consumer'; got: %s", info.Config.Description)
		}
	})

	t.Run("consumer does not exist", func(t *testing.T) {
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.Stream(ctx, "foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.Consumer(ctx, "cons")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if err := s.DeleteConsumer(ctx, "cons"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = c.Info(ctx)
		if err == nil || !errors.Is(err, jetstream.ErrConsumerNotFound) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrConsumerNotFound, err)
		}
	})
}

func TestConsumerPinned(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)

	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:        "cons",
		AckPolicy:      jetstream.AckExplicitPolicy,
		Description:    "test consumer",
		PriorityPolicy: jetstream.PriorityPolicyPinned,
		PinnedTTL:      2 * time.Second,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that consumer got proper priority policy and TTL
	info := c.CachedInfo()
	if info.Config.PriorityPolicy != jetstream.PriorityPolicyPinned {
		t.Fatalf("Invalid priority policy; expected: %v; got: %v", jetstream.PriorityPolicyPinned, info.Config.PriorityPolicy)
	}
	if info.Config.PinnedTTL != 2*time.Second {
		t.Fatalf("Invalid pinned TTL; expected: %v; got: %v", 2*time.Second, info.Config.PinnedTTL)
	}

	for i := 0; i < 100; i++ {
		_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
	}

	// Initial fetch.
	// Should get all messages and get a Pin ID.
	msgs, err := c.Fetch(10)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	count := 0
	id := ""
	for msg := range msgs.Messages() {
		if msg == nil {
			break
		}
		msg.Ack()
		count++
		natsMsgId := msg.Headers().Get("Nats-Pin-Id")
		if id == "" {
			id = natsMsgId
		} else {
			if id != natsMsgId {
				t.Fatalf("Expected Nats-Msg-Id to be the same for all messages")
			}
		}
	}
	if count != 10 {
		t.Fatalf("Expected 10 messages, got %d", count)

	}

	// Different
	msgs2, err := c.Fetch(10, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	count = 0
	for msg := range msgs2.Messages() {
		if msg == nil {
			break
		}
		msg.Ack()
		count++
	}
	if count != 0 {
		t.Fatalf("Expected 0 messages, got %d", count)
	}
	if msgs2.Error() != nil {
		t.Fatalf("Unexpected error: %v", msgs2.Error())
	}

	msgs3, err := c.Fetch(10, jetstream.FetchMaxWait(3*time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for msg := range msgs3.Messages() {
		if msg == nil {
			break
		}
		newId := msg.Headers().Get("Nats-Pin-Id")
		if newId == id {
			t.Fatalf("Expected new pull to have different ID")
		}
		msg.Ack()
		count++
	}
	if count != 10 {
		t.Fatalf("Expected 10 messages, got %d", count)
	}
	if msgs3.Error() != nil {
		t.Fatalf("Unexpected error: %v", msgs3.Error())
	}
}

func TestConsumerCachedInfo(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:     "cons",
		AckPolicy:   jetstream.AckExplicitPolicy,
		Description: "test consumer",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info := c.CachedInfo()

	if info.Stream != "foo" {
		t.Fatalf("Invalid stream name; expected: 'foo'; got: %s", info.Stream)
	}
	if info.Config.Description != "test consumer" {
		t.Fatalf("Invalid consumer description; expected: 'test consumer'; got: %s", info.Config.Description)
	}

	// update consumer and see if info is updated
	_, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
		Durable:     "cons",
		AckPolicy:   jetstream.AckExplicitPolicy,
		Description: "updated consumer",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info = c.CachedInfo()

	if info.Stream != "foo" {
		t.Fatalf("Invalid stream name; expected: 'foo'; got: %s", info.Stream)
	}

	// description should not be updated when using cached values
	if info.Config.Description != "test consumer" {
		t.Fatalf("Invalid consumer description; expected: 'updated consumer'; got: %s", info.Config.Description)
	}

}
