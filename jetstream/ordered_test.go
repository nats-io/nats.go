// Copyright 2020-2023 The NATS Authors
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

package jetstream

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestOrderedConsumerConsume(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	t.Run("base usage, delete consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		wg.Wait()

		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		wg.Add(len(testMsgs))
		publishTestMsgs(t, nc)
		wg.Wait()

		l.Stop()
	})

	t.Run("consumer used as fetch", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		msgs, err := c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for range msgs.Messages() {
		}
		if _, err := c.Consume(func(msg Msg) {}); !errors.Is(err, ErrOrderConsumerUsedAsFetch) {
			t.Fatalf("Expected error: %v; got: %v", ErrOrderConsumerUsedAsFetch, err)
		}
	})

	t.Run("error running concurrent consume requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		cc, err := c.Consume(func(msg Msg) {})
		defer cc.Stop()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := c.Consume(func(msg Msg) {}); !errors.Is(err, ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %v; got: %v", ErrOrderedConsumerConcurrentRequests, err)
		}
	})
}

func TestOrderedConsumerMessages(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := s.OrderedConsumer(ctx, OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msgs := make([]Msg, 0)
	it, err := c.Messages()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer it.Stop()
	publishTestMsgs(t, nc)
	for i := 0; i < 5; i++ {
		msg, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		msg.Ack()
		msgs = append(msgs, msg)
	}

	name := c.CachedInfo().Name
	if err := s.DeleteConsumer(ctx, name); err != nil {
		t.Fatal(err)
	}
	publishTestMsgs(t, nc)
	for i := 0; i < 5; i++ {
		msg, err := it.Next()
		if err != nil {
			t.Fatal(err)
		}
		msg.Ack()
		msgs = append(msgs, msg)
	}
}
