// Copyright 2022-2023 The NATS Authors
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
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

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

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
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

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
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
		if _, err := c.Consume(func(msg jetstream.Msg) {}); !errors.Is(err, jetstream.ErrOrderConsumerUsedAsFetch) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrOrderConsumerUsedAsFetch, err)
		}
	})

	t.Run("error running concurrent consume requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		cc, err := c.Consume(func(msg jetstream.Msg) {})
		defer cc.Stop()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := c.Consume(func(msg jetstream.Msg) {}); !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrOrderedConsumerConcurrentRequests, err)
		}
	})

	t.Run("with auto unsubscribe", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(50)
		_, err = c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			msg.Ack()
			wg.Done()
		}, jetstream.StopAfter(50), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg.Wait()
		time.Sleep(10 * time.Millisecond)
		ci, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.NumPending != 50 {
			t.Fatalf("Unexpected number of pending messages; want 50; got %d", ci.NumPending)
		}
		if ci.NumAckPending != 0 {
			t.Fatalf("Unexpected number of ack pending messages; want 0; got %d", ci.NumAckPending)
		}
		if ci.NumWaiting != 0 {
			t.Fatalf("Unexpected number of waiting pull requests; want 0; got %d", ci.NumWaiting)
		}
		time.Sleep(10 * time.Millisecond)
	})

	t.Run("with auto unsubscribe and consumer reset", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(100)
		_, err = c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			msg.Ack()
			wg.Done()
		}, jetstream.StopAfter(150), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg.Wait()
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg.Add(50)
		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		wg.Wait()

		time.Sleep(10 * time.Millisecond)
		ci, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.NumPending != 50 {
			t.Fatalf("Unexpected number of pending messages; want 50; got %d", ci.NumPending)
		}
		if ci.NumAckPending != 0 {
			t.Fatalf("Unexpected number of ack pending messages; want 0; got %d", ci.NumAckPending)
		}
		if ci.NumWaiting != 0 {
			t.Fatalf("Unexpected number of waiting pull requests; want 0; got %d", ci.NumWaiting)
		}
		time.Sleep(10 * time.Millisecond)
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
	t.Run("base usage, delete consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer it.Stop()

		publishTestMsgs(t, nc)
		for i := 0; i < 5; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
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
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs = append(msgs, msg)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("with auto unsubscribe", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "test", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.StopAfter(50), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 50; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)

		}
		if _, err := it.Next(); err != jetstream.ErrMsgIteratorClosed {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}
		if len(msgs) != 50 {
			t.Fatalf("Unexpected received message count; want %d; got %d", 50, len(msgs))
		}
		ci, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.NumPending != 50 {
			t.Fatalf("Unexpected number of pending messages; want 50; got %d", ci.NumPending)
		}
		if ci.NumAckPending != 0 {
			t.Fatalf("Unexpected number of ack pending messages; want 0; got %d", ci.NumAckPending)
		}
		if ci.NumWaiting != 0 {
			t.Fatalf("Unexpected number of waiting pull requests; want 0; got %d", ci.NumWaiting)
		}
	})

	t.Run("with auto unsubscribe and consumer reset", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "test", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.StopAfter(150), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 100; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
		}
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		for i := 0; i < 50; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
		}
		if _, err := it.Next(); err != jetstream.ErrMsgIteratorClosed {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}
		if len(msgs) != 150 {
			t.Fatalf("Unexpected received message count; want %d; got %d", 50, len(msgs))
		}
		ci, err := c.Info(ctx)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.NumPending != 50 {
			t.Fatalf("Unexpected number of pending messages; want 50; got %d", ci.NumPending)
		}
		if ci.NumAckPending != 0 {
			t.Fatalf("Unexpected number of ack pending messages; want 0; got %d", ci.NumAckPending)
		}
		if ci.NumWaiting != 0 {
			t.Fatalf("Unexpected number of waiting pull requests; want 0; got %d", ci.NumWaiting)
		}
	})

	t.Run("consumer used as fetch", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
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
		if _, err := c.Messages(); !errors.Is(err, jetstream.ErrOrderConsumerUsedAsFetch) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrOrderConsumerUsedAsFetch, err)
		}
	})

	t.Run("error running concurrent consume requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		cc, err := c.Messages()
		defer cc.Stop()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if _, err := c.Messages(); !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrOrderedConsumerConcurrentRequests, err)
		}
	})
}

func TestOrderedConsumerFetch(t *testing.T) {
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

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)

		publishTestMsgs(t, nc)
		res, err := c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		publishTestMsgs(t, nc)
		res, err = c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("consumer used as consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		cc.Stop()

		_, err = c.Fetch(5)
		if !errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderConsumerUsedAsConsume, err)
		}
	})

	t.Run("concurrent fetch requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		res, err := c.Fetch(1, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		_, err = c.Fetch(1)
		if !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderedConsumerConcurrentRequests, err)
		}
		for msg := range res.Messages() {
			msg.Ack()
		}
	})
}

func TestOrderedConsumerFetchBytes(t *testing.T) {
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

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)

		publishTestMsgs(t, nc)
		res, err := c.FetchBytes(500, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		publishTestMsgs(t, nc)
		res, err = c.Fetch(500, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("consumer used as consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		cc.Stop()

		_, err = c.FetchBytes(500)
		if !errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderConsumerUsedAsConsume, err)
		}
	})

	t.Run("concurrent fetch requests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		res, err := c.FetchBytes(500, jetstream.FetchMaxWait(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		_, err = c.FetchBytes(500)
		if !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderedConsumerConcurrentRequests, err)
		}
		for msg := range res.Messages() {
			msg.Ack()
		}
	})
}

func TestOrderedConsumerNext(t *testing.T) {
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

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		msg, err := c.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		msg.Ack()

		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		msg, err = c.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		msg.Ack()
	})

	t.Run("consumer used as consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		cc.Stop()

		_, err = c.Next()
		if !errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderConsumerUsedAsConsume, err)
		}
	})
}

func TestOrderedConsumerFetchNoWait(t *testing.T) {
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

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)

		publishTestMsgs(t, nc)
		res, err := c.FetchNoWait(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		name := c.CachedInfo().Name
		if err := s.DeleteConsumer(ctx, name); err != nil {
			t.Fatal(err)
		}
		publishTestMsgs(t, nc)
		res, err = c.FetchNoWait(5)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}

		for msg := range res.Messages() {
			msgs = append(msgs, msg)
		}
		if res.Error() != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Expected %d messages; got: %d", 2*len(testMsgs), len(msgs))
		}
	})

	t.Run("consumer used as consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		cc.Stop()

		_, err = c.FetchNoWait(5)
		if !errors.Is(err, jetstream.ErrOrderConsumerUsedAsConsume) {
			t.Fatalf("Expected error: %s; got: %s", jetstream.ErrOrderConsumerUsedAsConsume, err)
		}
	})
}

func TestOrderedConsumerInfo(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := js.OrderedConsumer(ctx, "foo", jetstream.OrderedConsumerConfig{})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	cc, err := c.Consume(func(msg jetstream.Msg) {})
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	defer cc.Stop()

	info, err := c.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	initialName := info.Name

	if err := s.DeleteConsumer(ctx, initialName); err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	time.Sleep(50 * time.Millisecond)

	info, err = c.Info(ctx)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if info.Name == initialName {
		t.Fatalf("New consumer should be returned; got: %s", info.Name)
	}
}
