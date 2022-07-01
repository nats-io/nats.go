// Copyright 2020-2022 The NATS Authors
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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{
			Durable:     "cons",
			AckPolicy:   nats.AckExplicitPolicy,
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
		_, err = s.UpdateConsumer(ctx, nats.ConsumerConfig{
			Durable:     "cons",
			AckPolicy:   nats.AckExplicitPolicy,
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
		js, err := New(nc)
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
		if err == nil || !errors.Is(err, ErrConsumerNotFound) {
			t.Fatalf("Expected error: %v; got: %v", ErrConsumerNotFound, err)
		}
	})
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
	js, err := New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{
		Durable:     "cons",
		AckPolicy:   nats.AckExplicitPolicy,
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
	_, err = s.UpdateConsumer(ctx, nats.ConsumerConfig{
		Durable:     "cons",
		AckPolicy:   nats.AckExplicitPolicy,
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

func TestPullConsumerNext(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	t.Run("no options", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		var msg JetStreamMsg
		errs := make(chan error)
		go func() {
			for {
				nextCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err = c.Next(nextCtx)
				cancel()
				if err != nil {
					errs <- err
					break
				}
				msgs = append(msgs, msg)
			}
		}()

		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, nc)
		err = <-errs
		if !errors.Is(err, ErrNoMessages) {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with no wait, no messages at the time of request", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		var msg JetStreamMsg
		errs := make(chan error)
		go func() {
			for {
				nextCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err = c.Next(nextCtx, WithNoWait(true))
				cancel()
				if err != nil {
					errs <- err
					break
				}
				msgs = append(msgs, msg)
			}
		}()

		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, nc)

		err = <-errs
		if !errors.Is(err, ErrNoMessages) {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(msgs) != 0 {
			t.Fatalf("Expected no messages, got %d", len(msgs))
		}
	})

	t.Run("with no wait, some messages available", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		msgs := make([]JetStreamMsg, 0)
		var msg JetStreamMsg
		errs := make(chan error)
		go func() {
			for {
				nextCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err = c.Next(nextCtx, WithNoWait(true))
				cancel()
				if err != nil {
					errs <- err
					break
				}
				msgs = append(msgs, msg)
			}
		}()

		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, nc)

		err = <-errs
		if !errors.Is(err, ErrNoMessages) {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(msgs) != 5 {
			t.Fatalf("Expected no messages, got %d", len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with idle heartbeat", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		var msg JetStreamMsg
		errs := make(chan error)
		go func() {
			for {
				nextCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err = c.Next(nextCtx, WithNextHeartbeat(10*time.Millisecond))
				cancel()
				if err != nil {
					errs <- err
					break
				}
				msgs = append(msgs, msg)
			}
		}()

		time.Sleep(20 * time.Millisecond)
		publishTestMsgs(t, nc)
		err = <-errs
		if !errors.Is(err, ErrNoMessages) {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with idle heartbeat, server shutdown", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		var msg JetStreamMsg
		errs := make(chan error)
		go func() {
			for {
				nextCtx, cancel := context.WithTimeout(ctx, 100*time.Millisecond)
				msg, err = c.Next(nextCtx, WithNextHeartbeat(10*time.Millisecond))
				cancel()
				if err != nil {
					errs <- err
					break
				}
				msgs = append(msgs, msg)
			}
		}()

		shutdownJSServerAndRemoveStorage(t, srv)
		err = <-errs
		if !errors.Is(err, ErrNoHeartbeat) {
			t.Fatalf("Unexpected error: %v; expected: %v", err, ErrNoHeartbeat)
		}
	})

	t.Run("with timeout", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		var msg JetStreamMsg
		errs := make(chan error)
		go func() {
			for {
				nextCtx, cancel := context.WithTimeout(ctx, 50*time.Millisecond)
				msg, err = c.Next(nextCtx)
				cancel()
				if err != nil {
					errs <- err
					break
				}
				msgs = append(msgs, msg)
			}
		}()

		err = <-errs
		if !errors.Is(err, ErrNoMessages) {
			t.Fatalf("Unexpected error: %v; expected: %v", err, ErrNoHeartbeat)
		}
	})
}

func TestPullConsumerStream(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	t.Run("no options", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		fmt.Println(srv.StoreDir())
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		err = c.Stream(ctx, func(msg JetStreamMsg, err error) {
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		wg.Wait()
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with custom batch size", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		err = c.Stream(ctx, func(msg JetStreamMsg, err error) {
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
			wg.Done()
		}, WithBatchSize(2))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		wg.Wait()
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// with batch size set to 2, and 5 messages published on subject, there should be a total of 3 requests sent
		if requestsNum != 3 {
			t.Fatalf("Unexpected number of requests sent; want 3; got %d", requestsNum)
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with invalid batch size", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		err = c.Stream(ctx, func(_ JetStreamMsg, _ error) {
		}, WithBatchSize(-1))
		if err == nil || !errors.Is(err, nats.ErrInvalidArg) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrInvalidArg, err)
		}
	})

	t.Run("with custom expiry", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		err = c.Stream(ctx, func(msg JetStreamMsg, err error) {
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
			wg.Done()
		}, WithExpiry(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		time.Sleep(60 * time.Millisecond)
		publishTestMsgs(t, nc)
		wg.Wait()

		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// with expiry set to 50ms, and 60ms wait before messages are published, there should be a total of 2 requests sent to the server
		if requestsNum != 2 {
			t.Fatalf("Unexpected number of requests sent; want 3; got %d", requestsNum)
		}
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with invalid expiry", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		err = c.Stream(ctx, func(_ JetStreamMsg, _ error) {
		}, WithExpiry(-1))
		if err == nil || !errors.Is(err, nats.ErrInvalidArg) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrInvalidArg, err)
		}
	})

	t.Run("with idle heartbeat", func(t *testing.T) {
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]JetStreamMsg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		err = c.Stream(ctx, func(msg JetStreamMsg, err error) {
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
			wg.Done()
		}, WithStreamHeartbeat(10*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		wg.Wait()
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with idle heartbeat, server shutdown", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		fmt.Println(srv.StoreDir())
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

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s, err := js.CreateStream(ctx, nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateConsumer(ctx, nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := &sync.WaitGroup{}
		wg.Add(1)
		err = c.Stream(ctx, func(_ JetStreamMsg, err error) {
			if !errors.Is(err, ErrNoHeartbeat) {
				t.Fatalf("Unexpected error: %v; expected: %v", err, ErrNoHeartbeat)
			}
			wg.Done()
		}, WithStreamHeartbeat(10*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		shutdownJSServerAndRemoveStorage(t, srv)
		wg.Wait()
	})
}
