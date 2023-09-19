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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPullConsumerFetch(t *testing.T) {
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
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		msgs, err := c.Fetch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var i int
		for msg := range msgs.Messages() {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
			i++
		}
		if len(testMsgs) != i {
			t.Fatalf("Invalid number of messages received; want: %d; got: %d", len(testMsgs), i)
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}
	})

	t.Run("delete consumer during fetch", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		msgs, err := c.Fetch(10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Error deleting consumer: %s", err)
		}

		var i int
		for msg := range msgs.Messages() {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
			i++
		}
		if len(testMsgs) != i {
			t.Fatalf("Invalid number of messages received; want: %d; got: %d", len(testMsgs), i)
		}
		if !errors.Is(msgs.Error(), jetstream.ErrConsumerDeleted) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrConsumerDeleted, msgs.Error())
		}
	})

	t.Run("no options, fetch single messages one by one", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		res := make([]jetstream.Msg, 0)
		errs := make(chan error)
		done := make(chan struct{})
		go func() {
			for {
				if len(res) == len(testMsgs) {
					close(done)
					return
				}
				msgs, err := c.Fetch(1)
				if err != nil {
					errs <- err
					return
				}

				msg := <-msgs.Messages()
				if msg != nil {
					res = append(res, msg)
				}
				if err := msgs.Error(); err != nil {
					errs <- err
					return
				}
			}
		}()

		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, nc)
		select {
		case err := <-errs:
			t.Fatalf("Unexpected error: %v", err)
		case <-done:
			if len(res) != len(testMsgs) {
				t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(res))
			}
		}
		for i, msg := range res {
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
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs, err := c.FetchNoWait(5)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		publishTestMsgs(t, nc)

		msg := <-msgs.Messages()
		if msg != nil {
			t.Fatalf("Expected no messages; got: %s", string(msg.Data()))
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
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		time.Sleep(50 * time.Millisecond)
		msgs, err := c.FetchNoWait(10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		publishTestMsgs(t, nc)

		var msgsNum int
		for range msgs.Messages() {
			msgsNum++
		}
		if err != nil {
			t.Fatalf("Unexpected error during fetch: %v", err)
		}

		if msgsNum != len(testMsgs) {
			t.Fatalf("Expected 5 messages, got: %d", msgsNum)
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
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs, err := c.Fetch(5, jetstream.FetchMaxWait(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msg := <-msgs.Messages()
		if msg != nil {
			t.Fatalf("Expected no messages; got: %s", string(msg.Data()))
		}
	})

	t.Run("with invalid timeout value", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Fetch(5, jetstream.FetchMaxWait(-50*time.Millisecond))
		if !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}
	})
}

func TestPullConsumerFetchBytes(t *testing.T) {
	testSubject := "FOO.123"
	msg := [10]byte{}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn, count int) {
		for i := 0; i < count; i++ {
			if err := nc.Publish(testSubject, msg[:]); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	t.Run("no options, exact byte count received", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, Name: "con"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc, 5)
		// actual received msg size will be 60 (payload=10 + Subject=7 + Reply=43)
		msgs, err := c.FetchBytes(300)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var i int
		for msg := range msgs.Messages() {
			msg.Ack()
			i++
		}
		if i != 5 {
			t.Fatalf("Expected 5 messages; got: %d", i)
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}
	})

	t.Run("no options, last msg does not fit max bytes", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, Name: "con"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc, 5)
		// actual received msg size will be 60 (payload=10 + Subject=7 + Reply=43)
		msgs, err := c.FetchBytes(250)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var i int
		for msg := range msgs.Messages() {
			msg.Ack()
			i++
		}
		if i != 4 {
			t.Fatalf("Expected 5 messages; got: %d", i)
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}
	})
	t.Run("no options, single msg is too large", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, Name: "con"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc, 5)
		// actual received msg size will be 60 (payload=10 + Subject=7 + Reply=43)
		msgs, err := c.FetchBytes(30)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var i int
		for msg := range msgs.Messages() {
			msg.Ack()
			i++
		}
		if i != 0 {
			t.Fatalf("Expected 5 messages; got: %d", i)
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}
	})

	t.Run("timeout waiting for messages", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, Name: "con"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc, 5)
		// actual received msg size will be 60 (payload=10 + Subject=7 + Reply=43)
		msgs, err := c.FetchBytes(1000, jetstream.FetchMaxWait(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		var i int
		for msg := range msgs.Messages() {
			msg.Ack()
			i++
		}
		if i != 5 {
			t.Fatalf("Expected 5 messages; got: %d", i)
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}
	})
}

func TestPullConsumerFetch_WithCluster(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	name := "cluster"
	stream := jetstream.StreamConfig{
		Name:     name,
		Replicas: 1,
		Subjects: []string{"FOO.*"},
	}
	t.Run("no options", func(t *testing.T) {
		withJSClusterAndStream(t, name, 3, stream, func(t *testing.T, subject string, srvs ...*jsServer) {
			srv := srvs[0]
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

			s, err := js.Stream(ctx, stream.Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			publishTestMsgs(t, nc)
			msgs, err := c.Fetch(5)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var i int
			for msg := range msgs.Messages() {
				if string(msg.Data()) != testMsgs[i] {
					t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
				}
				i++
			}
			if msgs.Error() != nil {
				t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
			}
		})
	})

	t.Run("with no wait, no messages at the time of request", func(t *testing.T) {
		withJSClusterAndStream(t, name, 3, stream, func(t *testing.T, subject string, srvs ...*jsServer) {
			nc, err := nats.Connect(srvs[0].ClientURL())
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

			s, err := js.Stream(ctx, stream.Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			msgs, err := c.FetchNoWait(5)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			time.Sleep(100 * time.Millisecond)
			publishTestMsgs(t, nc)

			msg := <-msgs.Messages()
			if msg != nil {
				t.Fatalf("Expected no messages; got: %s", string(msg.Data()))
			}
		})
	})
}

func TestPullConsumerMessages(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()

		// calling Stop() multiple times should have no effect
		it.Stop()
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
		_, err = it.Next()
		if err == nil || !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}
	})

	t.Run("with custom batch size", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.PullMaxMessages(3))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		for i, msg := range msgs {
			if string(msg.Data()) != testMsgs[i] {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
			}
		}
	})

	t.Run("with max fitting 1 message", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.PullMaxBytes(60))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// with batch size set to 1, and 5 messages published on subject, there should be a total of 5 requests sent
		if requestsNum < 5 {
			t.Fatalf("Unexpected number of requests sent; want at least 5; got %d", requestsNum)
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

	t.Run("remove consumer when fetching messages", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer it.Stop()

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Error deleting consumer: %s", err)
		}
		_, err = it.Next()
		if !errors.Is(err, jetstream.ErrConsumerDeleted) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrConsumerDeleted, err)
		}
		publishTestMsgs(t, nc)
		time.Sleep(50 * time.Millisecond)
		_, err = it.Next()
		if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
	})

	t.Run("with custom max bytes", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.PullMaxBytes(150))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if requestsNum < 3 {
			t.Fatalf("Unexpected number of requests sent; want at least 3; got %d", requestsNum)
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

	t.Run("with batch size set to 1", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages(jetstream.PullMaxMessages(1))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// with batch size set to 1, and 5 messages published on subject, there should be a total of 5 requests sent
		if requestsNum != 5 {
			t.Fatalf("Unexpected number of requests sent; want 5; got %d", requestsNum)
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
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
			if err := msg.DoubleAck(ctx); err != nil {
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

	t.Run("create iterator, stop, then create again", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		time.Sleep(10 * time.Millisecond)

		publishTestMsgs(t, nc)
		it, err = c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			if msg == nil {
				break
			}
			msg.Ack()
			msgs = append(msgs, msg)

		}
		it.Stop()
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		expectedMsgs := append(testMsgs, testMsgs...)
		for i, msg := range msgs {
			if string(msg.Data()) != expectedMsgs[i] {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Messages(jetstream.PullMaxMessages(-1))
		if err == nil || !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}
	})

	t.Run("with server restart", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs := make([]jetstream.Msg, 0)
		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer it.Stop()

		done := make(chan struct{})
		errs := make(chan error)
		publishTestMsgs(t, nc)
		go func() {
			for i := 0; i < 2*len(testMsgs); i++ {
				msg, err := it.Next()
				if err != nil {
					errs <- err
					return
				}
				msg.Ack()
				msgs = append(msgs, msg)
			}
			done <- struct{}{}
		}()
		time.Sleep(10 * time.Millisecond)
		// restart the server
		srv = restartBasicJSServer(t, srv)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, nc)

		select {
		case <-done:
			if len(msgs) != 2*len(testMsgs) {
				t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
			}
		case err := <-errs:
			t.Fatalf("Unexpected error: %s", err)
		}
	})

	t.Run("with graceful shutdown", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		it, err := c.Messages()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)

		errs := make(chan error)
		msgs := make([]jetstream.Msg, 0)

		go func() {
			for {
				msg, err := it.Next()
				if err != nil {
					errs <- err
					return
				}
				msg.Ack()
				msgs = append(msgs, msg)
			}
		}()

		time.Sleep(10 * time.Millisecond)
		it.Stop() // Next() should return ErrMsgIteratorClosed

		timeout := time.NewTimer(5 * time.Second)

		select {
		case <-timeout.C:
			t.Fatal("Timed out waiting for Next() to return after Stop()")
		case err := <-errs:
			if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				t.Fatalf("Unexpected error: %v", err)
			}

			if len(msgs) != len(testMsgs) {
				t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
			}
		}
	})
}

func TestPullConsumerConsume(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
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
		defer l.Stop()

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

	t.Run("subscribe twice on the same consumer", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := sync.WaitGroup{}
		msgs1, msgs2 := make([]jetstream.Msg, 0), make([]jetstream.Msg, 0)
		l1, err := c.Consume(func(msg jetstream.Msg) {
			msgs1 = append(msgs1, msg)
			wg.Done()
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l1.Stop()
		l2, err := c.Consume(func(msg jetstream.Msg) {
			msgs2 = append(msgs2, msg)
			wg.Done()
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l2.Stop()

		wg.Add(len(testMsgs))
		publishTestMsgs(t, nc)
		wg.Wait()

		if len(msgs1)+len(msgs2) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs1)+len(msgs2))
		}
		if len(msgs1) == 0 || len(msgs2) == 0 {
			t.Fatalf("Received no messages on one of the subscriptions")
		}
	})

	t.Run("subscribe, cancel subscription, then subscribe again", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := sync.WaitGroup{}
		wg.Add(len(testMsgs))
		msgs := make([]jetstream.Msg, 0)
		l, err := c.Consume(func(msg jetstream.Msg) {
			if err := msg.Ack(); err != nil {
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
		l.Stop()

		time.Sleep(10 * time.Millisecond)
		wg.Add(len(testMsgs))
		l, err = c.Consume(func(msg jetstream.Msg) {
			if err := msg.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()
		publishTestMsgs(t, nc)
		wg.Wait()
		if len(msgs) != 2*len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		expectedMsgs := append(testMsgs, testMsgs...)
		for i, msg := range msgs {
			if string(msg.Data()) != expectedMsgs[i] {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, jetstream.PullMaxMessages(4))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

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

	t.Run("fetch messages one by one", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, jetstream.PullMaxMessages(1))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

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

	t.Run("remove consumer during consume", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		errs := make(chan error, 10)
		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, jetstream.ConsumeErrHandler(func(consumeCtx jetstream.ConsumeContext, err error) {
			errs <- err
		}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		publishTestMsgs(t, nc)
		wg.Wait()
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Error deleting consumer: %s", err)
		}
		select {
		case err := <-errs:
			if !errors.Is(err, jetstream.ErrConsumerDeleted) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrConsumerDeleted, err)
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for %v", jetstream.ErrConsumerDeleted)
		}
		publishTestMsgs(t, nc)
		time.Sleep(50 * time.Millisecond)
		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
		}
	})

	t.Run("with custom max bytes", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// subscribe to next request subject to verify how many next requests were sent
		sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.foo.%s", c.CachedInfo().Name))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		publishTestMsgs(t, nc)
		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, jetstream.PullMaxBytes(150))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

		wg.Wait()
		requestsNum, _, err := sub.Pending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// new request should be sent after each consumed message (msg size is 57)
		if requestsNum < 3 {
			t.Fatalf("Unexpected number of requests sent; want at least 5; got %d", requestsNum)
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
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
	})

	t.Run("with invalid batch size", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Consume(func(_ jetstream.Msg) {
		}, jetstream.PullMaxMessages(-1))
		if err == nil || !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}
	})

	t.Run("with custom expiry", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, jetstream.PullExpiry(2*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

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

	t.Run("with invalid expiry", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = c.Consume(func(_ jetstream.Msg) {
		}, jetstream.PullExpiry(-1))
		if err == nil || !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}
	})

	t.Run("with idle heartbeat", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		msgs := make([]jetstream.Msg, 0)
		wg := &sync.WaitGroup{}
		wg.Add(len(testMsgs))
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		}, jetstream.PullMaxBytes(1*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()

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

	t.Run("with server restart", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		wg := &sync.WaitGroup{}
		wg.Add(2 * len(testMsgs))
		msgs := make([]jetstream.Msg, 0)
		publishTestMsgs(t, nc)
		l, err := c.Consume(func(msg jetstream.Msg) {
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer l.Stop()
		time.Sleep(10 * time.Millisecond)
		// restart the server
		srv = restartBasicJSServer(t, srv)
		defer shutdownJSServerAndRemoveStorage(t, srv)
		time.Sleep(10 * time.Millisecond)
		publishTestMsgs(t, nc)
		wg.Wait()
	})
}

func TestPullConsumerConsume_WithCluster(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, nc *nats.Conn) {
		for _, msg := range testMsgs {
			if err := nc.Publish(testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	name := "cluster"
	stream := jetstream.StreamConfig{
		Name:     name,
		Replicas: 1,
		Subjects: []string{"FOO.*"},
	}

	t.Run("no options", func(t *testing.T) {
		withJSClusterAndStream(t, name, 3, stream, func(t *testing.T, subject string, srvs ...*jsServer) {
			nc, err := nats.Connect(srvs[0].ClientURL())
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
			s, err := js.Stream(ctx, stream.Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
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
			defer l.Stop()

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
	})

	t.Run("subscribe, cancel subscription, then subscribe again", func(t *testing.T) {
		withJSClusterAndStream(t, name, 3, stream, func(t *testing.T, subject string, srvs ...*jsServer) {
			nc, err := nats.Connect(srvs[0].ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			s, err := js.Stream(ctx, stream.Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			wg := sync.WaitGroup{}
			wg.Add(len(testMsgs))
			msgs := make([]jetstream.Msg, 0)
			l, err := c.Consume(func(msg jetstream.Msg) {
				if err := msg.Ack(); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				msgs = append(msgs, msg)
				if len(msgs) == 5 {
					cancel()
				}
				wg.Done()
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			publishTestMsgs(t, nc)
			wg.Wait()
			l.Stop()

			time.Sleep(10 * time.Millisecond)
			wg.Add(len(testMsgs))
			defer cancel()
			l, err = c.Consume(func(msg jetstream.Msg) {
				if err := msg.Ack(); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				msgs = append(msgs, msg)
				wg.Done()
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer l.Stop()
			publishTestMsgs(t, nc)
			wg.Wait()
			if len(msgs) != 2*len(testMsgs) {
				t.Fatalf("Unexpected received message count; want %d; got %d", len(testMsgs), len(msgs))
			}
			expectedMsgs := append(testMsgs, testMsgs...)
			for i, msg := range msgs {
				if string(msg.Data()) != expectedMsgs[i] {
					t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, testMsgs[i], string(msg.Data()))
				}
			}
		})
	})
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
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		publishTestMsgs(t, nc)
		msgs := make([]jetstream.Msg, 0)

		var i int
		for i := 0; i < len(testMsgs); i++ {
			msg, err := c.Next()
			if err != nil {
				t.Fatalf("Error fetching message: %s", err)
			}
			msgs = append(msgs, msg)
		}
		if len(testMsgs) != len(msgs) {
			t.Fatalf("Invalid number of messages received; want: %d; got: %d", len(testMsgs), i)
		}
	})

	t.Run("delete consumer while waiting for message", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		time.AfterFunc(100*time.Millisecond, func() {
			if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
				t.Fatalf("Error deleting consumer: %s", err)
			}
		})

		if _, err := c.Next(); !errors.Is(err, jetstream.ErrConsumerDeleted) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrConsumerDeleted, err)
		}
		time.Sleep(100 * time.Millisecond)
	})

	t.Run("with custom timeout", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := c.Next(jetstream.FetchMaxWait(50 * time.Millisecond)); !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("Expected timeout; got: %s", err)
		}
	})
}
