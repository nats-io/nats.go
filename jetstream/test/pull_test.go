// Copyright 2022-2024 The NATS Authors
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
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)

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

		publishTestMsgs(t, js)
		time.Sleep(50 * time.Millisecond)
		msgs, err := c.FetchNoWait(10)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		publishTestMsgs(t, js)

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

	t.Run("with missing heartbeat", func(t *testing.T) {
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
		publishTestMsgs(t, js)
		// fetch 5 messages, should return normally
		msgs, err := c.Fetch(5, jetstream.FetchHeartbeat(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var i int
		for range msgs.Messages() {
			i++
		}
		if i != len(testMsgs) {
			t.Fatalf("Expected 5 messages; got: %d", i)
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}

		// fetch again, should timeout without any error
		msgs, err = c.Fetch(5, jetstream.FetchHeartbeat(50*time.Millisecond), jetstream.FetchMaxWait(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		select {
		case _, ok := <-msgs.Messages():
			if ok {
				t.Fatalf("Expected channel to be closed")
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Expected channel to be closed")
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}

		// delete the consumer, at this point server should stop sending heartbeats for pull requests
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Error deleting consumer: %s", err)
		}
		msgs, err = c.Fetch(5, jetstream.FetchHeartbeat(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case _, ok := <-msgs.Messages():
			if ok {
				t.Fatalf("Expected channel to be closed")
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Expected channel to be closed")
		}
		if !errors.Is(msgs.Error(), jetstream.ErrNoHeartbeat) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrNoHeartbeat, err)
		}
	})

	t.Run("with invalid heartbeat value", func(t *testing.T) {
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

		// default expiry (30s), hb too large
		_, err = c.Fetch(5, jetstream.FetchHeartbeat(20*time.Second))
		if !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}

		// custom expiry, hb too large
		_, err = c.Fetch(5, jetstream.FetchHeartbeat(2*time.Second), jetstream.FetchMaxWait(3*time.Second))
		if !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}

		// negative heartbeat
		_, err = c.Fetch(5, jetstream.FetchHeartbeat(-2*time.Second))
		if !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}
	})
}

func TestPullConsumerFetchBytes(t *testing.T) {
	testSubject := "FOO.123"
	msg := [10]byte{}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream, count int) {
		for i := 0; i < count; i++ {
			if _, err := js.Publish(context.Background(), testSubject, msg[:]); err != nil {
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

		publishTestMsgs(t, js, 5)
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

		publishTestMsgs(t, js, 5)
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

		publishTestMsgs(t, js, 5)
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

		publishTestMsgs(t, js, 5)
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

	t.Run("with missing heartbeat", func(t *testing.T) {
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

		// fetch again, should timeout without any error
		msgs, err := c.FetchBytes(5, jetstream.FetchHeartbeat(50*time.Millisecond), jetstream.FetchMaxWait(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		select {
		case _, ok := <-msgs.Messages():
			if ok {
				t.Fatalf("Expected channel to be closed")
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Expected channel to be closed")
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error during fetch: %v", msgs.Error())
		}

		// delete the consumer, at this point server should stop sending heartbeats for pull requests
		if err := s.DeleteConsumer(ctx, c.CachedInfo().Name); err != nil {
			t.Fatalf("Error deleting consumer: %s", err)
		}
		msgs, err = c.FetchBytes(5, jetstream.FetchHeartbeat(50*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case _, ok := <-msgs.Messages():
			if ok {
				t.Fatalf("Expected channel to be closed")
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Expected channel to be closed")
		}
		if !errors.Is(msgs.Error(), jetstream.ErrNoHeartbeat) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrNoHeartbeat, err)
		}
	})

	t.Run("with invalid heartbeat value", func(t *testing.T) {
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

		// default expiry (30s), hb too large
		_, err = c.FetchBytes(5, jetstream.FetchHeartbeat(20*time.Second))
		if !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}

		// custom expiry, hb too large
		_, err = c.FetchBytes(5, jetstream.FetchHeartbeat(2*time.Second), jetstream.FetchMaxWait(3*time.Second))
		if !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}

		// negative heartbeat
		_, err = c.FetchBytes(5, jetstream.FetchHeartbeat(-2*time.Second))
		if !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrInvalidOption, err)
		}
	})
}

func TestPullConsumerFetch_WithCluster(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
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

			publishTestMsgs(t, js)
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
			publishTestMsgs(t, js)

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
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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

	t.Run("with auto unsubscribe concurrent", func(t *testing.T) {
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
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		it, err := c.Messages(jetstream.StopAfter(50), jetstream.PullMaxMessages(40))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := 0; i < 100; i++ {
			if _, err := js.Publish(ctx, "FOO.A", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}

		var mu sync.Mutex // Mutex to guard the msgs slice.
		msgs := make([]jetstream.Msg, 0)
		var wg sync.WaitGroup

		wg.Add(50)
		for i := 0; i < 50; i++ {
			go func() {
				defer wg.Done()

				msg, err := it.Next()
				if err != nil {
					return
				}

				ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
				defer cancel()
				if err := msg.DoubleAck(ctx); err == nil {
					// Only append the msg if ack is successful.
					mu.Lock()
					msgs = append(msgs, msg)
					mu.Unlock()
				}
			}()
		}

		wg.Wait()

		// Call Next in a goroutine so we can timeout if it doesn't return.
		errs := make(chan error)
		go func() {
			// This call should return the error ErrMsgIteratorClosed.
			_, err := it.Next()
			errs <- err
		}()

		timer := time.NewTimer(5 * time.Second)
		defer timer.Stop()

		select {
		case <-timer.C:
			t.Fatal("Timed out waiting for Next() to return")
		case err := <-errs:
			if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		mu.Lock()
		wantLen, gotLen := 50, len(msgs)
		mu.Unlock()
		if wantLen != gotLen {
			t.Fatalf("Unexpected received message count; want %d; got %d", wantLen, gotLen)
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)

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
		cases := map[string]func(jetstream.MessagesContext){
			"stop":  func(mc jetstream.MessagesContext) { mc.Stop() },
			"drain": func(mc jetstream.MessagesContext) { mc.Drain() },
		}

		for name, unsubscribe := range cases {
			t.Run(name, func(t *testing.T) {
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

				publishTestMsgs(t, js)

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
				unsubscribe(it) // Next() should return ErrMsgIteratorClosed

				timer := time.NewTimer(5 * time.Second)
				defer timer.Stop()

				select {
				case <-timer.C:
					t.Fatal("Timed out waiting for Next() to return")
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
	})

	t.Run("no messages received after stop", func(t *testing.T) {
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

		publishTestMsgs(t, js)
		go func() {
			time.Sleep(100 * time.Millisecond)
			it.Stop()
		}()
		for i := 0; i < 2; i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(80 * time.Millisecond)
			msg.Ack()
			msgs = append(msgs, msg)
		}
		_, err = it.Next()
		if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}

		if len(msgs) != 2 {
			t.Fatalf("Unexpected received message count after drain; want %d; got %d", len(testMsgs), len(msgs))
		}
	})

	t.Run("drain mode", func(t *testing.T) {
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

		publishTestMsgs(t, js)
		go func() {
			time.Sleep(100 * time.Millisecond)
			it.Drain()
		}()
		for i := 0; i < len(testMsgs); i++ {
			msg, err := it.Next()
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			msgs = append(msgs, msg)
		}
		_, err = it.Next()
		if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
			t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgIteratorClosed, err)
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count after drain; want %d; got %d", len(testMsgs), len(msgs))
		}
	})
}

func TestPullConsumerConsume(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
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

		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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

		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)
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
		publishTestMsgs(t, js)
		wg.Wait()
	})

	t.Run("no messages received after stop", func(t *testing.T) {
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
		wg := &sync.WaitGroup{}
		wg.Add(2)
		publishTestMsgs(t, js)
		msgs := make([]jetstream.Msg, 0)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(80 * time.Millisecond)
			msg.Ack()
			msgs = append(msgs, msg)
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		cc.Stop()
		wg.Wait()
		// wait for some time to make sure no new messages are received
		time.Sleep(100 * time.Millisecond)

		if len(msgs) != 2 {
			t.Fatalf("Unexpected received message count after stop; want 2; got %d", len(msgs))
		}
	})

	t.Run("drain mode", func(t *testing.T) {
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
		wg := &sync.WaitGroup{}
		wg.Add(5)
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		cc.Drain()
		wg.Wait()
	})

	t.Run("wait for closed after drain", func(t *testing.T) {
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
		lock := sync.Mutex{}
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			lock.Lock()
			msgs = append(msgs, msg)
			lock.Unlock()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		closed := cc.Closed()
		time.Sleep(100 * time.Millisecond)

		cc.Drain()

		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consume to be closed")
		}

		if len(msgs) != len(testMsgs) {
			t.Fatalf("Unexpected received message count after consume closed; want %d; got %d", len(testMsgs), len(msgs))
		}
	})

	t.Run("wait for closed after stop", func(t *testing.T) {
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
		lock := sync.Mutex{}
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
			lock.Lock()
			msgs = append(msgs, msg)
			lock.Unlock()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
		closed := cc.Closed()

		cc.Stop()

		select {
		case <-closed:
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consume to be closed")
		}

		if len(msgs) < 1 || len(msgs) > 3 {
			t.Fatalf("Unexpected received message count after consume closed; want 1-3; got %d", len(msgs))
		}
	})

	t.Run("wait for closed on already closed consume", func(t *testing.T) {
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
		publishTestMsgs(t, js)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			time.Sleep(50 * time.Millisecond)
			msg.Ack()
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(100 * time.Millisecond)

		cc.Stop()

		time.Sleep(100 * time.Millisecond)

		select {
		case <-cc.Closed():
		case <-time.After(5 * time.Second):
			t.Fatalf("Timeout waiting for consume to be closed")
		}
	})
}

func TestPullConsumerConsume_WithCluster(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	name := "cluster"
	singleStream := jetstream.StreamConfig{
		Name:     name,
		Replicas: 1,
		Subjects: []string{"FOO.*"},
	}

	streamWithReplicas := jetstream.StreamConfig{
		Name:     name,
		Replicas: 3,
		Subjects: []string{"FOO.*"},
	}

	for _, stream := range []jetstream.StreamConfig{singleStream, streamWithReplicas} {
		t.Run(fmt.Sprintf("num replicas: %d, no options", stream.Replicas), func(t *testing.T) {
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

				publishTestMsgs(t, js)
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

		t.Run(fmt.Sprintf("num replicas: %d, subscribe, cancel subscription, then subscribe again", stream.Replicas), func(t *testing.T) {
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
				defer cancel()
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

				publishTestMsgs(t, js)
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
				publishTestMsgs(t, js)
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

		t.Run(fmt.Sprintf("num replicas: %d, recover consume after server restart", stream.Replicas), func(t *testing.T) {
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
				defer cancel()
				s, err := js.Stream(ctx, streamWithReplicas.Name)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, InactiveThreshold: 10 * time.Second})
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
				}, jetstream.PullExpiry(1*time.Second), jetstream.PullHeartbeat(500*time.Millisecond))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				defer l.Stop()

				publishTestMsgs(t, js)
				wg.Wait()

				time.Sleep(10 * time.Millisecond)
				srvs[0].Shutdown()
				srvs[1].Shutdown()
				srvs[0].Restart()
				srvs[1].Restart()
				wg.Add(len(testMsgs))

				for i := 0; i < 10; i++ {
					time.Sleep(500 * time.Millisecond)
					if _, err := js.Stream(context.Background(), stream.Name); err == nil {
						break
					} else if i == 9 {
						t.Fatal("JetStream not recovered: ", err)
					}
				}
				publishTestMsgs(t, js)
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
}

func TestPullConsumerNext(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
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

		publishTestMsgs(t, js)
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
