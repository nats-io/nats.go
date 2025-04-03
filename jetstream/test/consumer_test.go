// Copyright 2023-2025 The NATS Authors
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
	"sync/atomic"
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
		if info.Config.PauseUntil != nil {
			t.Fatalf("Consumer should not be paused")
		}
		if info.Paused != false {
			t.Fatalf("Consumer should not be paused")
		}
		if info.PauseRemaining != 0 {
			t.Fatalf("Consumer should not be paused")
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

func TestConsumerOverflow(t *testing.T) {
	t.Run("fetch", func(t *testing.T) {
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
			PriorityPolicy: jetstream.PriorityPolicyOverflow,
			PriorityGroups: []string{"A"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Check that consumer got proper priority policy and TTL
		info := c.CachedInfo()
		if info.Config.PriorityPolicy != jetstream.PriorityPolicyOverflow {
			t.Fatalf("Invalid priority policy; expected: %v; got: %v", jetstream.PriorityPolicyOverflow, info.Config.PriorityPolicy)
		}

		for range 100 {
			_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// We are below overflow, so we should not get any messages.
		msgs, err := c.Fetch(10, jetstream.FetchMinPending(110), jetstream.FetchMaxWait(500*time.Millisecond), jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count := 0
		for msg := range msgs.Messages() {
			msg.Ack()
			count++
		}
		if count != 0 {
			t.Fatalf("Expected 0 messages, got %d", count)
		}

		// Add more messages
		for range 100 {
			_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		msgs, err = c.Fetch(10, jetstream.FetchMinPending(110), jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count = 0
		for msg := range msgs.Messages() {
			if err := msg.DoubleAck(context.Background()); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			count++
		}
		if count != 10 {
			t.Fatalf("Expected 10 messages, got %d", count)
		}

		// try fetching messages with min ack pending
		msgs, err = c.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond), jetstream.FetchMinAckPending(10), jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count = 0
		for msg := range msgs.Messages() {
			msg.Ack()
			count++
		}
		if count != 0 {
			t.Fatalf("Expected 0 messages, got %d", count)
		}

		// now fetch some more messages but do not ack them,
		// we will test min ack pending
		msgs, err = c.Fetch(10, jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count = 0
		for range msgs.Messages() {
			count++
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error: %v", msgs.Error())
		}
		if count != 10 {
			t.Fatalf("Expected 10 messages, got %d", count)
		}

		// now fetch messages with min ack pending
		msgs, err = c.Fetch(10, jetstream.FetchMinAckPending(10), jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count = 0
		for msg := range msgs.Messages() {
			msg.Ack()
			count++
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error: %v", msgs.Error())
		}
		if count != 10 {
			t.Fatalf("Expected 10 messages, got %d", count)
		}
	})

	t.Run("consume", func(t *testing.T) {
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
			PriorityPolicy: jetstream.PriorityPolicyOverflow,
			PriorityGroups: []string{"A"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Check that consumer got proper priority policy and TTL
		info := c.CachedInfo()
		if info.Config.PriorityPolicy != jetstream.PriorityPolicyOverflow {
			t.Fatalf("Invalid priority policy; expected: %v; got: %v", jetstream.PriorityPolicyOverflow, info.Config.PriorityPolicy)
		}

		for i := range 100 {
			_, err = js.Publish(ctx, "FOO.bar", []byte(fmt.Sprintf("hello %d", i)))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		count := atomic.Uint32{}

		handler := func(m jetstream.Msg) {
			if err := m.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			count.Add(1)
		}
		cc, err := c.Consume(handler,
			jetstream.PullPriorityGroup("A"),
			jetstream.PullMinPending(110),
			jetstream.PullMaxMessages(20),
			jetstream.PullExpiry(time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer cc.Stop()

		time.Sleep(3 * time.Second)
		// there are 100 messages on the stream, and min pending is 110
		// so we should not get any messages
		if count.Load() != 0 {
			t.Fatalf("Expected 0 messages, got %d", count.Load())
		}

		// Add more messages
		for i := 100; i < 200; i++ {
			_, err = js.Publish(ctx, "FOO.bar", []byte(fmt.Sprintf("hello %d", i)))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		time.Sleep(100 * time.Millisecond)
		// now we should get 91 messages, because `Consume` will
		// keep getting messages until it drops below min pending
		if count.Load() != 91 {
			t.Fatalf("Expected 10 messages, got %d", count.Load())
		}
		cc.Stop()

		// now test min ack pending
		count.Store(0)

		// consume with min ack pending
		cc, err = c.Consume(handler,
			jetstream.PullPriorityGroup("A"),
			jetstream.PullMinAckPending(5),
			jetstream.PullMaxMessages(20))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer cc.Stop()

		time.Sleep(100 * time.Millisecond)
		// no messages should be received, there are no pending acks
		if count.Load() != 0 {
			t.Fatalf("Expected 0 messages, got %d", count.Load())
		}
		// fetch some messages, do not ack them
		msgs, err := c.Fetch(10, jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		i := 0
		for range msgs.Messages() {
			i++
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error: %v", msgs.Error())
		}
		if i != 10 {
			t.Fatalf("Expected 10 messages, got %d", i)
		}

		time.Sleep(100 * time.Millisecond)
		// we should process the rest of the stream minus the 10 unacked messages
		// 200 - 91 - 10 = 99
		if count.Load() != 99 {
			t.Fatalf("Expected 5 messages, got %d", count.Load())
		}
	})

	t.Run("messages", func(t *testing.T) {
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
			PriorityPolicy: jetstream.PriorityPolicyOverflow,
			PriorityGroups: []string{"A"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Check that consumer got proper priority policy and TTL
		info := c.CachedInfo()
		if info.Config.PriorityPolicy != jetstream.PriorityPolicyOverflow {
			t.Fatalf("Invalid priority policy; expected: %v; got: %v", jetstream.PriorityPolicyOverflow, info.Config.PriorityPolicy)
		}

		for i := range 100 {
			_, err = js.Publish(ctx, "FOO.bar", []byte(fmt.Sprintf("hello %d", i)))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		count := atomic.Uint32{}
		errs := make(chan error, 10)
		done := make(chan struct{}, 1)
		handler := func(it jetstream.MessagesContext) {
			for {
				msg, err := it.Next()
				if err != nil {
					if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
						errs <- err
					}
					break
				}
				if err := msg.Ack(); err != nil {
					errs <- err
					break
				}
				count.Add(1)
			}
			done <- struct{}{}
		}

		it, err := c.Messages(jetstream.PullPriorityGroup("A"),
			jetstream.PullMinPending(110),
			jetstream.PullMaxMessages(20))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		go handler(it)

		time.Sleep(100 * time.Millisecond)
		// there are 100 messages on the stream, and min pending is 110
		// so we should not get any messages
		if count.Load() != 0 {
			t.Fatalf("Expected 0 messages, got %d", count.Load())
		}

		// Add more messages
		for i := 100; i < 200; i++ {
			_, err = js.Publish(ctx, "FOO.bar", []byte(fmt.Sprintf("hello %d", i)))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		time.Sleep(100 * time.Millisecond)
		// now we should get 91 messages, because `Consume` will
		// keep getting messages until it drops below min pending
		if count.Load() != 91 {
			t.Fatalf("Expected 10 messages, got %d", count.Load())
		}
		it.Stop()
		<-done

		// now test min ack pending
		count.Store(0)

		it, err = c.Messages(jetstream.PullPriorityGroup("A"),
			jetstream.PullMinAckPending(5),
			jetstream.PullMaxMessages(20))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		go handler(it)

		time.Sleep(100 * time.Millisecond)
		// no messages should be received, there are no pending acks
		if count.Load() != 0 {
			t.Fatalf("Expected 0 messages, got %d", count.Load())
		}
		// fetch some messages, do not ack them
		msgs, err := c.Fetch(10, jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		i := 0
		for range msgs.Messages() {
			i++
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error: %v", msgs.Error())
		}
		if i != 10 {
			t.Fatalf("Expected 10 messages, got %d", i)
		}

		time.Sleep(100 * time.Millisecond)
		// we should process the rest of the stream minus the 10 unacked messages
		// 200 - 91 - 10 = 99
		if count.Load() != 99 {
			t.Fatalf("Expected 5 messages, got %d", count.Load())
		}
		it.Stop()
		<-done
	})
}

func TestConsumerPinned(t *testing.T) {
	t.Run("messages", func(t *testing.T) {
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
			PinnedTTL:      time.Second,
			PriorityGroups: []string{"A"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for range 1000 {
			_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		gcount := make(chan struct{}, 1000)
		count := atomic.Uint32{}

		// Initially pinned consumer instance
		initiallyPinned, err := s.Consumer(ctx, "cons")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		handler := func(it jetstream.MessagesContext, counter *atomic.Uint32, doneCh chan struct{}) {
			for {
				msg, err := it.Next()
				if err != nil {
					break
				}
				if err := msg.Ack(); err != nil {
					break
				}
				counter.Add(1)
				gcount <- struct{}{}
			}
			doneCh <- struct{}{}
		}
		// test priority group validation
		// invalid priority group
		_, err = initiallyPinned.Messages(jetstream.PullPriorityGroup("BAD"))
		if err == nil || err.Error() != "nats: invalid jetstream option: invalid priority group" {
			t.Fatalf("Expected invalid priority group error, got %v", err)
		}

		// no priority group
		_, err = initiallyPinned.Messages()
		if err == nil || err.Error() != "nats: invalid jetstream option: priority group is required for priority consumer" {
			t.Fatalf("Expected invalid priority group error")
		}

		ipDoneCh := make(chan struct{})
		ip, err := initiallyPinned.Messages(jetstream.PullPriorityGroup("A"), jetstream.PullHeartbeat(500*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer ip.Stop()

		_, err = ip.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count.Store(1)
		go handler(ip, &count, ipDoneCh)

		time.Sleep(100 * time.Millisecond)

		// Second consume instance that should remain passive.
		notPinnedC := atomic.Uint32{}
		npDoneCh := make(chan struct{})
		np, err := c.Messages(jetstream.PullPriorityGroup("A"), jetstream.PullHeartbeat(500*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer np.Stop()
		go handler(np, &notPinnedC, npDoneCh)

		waitForCounter := func(t *testing.T, c *atomic.Uint32, expected int) {
			t.Helper()

		outer:
			for {
				select {
				case <-gcount:
					if c.Load() == uint32(expected) {
						break outer
					}
				case <-time.After(10 * time.Second):
					t.Fatalf("Did not get all messages in time; expected %d, got %d", expected, c.Load())
				}
			}
		}

		waitForCounter(t, &count, 1000)
		if notPinnedC.Load() != 0 {
			t.Fatalf("Expected 0 messages for not pinned, got %d", notPinnedC.Load())
		}

		count.Store(0)
		ip.Stop()
		for range 100 {
			_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		time.Sleep(100 * time.Millisecond)
		if notPinnedC.Load() != 0 {
			t.Fatalf("Expected 0 messages for not pinned, got %d", notPinnedC.Load())
		}

		//wait for pinned ttl to expire and messages to be consumed by the second consumer
		waitForCounter(t, &notPinnedC, 100)
		if count.Load() != 0 {
			t.Fatalf("Expected 0 messages for pinned, got %d", count.Load())
		}
		np.Stop()
		select {
		case <-ipDoneCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("Expected pinned consumer to be done")
		}
		select {
		case <-npDoneCh:
		case <-time.After(5 * time.Second):
			t.Fatalf("Expected not pinned consumer to be done")
		}
	})

	t.Run("consume", func(t *testing.T) {
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
			PinnedTTL:      1 * time.Second,
			PriorityGroups: []string{"A"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for range 1000 {
			_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Initially pinned consumer instance
		initiallyPinned, err := s.Consumer(ctx, "cons")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// test priority group validation
		// invalid priority group
		_, err = initiallyPinned.Consume(func(m jetstream.Msg) {
		}, jetstream.PullPriorityGroup("BAD"))
		if err == nil || err.Error() != "nats: invalid jetstream option: invalid priority group" {
			t.Fatalf("Expected invalid priority group error")
		}

		// no priority group
		_, err = initiallyPinned.Consume(func(m jetstream.Msg) {})
		if err == nil || err.Error() != "nats: invalid jetstream option: priority group is required for priority consumer" {
			t.Fatalf("Expected invalid priority group error")
		}

		pinnedCount := atomic.Uint32{}
		pinnedDone := make(chan struct{})
		ip, err := initiallyPinned.Consume(func(m jetstream.Msg) {
			if err := m.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if pinnedCount.Add(1) == 1000 {
				close(pinnedDone)
			}
		}, jetstream.PullThresholdMessages(10), jetstream.PullPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer ip.Stop()

		time.Sleep(100 * time.Millisecond)

		// Second consume instance that should remain passive.
		notPinnedCount := atomic.Uint32{}
		notPinnedDone := make(chan struct{})
		np, err := c.Consume(func(m jetstream.Msg) {
			if err := m.Ack(); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if notPinnedCount.Add(1) == 100 {
				close(notPinnedDone)
			}

		}, jetstream.PullPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer np.Stop()

		select {
		case <-pinnedDone:
		case <-time.After(10 * time.Second):
			t.Fatalf("Expected pinned consumer to be done")
		}
		if notPinnedCount.Load() != 0 {
			t.Fatalf("Expected 0 messages for not pinned, got %d", notPinnedCount.Load())
		}

		pinnedCount.Store(0)
		ip.Stop()
		for range 100 {
			_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		time.Sleep(100 * time.Millisecond)
		if notPinnedCount.Load() != 0 {
			t.Fatalf("Expected 0 messages for not pinned, got %d", notPinnedCount.Load())
		}

		//wait for pinned ttl to expire and messages to be consumed by the second consumer
		select {
		case <-notPinnedDone:
		case <-time.After(10 * time.Second):
			t.Fatalf("Expected not pinned consumer to be done after pinned ttl expired")
		}
		if pinnedCount.Load() != 0 {
			t.Fatalf("Expected 0 messages for pinned, got %d", pinnedCount.Load())
		}
	})

	t.Run("fetch", func(t *testing.T) {
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
			PinnedTTL:      time.Second,
			PriorityGroups: []string{"A"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Check that consumer got proper priority policy and TTL
		info := c.CachedInfo()
		if info.Config.PriorityPolicy != jetstream.PriorityPolicyPinned {
			t.Fatalf("Invalid priority policy; expected: %v; got: %v", jetstream.PriorityPolicyPinned, info.Config.PriorityPolicy)
		}
		if info.Config.PinnedTTL != time.Second {
			t.Fatalf("Invalid pinned TTL; expected: %v; got: %v", time.Second, info.Config.PinnedTTL)
		}

		for range 100 {
			_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		// Initial fetch.
		// Should get all messages and get a Pin ID.
		msgs, err := c.Fetch(10, jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count := 0
		id := ""
		for msg := range msgs.Messages() {
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

		// Different consumer instance.
		cdiff, err := js.Consumer(ctx, "foo", "cons")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err = cdiff.Fetch(10, jetstream.FetchMaxWait(200*time.Millisecond), jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		count = 0
		for msg := range msgs.Messages() {
			msg.Ack()
			count++
		}
		if count != 0 {
			t.Fatalf("Expected 0 messages, got %d", count)
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error: %v", msgs.Error())
		}

		count = 0

		// Now lets fetch from the pinned one, which should be fine.
		msgs, err = c.Fetch(10, jetstream.FetchMaxWait(3*time.Second), jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for msg := range msgs.Messages() {
			if pinId := msg.Headers().Get("Nats-Pin-Id"); pinId == "" {
				t.Fatalf("missing Nats-Pin-Id header")
			}
			msg.Ack()
			count++
		}
		if count != 10 {
			t.Fatalf("Expected 10 messages, got %d", count)
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error: %v", msgs.Error())
		}

		// Wait for the TTL to expire, expect different ID
		count = 0
		time.Sleep(1500 * time.Millisecond)
		// The same instance, should work fine.

		msgs, err = c.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond), jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for msg := range msgs.Messages() {
			msg.Ack()
			count++
		}
		if !errors.Is(msgs.Error(), jetstream.ErrPinIDMismatch) {
			t.Fatalf("Expected error: %v, got: %v", jetstream.ErrPinIDMismatch, msgs.Error())
		}
		if count != 0 {
			t.Fatalf("Expected 0 messages, got %d", count)
		}

		msgs, err = c.Fetch(10, jetstream.FetchMaxWait(500*time.Millisecond), jetstream.FetchPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for msg := range msgs.Messages() {
			if msg == nil {
				break
			}
			newId := msg.Headers().Get("Nats-Pin-Id")
			if newId == id {
				t.Fatalf("Expected new pull to have different ID. old: %s, new: %s", id, newId)
			}
			msg.Ack()
			count++
		}
		if msgs.Error() != nil {
			t.Fatalf("Unexpected error: %v", msgs.Error())
		}
		if count != 10 {
			t.Fatalf("Expected 10 messages, got %d", count)
		}
	})
}

func TestConsumerUnpin(t *testing.T) {
	t.Run("unpin consumer", func(t *testing.T) {
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
			PinnedTTL:      50 * time.Second,
			PriorityGroups: []string{"A"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for range 1000 {
			_, err = js.Publish(ctx, "FOO.bar", []byte("hello"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		msgs, err := c.Messages(jetstream.PullPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer msgs.Stop()

		msg, err := msgs.Next()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		firstPinID := msg.Headers().Get("Nats-Pin-Id")
		if firstPinID == "" {
			t.Fatalf("Expected pinned message")
		}

		second, err := s.Consumer(ctx, "cons")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		noMsgs, err := second.Messages(jetstream.PullPriorityGroup("A"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer noMsgs.Stop()

		done := make(chan struct{})
		errC := make(chan error)
		go func() {
			_, err := noMsgs.Next()
			if err != nil {
				errC <- err
				return
			}
			done <- struct{}{}
		}()

		select {
		case <-done:
			t.Fatalf("Expected no message")
		case <-time.After(500 * time.Millisecond):
			noMsgs.Stop()
		}
		select {
		case <-time.After(5 * time.Second):
			t.Fatalf("Expected error")
		case err := <-errC:
			if !errors.Is(err, jetstream.ErrMsgIteratorClosed) {
				t.Fatalf("Expected error: %v, got: %v", jetstream.ErrMsgIteratorClosed, err)
			}
		}

		third, err := s.Consumer(ctx, "cons")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		yesMsgs, err := third.Messages(jetstream.PullPriorityGroup("A"), jetstream.PullExpiry(time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer yesMsgs.Stop()

		go func() {
			msg, err := yesMsgs.Next()
			newPinID := msg.Headers().Get("Nats-Pin-Id")
			if newPinID == firstPinID || newPinID == "" {
				errC <- fmt.Errorf("Expected new pin ID, got %s", newPinID)
				return
			}
			if err != nil {
				errC <- err
				return
			}
			done <- struct{}{}
		}()

		err = s.UnpinConsumer(ctx, "cons", "A")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case <-done:
		case err := <-errC:
			t.Fatalf("Unexpected error: %v", err)
		case <-time.After(4 * time.Second):
			t.Fatalf("Should not time out")
		}
	})
	t.Run("consumer not found", func(t *testing.T) {
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

		// try unpinning consumer with invalid name
		err = s.UnpinConsumer(ctx, "cons", "A")
		if !errors.Is(err, jetstream.ErrConsumerNotFound) {
			t.Fatalf("Expected error: %v, got: %v", jetstream.ErrConsumerNotFound, err)
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
