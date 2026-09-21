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
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestPublishAsyncPendingWaitExit(t *testing.T) {
	for _, reason := range []string{"stall timeout", "ack timeout", "cleanup", "connection close"} {
		t.Run(reason, func(t *testing.T) {
			withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
				opts := []jetstream.JetStreamOpt{jetstream.WithPublishAsyncMaxPending(1)}
				if reason == "ack timeout" {
					opts = append(opts, jetstream.WithPublishAsyncTimeout(200*time.Millisecond))
				}
				js, err := jetstream.New(nc, opts...)
				if err != nil {
					t.Fatal(err)
				}
				defer js.CleanupPublisher()
				sub, err := nc.SubscribeSync("pending.exit")
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				if err := nc.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
				first, err := js.PublishAsync("pending.exit", nil)
				if err != nil {
					t.Fatal(err)
				}
				msg, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatal(err)
				}
				wait := 2 * time.Second
				if reason == "stall timeout" {
					wait = 100 * time.Millisecond
				}
				result := make(chan error, 1)
				var wg sync.WaitGroup
				wg.Add(1)
				defer wg.Wait()
				go func() {
					defer wg.Done()
					_, err := js.PublishAsync("pending.exit", nil, jetstream.WithStallWait(wait))
					result <- err
				}()
				if _, err := sub.NextMsg(50 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("expected stalled publisher, got %v", err)
				}
				var want error
				switch reason {
				case "stall timeout":
					want = jetstream.ErrTooManyStalledMsgs
				case "ack timeout":
					select {
					case err := <-first.Err():
						if !errors.Is(err, jetstream.ErrAsyncPublishTimeout) {
							t.Fatal(err)
						}
					case <-time.After(time.Second):
						t.Fatal("ACK timeout did not resolve first publish")
					}
				case "cleanup":
					js.CleanupPublisher()
					want = jetstream.ErrJetStreamPublisherClosed
				case "connection close":
					nc.Close()
					want = nats.ErrConnectionClosed
				}
				select {
				case err := <-result:
					if !errors.Is(err, want) {
						t.Fatalf("expected %v, got %v", want, err)
					}
				case <-time.After(time.Second):
					t.Fatal("publisher did not leave capacity wait")
				}
				if reason == "connection close" {
					return
				}
				if reason == "stall timeout" {
					if n := js.PublishAsyncPending(); n != 1 {
						t.Fatalf("timed out waiter changed pending count: %d", n)
					}
				} else {
					if reason == "cleanup" {
						if _, err := js.PublishAsync("pending.exit", nil); err != nil {
							t.Fatalf("publish after cleanup: %v", err)
						}
					}
					msg, err = sub.NextMsg(time.Second)
					if err != nil {
						t.Fatal(err)
					}
				}
				if err := msg.Respond([]byte(`{"stream":"TEST","seq":1}`)); err != nil {
					t.Fatal(err)
				}
				select {
				case <-js.PublishAsyncComplete():
				case <-time.After(time.Second):
					t.Fatal("pending publishes did not complete")
				}
			})
		})
	}
}

func TestPublishAsyncConcurrentPendingRecovery(t *testing.T) {
	for _, maxPending := range []int{1, 5} {
		t.Run(fmt.Sprintf("max_pending_%d", maxPending), func(t *testing.T) {
			withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
				js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(maxPending))
				if err != nil {
					t.Fatal(err)
				}
				defer js.CleanupPublisher()
				// A core subscriber lets the test control when publish ACKs arrive.
				sub, err := nc.SubscribeSync("pending.test")
				if err != nil {
					t.Fatal(err)
				}
				defer sub.Unsubscribe()
				if err := nc.FlushTimeout(time.Second); err != nil {
					t.Fatal(err)
				}
				var initial []*nats.Msg
				for i := 0; i < maxPending; i++ {
					if _, err := js.PublishAsync("pending.test", []byte("initial")); err != nil {
						t.Fatal(err)
					}
					msg, err := sub.NextMsg(time.Second)
					if err != nil {
						t.Fatal(err)
					}
					initial = append(initial, msg)
				}
				const publishers = 16
				var wg sync.WaitGroup
				defer wg.Wait()
				started := make(chan struct{}, publishers)
				results := make(chan error, publishers)
				for i := 0; i < publishers; i++ {
					wg.Add(1)
					go func() {
						defer wg.Done()
						started <- struct{}{}
						_, err := js.PublishAsync("pending.test", []byte("waiting"), jetstream.WithStallWait(2*time.Second))
						results <- err
					}()
				}
				for i := 0; i < publishers; i++ {
					<-started
				}
				// Nothing may be sent until an existing publish frees capacity.
				if _, err := sub.NextMsg(100 * time.Millisecond); err != nats.ErrTimeout {
					t.Fatalf("expected publishers to stall, got %v", err)
				}
				if pending := js.PublishAsyncPending(); pending > maxPending {
					t.Errorf("pending publishes exceeded limit: %d > %d", pending, maxPending)
				}
				ack := []byte(`{"stream":"TEST","seq":1}`)
				for _, msg := range initial {
					if err := msg.Respond(ack); err != nil {
						t.Fatal(err)
					}
				}
				// ACK recovery must allow progress before any stall timeout expires.
				for i := 0; i < publishers; i++ {
					msg, err := sub.NextMsg(500 * time.Millisecond)
					if err != nil {
						t.Fatalf("publisher did not recover after ACKs: %v", err)
					}
					if pending := js.PublishAsyncPending(); pending > maxPending {
						t.Fatalf("pending publishes exceeded limit: %d > %d", pending, maxPending)
					}
					if err := msg.Respond(ack); err != nil {
						t.Fatal(err)
					}
				}
				for i := 0; i < publishers; i++ {
					if err := <-results; err != nil {
						t.Fatalf("publish failed: %v", err)
					}
				}
				select {
				case <-js.PublishAsyncComplete():
				case <-time.After(time.Second):
					t.Fatal("pending publishes did not complete")
				}
			})
		})
	}
}

func TestPublishAsyncPendingSendFailure(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn, _ jetstream.JetStream) {
		js, err := jetstream.New(nc, jetstream.WithPublishAsyncMaxPending(1), jetstream.WithPublishAsyncTimeout(time.Second))
		if err != nil {
			t.Fatal(err)
		}
		defer js.CleanupPublisher()
		sub, err := nc.SubscribeSync("pending.failure")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if _, err := js.PublishAsync("pending.failure", make([]byte, nc.MaxPayload()+1)); !errors.Is(err, nats.ErrMaxPayload) {
			t.Fatalf("expected max payload error, got %v", err)
		}
		if n := js.PublishAsyncPending(); n != 0 {
			t.Fatalf("failed publish retained pending slot: %d", n)
		}
		select {
		case <-js.PublishAsyncComplete():
		default:
			t.Fatal("failed publish prevented completion")
		}
		future, err := js.PublishAsync("pending.failure", nil)
		if err != nil {
			t.Fatalf("publish after send failure: %v", err)
		}
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if err := msg.Respond([]byte(`{"stream":"TEST","seq":1}`)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-future.Ok():
		case err := <-future.Err():
			t.Fatal(err)
		case <-time.After(2 * time.Second):
			t.Fatal("publish after send failure did not complete")
		}
	})
}
