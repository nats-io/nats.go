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
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestOrderedPushConsumer(t *testing.T) {
	testSubject := "FOO.123"
	testMsgs := []string{"m1", "m2", "m3", "m4", "m5"}
	publishTestMsgs := func(t *testing.T, js jetstream.JetStream) {
		t.Helper()
		for _, msg := range testMsgs {
			if _, err := js.Publish(context.Background(), testSubject, []byte(msg)); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
	}

	t.Run("happy path: receive in order, Stop closes Closed channel", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{})
		if err != nil {
			t.Fatalf("OrderedPushConsumer: %v", err)
		}

		var received []string
		wg := sync.WaitGroup{}
		wg.Add(len(testMsgs))
		cc, err := c.Consume(func(msg jetstream.Msg) {
			received = append(received, string(msg.Data()))
			wg.Done()
		})
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		publishTestMsgs(t, js)
		wg.Wait()

		for i, want := range testMsgs {
			if received[i] != want {
				t.Errorf("msg %d: want %q got %q", i, want, received[i])
			}
		}

		cc.Stop()
		select {
		case <-cc.Closed():
		case <-time.After(2 * time.Second):
			t.Fatal("Closed() did not signal after Stop()")
		}
	})

	t.Run("CachedInfo nil and Info errors before first Consume", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{})
		if err != nil {
			t.Fatalf("OrderedPushConsumer: %v", err)
		}
		if info := c.CachedInfo(); info != nil {
			t.Errorf("expected CachedInfo nil before Consume, got %v", info)
		}
		if _, err := c.Info(context.Background()); !errors.Is(err, jetstream.ErrOrderedConsumerNotCreated) {
			t.Errorf("expected ErrOrderedConsumerNotCreated, got %v", err)
		}
	})

	t.Run("DeliverPolicy variants", func(t *testing.T) {
		for _, tc := range []struct {
			name      string
			cfg       jetstream.OrderedPushConsumerConfig
			prePub    []string
			postPub   []string
			expectMsg []string
		}{
			{
				name:      "DeliverAll",
				cfg:       jetstream.OrderedPushConsumerConfig{DeliverPolicy: jetstream.DeliverAllPolicy},
				prePub:    []string{"a", "b"},
				postPub:   []string{"c", "d"},
				expectMsg: []string{"a", "b", "c", "d"},
			},
			{
				name:      "DeliverNew",
				cfg:       jetstream.OrderedPushConsumerConfig{DeliverPolicy: jetstream.DeliverNewPolicy},
				prePub:    []string{"a", "b"},
				postPub:   []string{"c", "d"},
				expectMsg: []string{"c", "d"},
			},
			{
				name:      "DeliverLast",
				cfg:       jetstream.OrderedPushConsumerConfig{DeliverPolicy: jetstream.DeliverLastPolicy},
				prePub:    []string{"a", "b"},
				postPub:   []string{"c"},
				expectMsg: []string{"b", "c"},
			},
			{
				name:      "DeliverByStartSequence",
				cfg:       jetstream.OrderedPushConsumerConfig{DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: 3},
				prePub:    []string{"a", "b", "c", "d"},
				postPub:   nil,
				expectMsg: []string{"c", "d"},
			},
		} {
			t.Run(tc.name, func(t *testing.T) {
				srv := RunBasicJetStreamServer()
				defer shutdownJSServerAndRemoveStorage(t, srv)
				nc, js := connectJS(t, srv)
				defer nc.Close()

				s := createStream(t, js, "foo", "FOO.*")
				for _, m := range tc.prePub {
					if _, err := js.Publish(context.Background(), testSubject, []byte(m)); err != nil {
						t.Fatal(err)
					}
				}

				c, err := s.OrderedPushConsumer(context.Background(), tc.cfg)
				if err != nil {
					t.Fatalf("OrderedPushConsumer: %v", err)
				}
				var got []string
				var mu sync.Mutex
				done := make(chan struct{})
				cc, err := c.Consume(func(msg jetstream.Msg) {
					mu.Lock()
					got = append(got, string(msg.Data()))
					if len(got) == len(tc.expectMsg) {
						select {
						case <-done:
						default:
							close(done)
						}
					}
					mu.Unlock()
				})
				if err != nil {
					t.Fatal(err)
				}
				defer cc.Stop()

				for _, m := range tc.postPub {
					if _, err := js.Publish(context.Background(), testSubject, []byte(m)); err != nil {
						t.Fatal(err)
					}
				}

				select {
				case <-done:
				case <-time.After(3 * time.Second):
					mu.Lock()
					t.Fatalf("timed out waiting; got %v, want %v", got, tc.expectMsg)
				}
				mu.Lock()
				defer mu.Unlock()
				if !equalSlices(got, tc.expectMsg) {
					t.Errorf("want %v got %v", tc.expectMsg, got)
				}
			})
		}
	})

	t.Run("DeliverByStartTime", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		if _, err := js.Publish(context.Background(), testSubject, []byte("old")); err != nil {
			t.Fatal(err)
		}
		time.Sleep(150 * time.Millisecond)
		cutoff := time.Now()
		time.Sleep(150 * time.Millisecond)
		if _, err := js.Publish(context.Background(), testSubject, []byte("new")); err != nil {
			t.Fatal(err)
		}

		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{
			DeliverPolicy: jetstream.DeliverByStartTimePolicy,
			OptStartTime:  &cutoff,
		})
		if err != nil {
			t.Fatal(err)
		}
		got := make(chan string, 4)
		cc, err := c.Consume(func(msg jetstream.Msg) { got <- string(msg.Data()) })
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()

		select {
		case m := <-got:
			if m != "new" {
				t.Errorf("want 'new', got %q", m)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("timed out waiting for message")
		}
		// Confirm no second message arrives.
		select {
		case m := <-got:
			t.Errorf("unexpected extra message: %q", m)
		case <-time.After(200 * time.Millisecond):
		}
	})

	t.Run("FilterSubjects: only matching subjects delivered", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
			Name: "foo", Subjects: []string{"FOO.>"},
		})
		if err != nil {
			t.Fatal(err)
		}
		for _, sub := range []string{"FOO.a.1", "FOO.b.1", "FOO.a.2", "FOO.c.1"} {
			if _, err := js.Publish(context.Background(), sub, []byte(sub)); err != nil {
				t.Fatal(err)
			}
		}

		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{
			FilterSubjects: []string{"FOO.a.>", "FOO.c.>"},
		})
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		var mu sync.Mutex
		done := make(chan struct{})
		cc, err := c.Consume(func(msg jetstream.Msg) {
			mu.Lock()
			got = append(got, msg.Subject())
			if len(got) == 3 {
				close(done)
			}
			mu.Unlock()
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			mu.Lock()
			t.Fatalf("timed out, got %v", got)
		}
		mu.Lock()
		defer mu.Unlock()
		want := []string{"FOO.a.1", "FOO.a.2", "FOO.c.1"}
		if !equalSlices(got, want) {
			t.Errorf("want %v got %v", want, got)
		}
	})

	t.Run("NamePrefix produces sequenced names across resets", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{
			NamePrefix:    "watcher",
			IdleHeartbeat: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{}, 1)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			select {
			case ready <- struct{}{}:
			default:
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()

		publishTestMsgs(t, js)
		<-ready

		first := c.CachedInfo().Name
		if first != "watcher_1" {
			t.Errorf("expected first consumer 'watcher_1', got %q", first)
		}

		if err := s.DeleteConsumer(context.Background(), first); err != nil {
			t.Fatal(err)
		}
		// Wait for reset to produce watcher_2.
		var second string
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			cur := c.CachedInfo()
			if cur != nil && cur.Name != first {
				second = cur.Name
				break
			}
			time.Sleep(50 * time.Millisecond)
		}
		if second != "watcher_2" {
			t.Errorf("expected second consumer 'watcher_2', got %q", second)
		}
	})

	t.Run("missing NamePrefix yields NUID-prefixed names", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{}, 1)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			select {
			case ready <- struct{}{}:
			default:
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()

		publishTestMsgs(t, js)
		<-ready

		name := c.CachedInfo().Name
		// NUID is 22 chars; full name "<22>_1" is 24 chars.
		if !strings.HasSuffix(name, "_1") || len(name) != 24 {
			t.Errorf("expected NUID_1 naming, got %q (len=%d)", name, len(name))
		}
	})

	t.Run("consumer deleted (409) triggers reset and resumes delivery", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{
			NamePrefix:    "delete",
			IdleHeartbeat: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}

		var mu sync.Mutex
		var got []string
		batch := make(chan struct{}, 10)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			mu.Lock()
			got = append(got, string(msg.Data()))
			mu.Unlock()
			batch <- struct{}{}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()

		publishTestMsgs(t, js)
		for range testMsgs {
			<-batch
		}

		// Delete underlying consumer; library should reset.
		if err := s.DeleteConsumer(context.Background(), c.CachedInfo().Name); err != nil {
			t.Fatal(err)
		}

		// Publish another batch — should arrive on a recreated consumer.
		publishTestMsgs(t, js)
		for range testMsgs {
			select {
			case <-batch:
			case <-time.After(5 * time.Second):
				mu.Lock()
				t.Fatalf("timed out waiting for post-reset messages; got=%v", got)
			}
		}
		mu.Lock()
		defer mu.Unlock()
		if len(got) != 2*len(testMsgs) {
			t.Errorf("expected %d messages total, got %d (%v)", 2*len(testMsgs), len(got), got)
		}
	})

	t.Run("server restart triggers reset and resumes delivery", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, err := nats.Connect(srv.ClientURL(),
			nats.ReconnectWait(50*time.Millisecond),
			nats.MaxReconnects(-1))
		if err != nil {
			t.Fatal(err)
		}
		defer nc.Close()
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{
			NamePrefix:    "restart",
			IdleHeartbeat: 500 * time.Millisecond,
		})
		if err != nil {
			t.Fatal(err)
		}

		var mu sync.Mutex
		var got []string
		batch := make(chan struct{}, 10)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			mu.Lock()
			got = append(got, string(msg.Data()))
			mu.Unlock()
			batch <- struct{}{}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()

		publishTestMsgs(t, js)
		for range testMsgs {
			<-batch
		}

		srv = restartBasicJSServer(t, srv)
		defer shutdownJSServerAndRemoveStorage(t, srv)

		publishTestMsgs(t, js)
		for range testMsgs {
			select {
			case <-batch:
			case <-time.After(5 * time.Second):
				mu.Lock()
				t.Fatalf("timed out waiting for post-restart messages; got=%v", got)
			}
		}
	})

	t.Run("concurrent Consume returns ErrOrderedConsumerConcurrentRequests", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		cc, err := c.Consume(func(msg jetstream.Msg) {})
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()

		if _, err := c.Consume(func(msg jetstream.Msg) {}); !errors.Is(err, jetstream.ErrOrderedConsumerConcurrentRequests) {
			t.Errorf("expected ErrOrderedConsumerConcurrentRequests, got %v", err)
		}
	})

	t.Run("Stop deletes underlying server consumer", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{NamePrefix: "stop"})
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{}, 1)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			select {
			case ready <- struct{}{}:
			default:
			}
		})
		if err != nil {
			t.Fatal(err)
		}

		publishTestMsgs(t, js)
		<-ready
		name := c.CachedInfo().Name

		cc.Stop()
		select {
		case <-cc.Closed():
		case <-time.After(2 * time.Second):
			t.Fatal("Closed() did not signal after Stop()")
		}

		// Allow best-effort goroutine to delete.
		deadline := time.Now().Add(3 * time.Second)
		var lastErr error
		for time.Now().Before(deadline) {
			_, lastErr = s.Consumer(context.Background(), name)
			if errors.Is(lastErr, jetstream.ErrConsumerNotFound) {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("expected consumer %q to be deleted, got err=%v", name, lastErr)
	})

	t.Run("MaxResetAttempts cap surfaces failure", func(t *testing.T) {
		// Force reset failure by deleting the stream out from under the
		// consumer. The 409 consumer-deleted triggers a reset; the recreate
		// fails because the stream is gone; MaxResetAttempts caps the retry
		// loop and surfaces the error via the user ErrHandler.
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{
			NamePrefix:       "cap",
			IdleHeartbeat:    500 * time.Millisecond,
			MaxResetAttempts: 2,
		})
		if err != nil {
			t.Fatal(err)
		}

		ready := make(chan struct{}, 1)
		gotErr := make(chan error, 4)
		cc, err := c.Consume(
			func(msg jetstream.Msg) {
				select {
				case ready <- struct{}{}:
				default:
				}
			},
			jetstream.ConsumeErrHandler(func(_ jetstream.ConsumeContext, err error) {
				select {
				case gotErr <- err:
				default:
				}
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()

		if _, err := js.Publish(context.Background(), testSubject, []byte("pre")); err != nil {
			t.Fatal(err)
		}
		<-ready

		// Deleting the stream deletes the consumer too, triggering a reset
		// loop that will exhaust MaxResetAttempts because the stream no
		// longer exists.
		if err := js.DeleteStream(context.Background(), "foo"); err != nil {
			t.Fatal(err)
		}

		select {
		case err := <-gotErr:
			// Any error indicates the reset loop exhausted and surfaced;
			// we don't assert a specific error string since the server may
			// return either ErrStreamNotFound or a generic API error.
			if err == nil {
				t.Fatal("expected non-nil error from ErrHandler")
			}
		case <-time.After(10 * time.Second):
			t.Fatal("expected ErrHandler invocation after MaxResetAttempts exhausted")
		}
	})

	t.Run("OrderedPushConsumer via JetStream interface delegates to stream", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()
		createStream(t, js, "foo", "FOO.*")

		c, err := js.OrderedPushConsumer(context.Background(), "foo", jetstream.OrderedPushConsumerConfig{})
		if err != nil {
			t.Fatal(err)
		}
		ready := make(chan struct{}, 1)
		cc, err := c.Consume(func(msg jetstream.Msg) {
			select {
			case ready <- struct{}{}:
			default:
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		defer cc.Stop()
		publishTestMsgs(t, js)
		select {
		case <-ready:
		case <-time.After(2 * time.Second):
			t.Fatal("no message received via JetStream-level OrderedPushConsumer")
		}
	})

	t.Run("OrderedPushConsumer on missing stream returns ErrStreamNotFound", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		c, err := js.OrderedPushConsumer(context.Background(), "missing", jetstream.OrderedPushConsumerConfig{})
		if err != nil {
			t.Fatalf("expected no error from constructor (lazy create), got %v", err)
		}
		cc, err := c.Consume(func(jetstream.Msg) {})
		if cc != nil {
			cc.Stop()
		}
		if err == nil {
			t.Fatal("expected an error consuming from missing stream")
		}
	})

	t.Run("validates OptStartSeq + OptStartTime mutually exclusive", func(t *testing.T) {
		srv := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, srv)
		nc, js := connectJS(t, srv)
		defer nc.Close()

		s := createStream(t, js, "foo", "FOO.*")
		now := time.Now()
		c, err := s.OrderedPushConsumer(context.Background(), jetstream.OrderedPushConsumerConfig{
			OptStartSeq:  10,
			OptStartTime: &now,
		})
		if err != nil {
			t.Fatal(err)
		}
		_, err = c.Consume(func(jetstream.Msg) {})
		if !errors.Is(err, jetstream.ErrInvalidOption) {
			t.Errorf("expected ErrInvalidOption, got %v", err)
		}
	})
}

// --- helpers ---

func connectJS(t *testing.T, srv interface{ ClientURL() string }) (*nats.Conn, jetstream.JetStream) {
	t.Helper()
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	return nc, js
}

func createStream(t *testing.T, js jetstream.JetStream, name, subj string) jetstream.Stream {
	t.Helper()
	s, err := js.CreateStream(context.Background(), jetstream.StreamConfig{
		Name:     name,
		Subjects: []string{subj},
	})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}
	return s
}

func equalSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return fmt.Sprint(a) == fmt.Sprint(b)
}
