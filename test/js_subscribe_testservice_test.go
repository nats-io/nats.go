// Copyright 2020-2026 The NATS Authors
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
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
	"github.com/nats-io/nuid"
)

func TestJetStreamSubscribe_RateLimit(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		totalMsgs := 2048
		for range totalMsgs {
			payload := strings.Repeat("A", 1024)
			js.Publish("foo", []byte(payload))
		}

		// By default there is no RateLimit
		isub, err := js.SubscribeSync("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ci, err := isub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.Config.RateLimit != 0 {
			t.Fatalf("Expected no rate limit, got: %v", ci.Config.RateLimit)
		}

		recvd := make(chan *nats.Msg, totalMsgs)
		duration := 2 * time.Second
		ctx, cancel := context.WithTimeout(context.Background(), duration)
		defer cancel()

		var rl uint64 = 1024
		sub, err := js.Subscribe("foo", func(m *nats.Msg) {
			recvd <- m

			if len(recvd) == totalMsgs {
				cancel()
			}
		}, nats.RateLimit(rl))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ci, err = sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci.Config.RateLimit != rl {
			t.Fatalf("Expected %v, got: %v", rl, ci.Config.RateLimit)
		}
		<-ctx.Done()

		if len(recvd) >= int(rl) {
			t.Errorf("Expected applied rate limit to push consumer, got %v msgs in %v", recvd, duration)
		}
	})
}

func TestJetStreamSubscribe_FilterSubjects(t *testing.T) {
	tests := []struct {
		name    string
		durable string
	}{
		{
			name: "ephemeral consumer",
		},
		{
			name:    "durable consumer",
			durable: "cons",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			withJSServer(t, func(t *testing.T, nc *nats.Conn) {
				js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				_, err = js.AddStream(&nats.StreamConfig{
					Name:     "TEST",
					Subjects: []string{"foo", "bar", "baz"},
				})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				for range 5 {
					js.Publish("foo", []byte("msg"))
				}
				for range 5 {
					js.Publish("bar", []byte("msg"))
				}
				for range 5 {
					js.Publish("baz", []byte("msg"))
				}

				opts := []nats.SubOpt{nats.BindStream("TEST"), nats.ConsumerFilterSubjects("foo", "baz")}
				if test.durable != "" {
					opts = append(opts, nats.Durable(test.durable))
				}
				sub, err := js.SubscribeSync("", opts...)
				if err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}

				for range 10 {
					msg, err := sub.NextMsg(500 * time.Millisecond)
					if err != nil {
						t.Fatalf("Unexpected error: %s", err)
					}
					if msg.Subject != "foo" && msg.Subject != "baz" {
						t.Fatalf("Unexpected message subject: %s", msg.Subject)
					}
				}
			})
		})
	}
}

func TestJetStreamSubscribe_ConfigCantChange(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for _, test := range []struct {
			name   string
			first  nats.SubOpt
			second nats.SubOpt
		}{
			{"description", nats.Description("a"), nats.Description("b")},
			{"deliver policy", nats.DeliverAll(), nats.DeliverLast()},
			{"optional start sequence", nats.StartSequence(1), nats.StartSequence(10)},
			{"optional start time", nats.StartTime(time.Now()), nats.StartTime(time.Now().Add(-2 * time.Hour))},
			{"ack wait", nats.AckWait(10 * time.Second), nats.AckWait(15 * time.Second)},
			{"max deliver", nats.MaxDeliver(3), nats.MaxDeliver(5)},
			{"replay policy", nats.ReplayOriginal(), nats.ReplayInstant()},
			{"max waiting", nats.PullMaxWaiting(10), nats.PullMaxWaiting(20)},
			{"max ack pending", nats.MaxAckPending(10), nats.MaxAckPending(20)},
		} {
			t.Run(test.name, func(t *testing.T) {
				durName := nuid.Next()
				sub, err := js.PullSubscribe("foo", durName, test.first)
				if err != nil {
					t.Fatalf("Error on subscribe: %v", err)
				}
				// Once it is created, options can't be changed.
				_, err = js.PullSubscribe("foo", durName, test.second)
				if err == nil || !strings.Contains(err.Error(), test.name) {
					t.Fatalf("Unexpected error: %v", err)
				}
				sub.Unsubscribe()
			})
		}

		for _, test := range []struct {
			name string
			cc   *nats.ConsumerConfig
			opt  nats.SubOpt
		}{
			{"ack policy", &nats.ConsumerConfig{AckPolicy: nats.AckAllPolicy}, nats.AckNone()},
			{"rate limit", &nats.ConsumerConfig{RateLimit: 10}, nats.RateLimit(100)},
			{"flow control", &nats.ConsumerConfig{FlowControl: false}, nats.EnableFlowControl()},
			{"heartbeat", &nats.ConsumerConfig{Heartbeat: 10 * time.Second}, nats.IdleHeartbeat(20 * time.Second)},
		} {
			t.Run(test.name, func(t *testing.T) {
				durName := nuid.Next()

				cc := test.cc
				cc.Durable = durName
				cc.DeliverSubject = nuid.Next()
				if _, err := js.AddConsumer("TEST", cc); err != nil {
					t.Fatalf("Error creating consumer: %v", err)
				}

				sub, err := js.SubscribeSync("foo", nats.Durable(durName), test.opt)
				if err == nil || !strings.Contains(err.Error(), test.name) {
					t.Fatalf("Unexpected error: %v", err)
				}
				sub.Unsubscribe()
			})
		}

		// Verify that we don't fail if user did not set it.
		for _, test := range []struct {
			name string
			opt  nats.SubOpt
		}{
			{"description", nats.Description("a")},
			{"deliver policy", nats.DeliverAll()},
			{"optional start sequence", nats.StartSequence(10)},
			{"optional start time", nats.StartTime(time.Now())},
			{"ack wait", nats.AckWait(10 * time.Second)},
			{"max deliver", nats.MaxDeliver(3)},
			{"replay policy", nats.ReplayOriginal()},
			{"max waiting", nats.PullMaxWaiting(10)},
			{"max ack pending", nats.MaxAckPending(10)},
		} {
			t.Run(test.name+" not set", func(t *testing.T) {
				durName := nuid.Next()
				sub, err := js.PullSubscribe("foo", durName, test.opt)
				if err != nil {
					t.Fatalf("Error on subscribe: %v", err)
				}
				// If not explicitly asked by the user, we are ok
				_, err = js.PullSubscribe("foo", durName)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sub.Unsubscribe()
			})
		}

		for _, test := range []struct {
			name string
			opt  nats.SubOpt
		}{
			{"default deliver policy", nats.DeliverAll()},
			{"default ack wait", nats.AckWait(30 * time.Second)},
			{"default replay policy", nats.ReplayInstant()},
			{"default max waiting", nats.PullMaxWaiting(512)},
			{"default ack pending", nats.MaxAckPending(65536)},
		} {
			t.Run(test.name, func(t *testing.T) {
				durName := nuid.Next()
				sub, err := js.PullSubscribe("foo", durName)
				if err != nil {
					t.Fatalf("Error on subscribe: %v", err)
				}
				// If the option is the same as the server default, it is not an error either.
				_, err = js.PullSubscribe("foo", durName, test.opt)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				sub.Unsubscribe()
			})
		}

		for _, test := range []struct {
			name string
			opt  nats.SubOpt
		}{
			{"policy", nats.DeliverNew()},
			{"ack wait", nats.AckWait(31 * time.Second)},
			{"replay policy", nats.ReplayOriginal()},
			{"max waiting", nats.PullMaxWaiting(513)},
			{"ack pending", nats.MaxAckPending(2)},
		} {
			t.Run(test.name+" changed from default", func(t *testing.T) {
				durName := nuid.Next()
				sub, err := js.PullSubscribe("foo", durName)
				if err != nil {
					t.Fatalf("Error on subscribe: %v", err)
				}
				// First time it was created with defaults and the
				// second time a change is attempted, so it is an error.
				_, err = js.PullSubscribe("foo", durName, test.opt)
				if err == nil || !strings.Contains(err.Error(), test.name) {
					t.Fatalf("Unexpected error: %v", err)
				}
				sub.Unsubscribe()
			})
		}

		if _, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:        "BindDurable",
			DeliverSubject: "bar",
		}); err != nil {
			t.Fatalf("Failed to create consumer: %v", err)
		}
		if _, err := js.SubscribeSync("foo", nats.Bind("TEST", "BindDurable")); err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
	})
}

func TestJetStreamStreamMirror(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Original uses Placement: { Tags: ["NODE_0"] } but the cluster
		// helper only assigns NODE_x tags when size > 1. Single-server
		// withJSServer in the original has no tags either; drop placement
		// here so the stream creation succeeds on the testservice-managed
		// single server.
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "origin",
			Storage:  nats.MemoryStorage,
			Replicas: 1,
		})
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		totalMsgs := 10
		for i := range totalMsgs {
			payload := fmt.Sprintf("i:%d", i)
			js.Publish("origin", []byte(payload))
		}

		t.Run("create mirrors", func(t *testing.T) {
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "m1",
				Mirror:   &nats.StreamSource{Name: "origin"},
				Storage:  nats.FileStorage,
				Replicas: 1,
			})
			if err != nil {
				t.Fatalf("Unexpected error creating stream: %v", err)
			}

			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "m2",
				Mirror:   &nats.StreamSource{Name: "origin"},
				Storage:  nats.MemoryStorage,
				Replicas: 1,
			})
			if err != nil {
				t.Fatalf("Unexpected error creating stream: %v", err)
			}
			msgs := make([]*nats.RawStreamMsg, 0)

			startSequence := 1

		GetNextMsg:
			for i := startSequence; i < totalMsgs+1; i++ {
				var (
					err       error
					seq       = uint64(i)
					msgA      *nats.RawStreamMsg
					msgB      *nats.RawStreamMsg
					sourceMsg *nats.RawStreamMsg
					timeout   = time.Now().Add(2 * time.Second)
				)

				for time.Now().Before(timeout) {
					sourceMsg, err = js.GetMsg("origin", seq)
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					msgA, err = js.GetMsg("m1", seq)
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if !reflect.DeepEqual(sourceMsg, msgA) {
						t.Errorf("Expected %+v, got: %+v", sourceMsg, msgA)
					}

					msgB, err = js.GetMsg("m2", seq)
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue
					}
					if !reflect.DeepEqual(sourceMsg, msgB) {
						t.Errorf("Expected %+v, got: %+v", sourceMsg, msgB)
					}

					msgs = append(msgs, msgA)
					continue GetNextMsg
				}
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}

			got := len(msgs)
			if got < totalMsgs {
				t.Errorf("Expected %v, got: %v", totalMsgs, got)
			}

			t.Run("consume from mirror", func(t *testing.T) {
				sub, err := js.SubscribeSync("origin", nats.BindStream("m1"))
				if err != nil {
					t.Fatal(err)
				}

				mmsgs := make([]*nats.Msg, 0)
				for range totalMsgs {
					msg, err := sub.NextMsg(2 * time.Second)
					if err != nil {
						t.Error(err)
					}
					meta, err := msg.Metadata()
					if err != nil {
						t.Error(err)
					}
					if meta.Stream != "m1" {
						t.Errorf("Expected m1, got: %v", meta.Stream)
					}
					mmsgs = append(mmsgs, msg)
				}
				if len(mmsgs) != totalMsgs {
					t.Errorf("Expected to consume %v msgs, got: %v", totalMsgs, len(mmsgs))
				}
			})
		})

		t.Run("consume from original source", func(t *testing.T) {
			sub, err := js.SubscribeSync("origin")
			defer sub.Unsubscribe()
			if err != nil {
				t.Fatal(err)
			}
			msg, err := sub.NextMsg(2 * time.Second)
			if err != nil {
				t.Error(err)
			}
			meta, err := msg.Metadata()
			if err != nil {
				t.Error(err)
			}
			if meta.Stream != "origin" {
				t.Errorf("Expected m1, got: %v", meta.Stream)
			}
		})

		t.Run("bind to non existing stream fails", func(t *testing.T) {
			_, err := js.SubscribeSync("origin", nats.BindStream("foo"))
			if err == nil {
				t.Fatal("Unexpected success")
			}
			if !errors.Is(err, nats.ErrStreamNotFound) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrStreamNotFound, err)
			}
		})

		t.Run("bind to origin stream", func(t *testing.T) {
			sub, err := js.SubscribeSync("origin", nats.BindStream("origin"))
			if err != nil {
				t.Fatal(err)
			}
			msg, err := sub.NextMsg(2 * time.Second)
			if err != nil {
				t.Error(err)
			}
			meta, err := msg.Metadata()
			if err != nil {
				t.Error(err)
			}
			if meta.Stream != "origin" {
				t.Errorf("Expected m1, got: %v", meta.Stream)
			}
		})

		t.Run("get mirror info", func(t *testing.T) {
			m1, err := js.StreamInfo("m1")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			got := m1.Mirror.Name
			expected := "origin"
			if got != expected {
				t.Errorf("Expected %v, got: %v", expected, got)
			}

			m2, err := js.StreamInfo("m2")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			got = m2.Mirror.Name
			expected = "origin"
			if got != expected {
				t.Errorf("Expected %v, got: %v", expected, got)
			}
		})

		t.Run("create stream from sources", func(t *testing.T) {
			sources := make([]*nats.StreamSource, 0)
			sources = append(sources, &nats.StreamSource{Name: "m1"})
			sources = append(sources, &nats.StreamSource{Name: "m2"})
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "s1",
				Sources:  sources,
				Storage:  nats.FileStorage,
				Replicas: 1,
			})
			if err != nil {
				t.Fatalf("Unexpected error creating stream: %v", err)
			}

			msgs := make([]*nats.RawStreamMsg, 0)

			startSequence := 1
			expectedTotal := totalMsgs * 2

		GetNextMsg:
			for i := startSequence; i < expectedTotal+1; i++ {
				var (
					err     error
					seq     = uint64(i)
					msg     *nats.RawStreamMsg
					timeout = time.Now().Add(5 * time.Second)
				)

			Retry:
				for time.Now().Before(timeout) {
					msg, err = js.GetMsg("s1", seq)
					if err != nil {
						time.Sleep(100 * time.Millisecond)
						continue Retry
					}
					msgs = append(msgs, msg)
					continue GetNextMsg
				}
				if err != nil {
					t.Fatalf("Unexpected error fetching seq=%v: %v", seq, err)
				}
			}

			got := len(msgs)
			if got < expectedTotal {
				t.Errorf("Expected %v, got: %v", expectedTotal, got)
			}

			si, err := js.StreamInfo("s1")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			got = int(si.State.Msgs)
			if got != expectedTotal {
				t.Errorf("Expected %v, got: %v", expectedTotal, got)
			}

			got = len(si.Sources)
			expected := 2
			if got != expected {
				t.Errorf("Expected %v, got: %v", expected, got)
			}

			t.Run("consume from sourced stream", func(t *testing.T) {
				sub, err := js.SubscribeSync("origin", nats.BindStream("s1"))
				if err != nil {
					t.Error(err)
				}
				_, err = sub.NextMsg(2 * time.Second)
				if err != nil {
					t.Error(err)
				}
			})
		})

		t.Run("update stream with sources", func(t *testing.T) {
			si, err := js.StreamInfo("s1")
			if err != nil {
				t.Fatalf("Unexpected error creating stream: %v", err)
			}
			got := len(si.Config.Sources)
			expected := 2
			if got != expected {
				t.Errorf("Expected %v, got: %v", expected, got)
			}

			got = len(si.Sources)
			if got != expected {
				t.Errorf("Expected %v, got: %v", expected, got)
			}

			config := si.Config
			config.MaxMsgs = 128
			updated, err := js.UpdateStream(&config)
			if err != nil {
				t.Fatalf("Unexpected error creating stream: %v", err)
			}

			got = len(updated.Config.Sources)
			if got != expected {
				t.Errorf("Expected %v, got: %v", expected, got)
			}

			got = int(updated.Config.MaxMsgs)
			expected = int(config.MaxMsgs)
			if got != expected {
				t.Errorf("Expected %v, got: %v", expected, got)
			}
		})

		t.Run("bind to stream with subject not in stream", func(t *testing.T) {
			sub, err := js.SubscribeSync("nothing", nats.BindStream("origin"))
			if err != nil {
				t.Fatal(err)
			}
			_, err = sub.NextMsg(1 * time.Second)
			if !errors.Is(err, nats.ErrTimeout) {
				t.Fatal("Expected timeout error")
			}

			info, err := sub.ConsumerInfo()
			if err != nil {
				t.Fatal(err)
			}
			got := info.Stream
			expected := "origin"
			if got != expected {
				t.Fatalf("Expected %v, got %v", expected, got)
			}

			got = info.Config.FilterSubject
			expected = "nothing"
			if got != expected {
				t.Fatalf("Expected %v, got %v", expected, got)
			}

			t.Run("can consume after stream update", func(t *testing.T) {
				_, err = js.UpdateStream(&nats.StreamConfig{
					Name:     "origin",
					Storage:  nats.MemoryStorage,
					Replicas: 1,
					Subjects: []string{"origin", "nothing"},
				})
				js.Publish("nothing", []byte("hello world"))

				msg, err := sub.NextMsg(1 * time.Second)
				if err != nil {
					t.Error(err)
				}
				got = msg.Subject
				expected = "nothing"
				if got != expected {
					t.Fatalf("Expected %v, got %v", expected, got)
				}
			})
		})

		t.Run("create sourced stream with a cycle", func(t *testing.T) {
			sources := make([]*nats.StreamSource, 0)
			sources = append(sources, &nats.StreamSource{Name: "origin"})
			sources = append(sources, &nats.StreamSource{Name: "m1"})
			streamName := "s2"
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     streamName,
				Sources:  sources,
				Storage:  nats.FileStorage,
				Replicas: 1,
			})
			var aerr *nats.APIError
			if ok := errors.As(err, &aerr); !ok || aerr.ErrorCode != nats.JSStreamInvalidConfig {
				t.Fatalf("Expected nats.APIError, got %v", err)
			}
		})
	})
}

func TestJetStream_ClusterMultipleSubscribe(t *testing.T) {
	nodes := []int{1, 3}
	replicas := []int{1}

	for _, n := range nodes {
		for _, r := range replicas {
			if r > 1 && n == 1 {
				continue
			}

			t.Run(fmt.Sprintf("qsub n=%d r=%d", n, r), func(t *testing.T) {
				name := fmt.Sprintf("MSUB%d%d", n, r)
				stream := &nats.StreamConfig{
					Name:     name,
					Replicas: r,
				}
				withClusterAndStream(t, n, stream, testJetStreamClusterMultipleQueueSubscribeTS)
			})

			t.Run(fmt.Sprintf("psub n=%d r=%d", n, r), func(t *testing.T) {
				name := fmt.Sprintf("PSUBN%d%d", n, r)
				stream := &nats.StreamConfig{
					Name:     name,
					Replicas: n,
				}
				withClusterAndStream(t, n, stream, testJetStreamClusterMultiplePullSubscribeTS)
			})

			t.Run(fmt.Sprintf("psub n=%d r=%d multi fetch", n, r), func(t *testing.T) {
				name := fmt.Sprintf("PFSUBN%d%d", n, r)
				stream := &nats.StreamConfig{
					Name:     name,
					Replicas: n,
				}
				withClusterAndStream(t, n, stream, testJetStreamClusterMultipleFetchPullSubscribeTS)
			})
		}
	}
}

// withClusterAndStream spins up a single JS server when size == 1 or a JS
// cluster when size > 1, then creates the supplied stream and invokes fn.
// Sized for the testservice-based TestJetStream_ClusterMultipleSubscribe.
func withClusterAndStream(t *testing.T, size int, stream *nats.StreamConfig, fn func(t *testing.T, subject string, nc *nats.Conn)) {
	t.Helper()
	c := newTester(t)
	var inst *testservice.Instance
	if size == 1 {
		inst = c.CreateServer(t, true, clientAdvertiseOpt(t))
	} else {
		inst = c.CreateCluster(t, size, true, clientAdvertiseOpt(t))
	}
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	c.WaitForJetStream(t, nc)

	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Retry stream creation while cluster settles (mirrors withJSClusterAndStream).
	deadline := time.Now().Add(10 * time.Second)
	for {
		_, err = js.AddStream(stream)
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}

	fn(t, stream.Name, nc)
}

func testJetStreamClusterMultipleQueueSubscribeTS(t *testing.T, subject string, nc *nats.Conn) {
	var wg sync.WaitGroup
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	size := 5
	subs := make([]*nats.Subscription, size)
	errCh := make(chan error, size)

	sub, err := js.QueueSubscribeSync(subject, "wq", nats.Durable("shared"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	subs[0] = sub

	for i := 1; i < size; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var sub *nats.Subscription
			var err error
			for range 5 {
				sub, err = js.QueueSubscribeSync(subject, "wq", nats.Durable("shared"))
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				break
			}
			if err != nil {
				errCh <- err
			} else {
				subs[n] = sub
			}
		}(i)
	}

	go func() {
		wg.Wait()
		done()
	}()

	wg.Wait()
	for range size * 2 {
		js.Publish(subject, []byte("test"))
	}

	delivered := 0
	for _, sub := range subs {
		if sub == nil {
			continue
		}
		if nmsgs, _, _ := sub.Pending(); err != nil || nmsgs > 0 {
			delivered++
		}
	}
	if delivered < 2 {
		t.Fatalf("Expected more than one subscriber to receive a message, got: %v", delivered)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Unexpected error with multiple queue subscribers: %v", err)
		}
	}
}

func testJetStreamClusterMultiplePullSubscribeTS(t *testing.T, subject string, nc *nats.Conn) {
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()

	size := 5
	subs := make([]*nats.Subscription, size)
	errCh := make(chan error, size)
	for i := range size {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var sub *nats.Subscription
			var err error
			for range 5 {
				sub, err = js.PullSubscribe(subject, "shared")
				if err != nil {
					time.Sleep(1 * time.Second)
					continue
				}
				break
			}
			if err != nil {
				errCh <- err
			} else {
				subs[n] = sub
			}
		}(i)
	}

	go func() {
		wg.Wait()
		done()
	}()

	wg.Wait()
	for range size * 2 {
		js.Publish(subject, []byte("test"))
	}

	delivered := 0
	for i, sub := range subs {
		if sub == nil {
			continue
		}
		for range 4 {
			_, err := sub.Fetch(1, nats.MaxWait(250*time.Millisecond))
			if err != nil {
				t.Logf("%v WARN: Timeout waiting for next message: %v", i, err)
				continue
			}
			delivered++
			break
		}
	}

	if delivered < 2 {
		t.Fatalf("Expected more than one subscriber to receive a message, got: %v", delivered)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Unexpected error with multiple pull subscribers: %v", err)
		}
	}
}

func testJetStreamClusterMultipleFetchPullSubscribeTS(t *testing.T, subject string, nc *nats.Conn) {
	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	var wg sync.WaitGroup
	ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
	defer done()

	nsubs := 4
	subs := make([]*nats.Subscription, nsubs)
	errCh := make(chan error, nsubs)
	var queues sync.Map
	for i := range nsubs {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			sub, err := js.PullSubscribe(subject, "shared")
			if err != nil {
				errCh <- err
			} else {
				subs[n] = sub
				queues.Store(sub.Subject, make([]*nats.Msg, 0))
			}
		}(i)
	}

	wg.Wait()
	var (
		total     uint64 = 100
		delivered uint64
		batchSize = 2
	)
	go func() {
		for i := range int(total) {
			js.Publish(subject, []byte(fmt.Sprintf("n:%v", i)))
			time.Sleep(1 * time.Millisecond)
		}
	}()

	ctx2, done2 := context.WithTimeout(ctx, 3*time.Second)
	defer done2()

	for _, psub := range subs {
		if psub == nil {
			continue
		}
		sub := psub
		subject := sub.Subject
		v, _ := queues.Load(sub.Subject)
		queue := v.([]*nats.Msg)
		go func() {
			for {
				select {
				case <-ctx2.Done():
					return
				default:
				}

				if current := atomic.LoadUint64(&delivered); current >= total {
					done2()
					return
				}

				for range 4 {
					recvd, err := sub.Fetch(batchSize, nats.MaxWait(1*time.Second))
					if err != nil {
						if err == nats.ErrConnectionClosed {
							return
						}
						current := atomic.LoadUint64(&delivered)
						if current >= total {
							done2()
							return
						} else {
							t.Logf("WARN: Timeout waiting for next message: %v", err)
						}
						continue
					}
					for _, msg := range recvd {
						queue = append(queue, msg)
						queues.Store(subject, queue)
					}
					atomic.AddUint64(&delivered, uint64(len(recvd)))
					break
				}
			}
		}()
	}

	<-ctx2.Done()

	if delivered < total {
		t.Fatalf("Expected %v, got: %v", total, delivered)
	}

	select {
	case <-ctx.Done():
	case err := <-errCh:
		if err != nil {
			t.Fatalf("Unexpected error with multiple pull subscribers: %v", err)
		}
	}

	var (
		gotNoMessages bool
		count         = 0
	)
	queues.Range(func(k, v any) bool {
		msgs := v.([]*nats.Msg)
		count += len(msgs)

		if len(msgs) == 0 {
			gotNoMessages = true
			return false
		}
		return true
	})

	if gotNoMessages {
		t.Error("Expected all pull subscribers to receive some messages")
	}
}

func TestJetStream_ClusterReconnect(t *testing.T) {
	// Preserved INHERITED skip from the original test (js_test.go:7323).
	t.Skip("This test need to be revisited")
}

func TestJetStreamPullSubscribeOptions(t *testing.T) {
	c := newTester(t)
	inst := c.CreateCluster(t, 3, true, clientAdvertiseOpt(t))
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	c.WaitForJetStream(t, nc)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subject := "WQ"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     subject,
		Replicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	sendMsgs := func(t *testing.T, totalMsgs int) {
		t.Helper()
		for i := range totalMsgs {
			payload := fmt.Sprintf("i:%d", i)
			if _, err := js.Publish(subject, []byte(payload)); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		}
	}

	t.Run("max request batch", func(t *testing.T) {
		defer js.PurgeStream(subject)

		sub, err := js.PullSubscribe(subject, "max-request-batch", nats.MaxRequestBatch(2))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if _, err := sub.Fetch(10); err == nil || !strings.Contains(err.Error(), "MaxRequestBatch of 2") {
			t.Fatalf("Expected error about max request batch size, got %v", err)
		}
	})

	t.Run("max request max bytes", func(t *testing.T) {
		defer js.PurgeStream(subject)

		sub, err := js.PullSubscribe(subject, "max-request-max-bytes", nats.MaxRequestMaxBytes(100))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		if _, err := sub.Fetch(10, nats.PullMaxBytes(200)); err == nil || !strings.Contains(err.Error(), "MaxRequestMaxBytes of 100") {
			t.Fatalf("Expected error about max request max bytes, got %v", err)
		}
	})

	t.Run("max request expires", func(t *testing.T) {
		defer js.PurgeStream(subject)

		sub, err := js.PullSubscribe(subject, "max-request-expires", nats.MaxRequestExpires(50*time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()
		if _, err := sub.Fetch(10); err == nil || !strings.Contains(err.Error(), "MaxRequestExpires of 50ms") {
			t.Fatalf("Expected error about max request expiration, got %v", err)
		}
	})

	t.Run("batch size", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)
		sub, err := js.PullSubscribe(subject, "batch-size")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		msgs, err := sub.Fetch(expected, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatal(err)
		}

		for _, msg := range msgs {
			msg.AckSync()
		}

		got := len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}

		_, err = sub.Fetch(1, nats.MaxWait(250*time.Millisecond))
		if err != nats.ErrTimeout {
			t.Errorf("Expected timeout fetching next message, got: %v", err)
		}

		expected = 5
		sendMsgs(t, expected)
		msgs, err = sub.Fetch(expected, nats.MaxWait(1*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got = len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}

		for _, msg := range msgs {
			msg.Ack()
		}
	})

	t.Run("sub drain is no op", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)
		sub, err := js.PullSubscribe(subject, "batch-ctx")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		msgs, err := sub.Fetch(expected, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got := len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}
		if err := sub.Drain(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("fetch after unsubscribe", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)
		sub, err := js.PullSubscribe(subject, "fetch-unsub")
		if err != nil {
			t.Fatal(err)
		}

		if err := sub.Unsubscribe(); err != nil {
			t.Fatal(err)
		}

		_, err = sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if !errors.Is(err, nats.ErrBadSubscription) {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("max waiting exceeded", func(t *testing.T) {
		defer js.PurgeStream(subject)

		_, err := js.AddConsumer(subject, &nats.ConsumerConfig{
			Durable:    "max-waiting",
			MaxWaiting: 2,
			AckPolicy:  nats.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatal(err)
		}

		var wg sync.WaitGroup
		wg.Add(2)
		for range 2 {
			go func() {
				defer wg.Done()

				sub, err := js.PullSubscribe(subject, "max-waiting")
				if err != nil {
					return
				}
				sub.Fetch(1, nats.MaxWait(time.Second))
			}()
		}

		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			ci, err := js.ConsumerInfo(subject, "max-waiting")
			if err != nil {
				return err
			}
			if n := ci.NumWaiting; n != 2 {
				return fmt.Errorf("NumWaiting should be 2, was %v", n)
			}
			return nil
		})

		sub, err := js.PullSubscribe(subject, "max-waiting")
		if err != nil {
			t.Fatal(err)
		}
		_, err = sub.Fetch(1, nats.MaxWait(time.Second))
		if err == nil || !strings.Contains(err.Error(), "MaxWaiting") {
			t.Fatalf("Unexpected error: %v", err)
		}
		wg.Wait()
	})

	t.Run("no wait", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)
		sub, err := js.PullSubscribe(subject, "no-wait")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		ctx, done := context.WithTimeout(context.Background(), 5*time.Second)
		defer done()
		recvd := make([]*nats.Msg, 0)

	Loop:
		for range time.NewTicker(100 * time.Millisecond).C {
			select {
			case <-ctx.Done():
				break Loop
			default:
			}

			msgs, err := sub.Fetch(1, nats.MaxWait(250*time.Millisecond))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			recvd = append(recvd, msgs[0])

			for _, msg := range msgs {
				if err := msg.AckSync(); err != nil {
					t.Error(err)
				}
			}

			if len(recvd) == expected {
				done()
				break
			}
		}

		got := len(recvd)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}

		msgs, err := sub.Fetch(expected, nats.MaxWait(2*time.Second))
		if err == nil {
			t.Fatal("Unexpected success", len(msgs))
		}
		if err != nats.ErrTimeout {
			t.Fatalf("Expected timeout error, got: %v", err)
		}
	})
}

func TestJetStreamPublishAsync(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		paf, err := js.PublishAsync("foo", []byte("Hello JS"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case <-paf.Ok():
			t.Fatalf("Did not expect to get PubAck")
		case err := <-paf.Err():
			if err != nats.ErrNoResponders {
				t.Fatalf("Expected a ErrNoResponders error, got %v", err)
			}
			m := paf.Msg()
			if m == nil {
				t.Fatalf("Expected to be able to retrieve the message")
			}
			if m.Subject != "foo" || string(m.Data) != "Hello JS" {
				t.Fatalf("Wrong message: %+v", m)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an error in time")
		}

		if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST"}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		paf, err = js.PublishAsync("TEST", []byte("Hello JS ASYNC PUB"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case pa := <-paf.Ok():
			if pa.Stream != "TEST" || pa.Sequence != 1 {
				t.Fatalf("Bad PubAck: %+v", pa)
			}
		case err := <-paf.Err():
			t.Fatalf("Did not expect to get an error: %v", err)
		case <-time.After(time.Second):
			t.Fatalf("Did not receive a PubAck in time")
		}

		errCh := make(chan error, 1)

		errHandler := func(js nats.JetStream, originalMsg *nats.Msg, err error) {
			if originalMsg == nil {
				t.Fatalf("Expected non-nil original message")
			}
			errCh <- err
		}

		js, err = nc.JetStream(nats.PublishAsyncErrHandler(errHandler))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case err := <-errCh:
			if err != nats.ErrNoResponders {
				t.Fatalf("Expected a ErrNoResponders error, got %v", err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an async err in time")
		}

		js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for range 100 {
			if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if np := js.PublishAsyncPending(); np > 10 {
				t.Fatalf("Expected num pending to not exceed 10, got %d", np)
			}
		}

		js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for range 500 {
			if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		_, err = js.PublishAsync("foo", []byte("Bad"), nats.StallWait(0))
		expectedErr := "nats: stall wait should be more than 0"
		if err == nil || err.Error() != expectedErr {
			t.Errorf("Expected %v, got: %v", expectedErr, err)
		}

		_, err = js.Publish("foo", []byte("Also bad"), nats.StallWait(200*time.Millisecond))
		expectedErr = "nats: stall wait cannot be set to sync publish"
		if err == nil || err.Error() != expectedErr {
			t.Errorf("Expected %v, got: %v", expectedErr, err)
		}
	})

	// CustomInboxPrefix variant in its own scope so the connection is opened
	// with the custom prefix from the start.
	withJSServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc := dialInstance(t, inst, nats.CustomInboxPrefix("_BOX"))
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		paf, err := js.PublishAsync("foo", []byte("Hello JS with Custom Inbox"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case <-paf.Ok():
			t.Fatalf("Did not expect to get PubAck")
		case err := <-paf.Err():
			if err != nats.ErrNoResponders {
				t.Fatalf("Expected a ErrNoResponders error, got %v", err)
			}
			m := paf.Msg()
			if m == nil {
				t.Fatalf("Expected to be able to retrieve the message")
			}
			if m.Subject != "foo" || string(m.Data) != "Hello JS with Custom Inbox" {
				t.Fatalf("Wrong message: %+v", m)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an error in time")
		}
	})
}

func TestPublishAsyncResetPendingOnReconnect(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"FOO"}}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		errs := make(chan error, 1)
		doneCh := make(chan struct{}, 1)
		acks := make(chan nats.PubAckFuture, 100)
		go func() {
			for range 100 {
				if ack, err := js.PublishAsync("FOO", []byte("hello")); err != nil {
					errs <- err
					return
				} else {
					acks <- ack
				}
			}
			close(acks)
			doneCh <- struct{}{}
		}()
		select {
		case <-doneCh:
		case err := <-errs:
			t.Fatalf("Unexpected error during publish: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}
		inst.StopServer(t, inst.Servers[0])
		time.Sleep(100 * time.Millisecond)
		if pending := js.PublishAsyncPending(); pending != 0 {
			t.Fatalf("Expected no pending messages after server shutdown; got: %d", pending)
		}
		inst.StartServer(t, inst.Servers[0])

		for ack := range acks {
			select {
			case <-ack.Ok():
			case err := <-ack.Err():
				if !errors.Is(err, nats.ErrDisconnected) && !errors.Is(err, nats.ErrNoResponders) {
					t.Fatalf("Expected error: %v or %v; got: %v", nats.ErrDisconnected, nats.ErrNoResponders, err)
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}
		}
	})
}

func TestPublishAsyncAckTimeout(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		errs := make(chan error, 1)
		js, err := nc.JetStream(
			nats.PublishAsyncTimeout(50*time.Millisecond),
			nats.PublishAsyncErrHandler(func(js nats.JetStream, m *nats.Msg, e error) {
				errs <- e
			}),
		)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, NoAck: true})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ack, err := js.PublishAsync("FOO.A", []byte("hello"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		select {
		case <-ack.Ok():
			t.Fatalf("Expected timeout")
		case err := <-ack.Err():
			if !errors.Is(err, nats.ErrAsyncPublishTimeout) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrAsyncPublishTimeout, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive ack timeout")
		}

		select {
		case err := <-errs:
			if !errors.Is(err, nats.ErrAsyncPublishTimeout) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrAsyncPublishTimeout, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive error from error handler")
		}

		if js.PublishAsyncPending() != 0 {
			t.Fatalf("Expected no pending messages")
		}

		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(100 * time.Millisecond):
			t.Fatalf("Did not receive completion signal")
		}
	})
}

func TestPublishAsyncClearStall(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(
			nats.PublishAsyncTimeout(500*time.Millisecond),
			nats.PublishAsyncMaxPending(100))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, NoAck: true})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for range 100 {
			_, err := js.PublishAsync("FOO.A", []byte("hello"), nats.StallWait(1*time.Nanosecond))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		_, err = js.PublishAsync("FOO.A", []byte("hello"), nats.StallWait(50*time.Millisecond))
		if !errors.Is(err, nats.ErrTooManyStalledMsgs) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrTooManyStalledMsgs, err)
		}

		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		if _, err = js.PublishAsync("FOO.A", []byte("hello")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if js.PublishAsyncPending() != 1 {
			t.Fatalf("Expected 1 pending message; got: %d", js.PublishAsyncPending())
		}
	})
}

func TestPublishAsyncRetryInErrHandler(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		streamCreated := make(chan struct{})
		errCB := func(js nats.JetStream, m *nats.Msg, e error) {
			<-streamCreated
			_, err := js.PublishMsgAsync(m)
			if err != nil {
				t.Fatalf("Unexpected error when republishing: %v", err)
			}
		}

		js, err := nc.JetStream(nats.PublishAsyncErrHandler(errCB))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		errs := make(chan error, 1)
		doneCh := make(chan struct{}, 1)
		go func() {
			for range 10 {
				if _, err := js.PublishAsync("FOO.A", []byte("hello")); err != nil {
					errs <- err
					return
				}
			}
			doneCh <- struct{}{}
		}()
		select {
		case <-doneCh:
		case err := <-errs:
			t.Fatalf("Unexpected error during publish: %v", err)
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}
		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		close(streamCreated)
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		info, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if info.State.Msgs != 10 {
			t.Fatalf("Expected 10 messages in the stream; got: %d", info.State.Msgs)
		}
	})
}

func TestJetStreamPublishAsyncPerf(t *testing.T) {
	// Preserved INHERITED skip from the original test (js_test.go:8427).
	t.SkipNow()

	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		// 64 byte payload.
		msg := make([]byte, 64)
		rand.Read(msg)

		var asyncErrs uint32
		errHandler := func(js nats.JetStream, originalMsg *nats.Msg, err error) {
			t.Logf("Got an async err: %v", err)
			atomic.AddUint32(&asyncErrs, 1)
		}

		js, err := nc.JetStream(
			nats.PublishAsyncErrHandler(errHandler),
			nats.PublishAsyncMaxPending(256),
		)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.AddStream(&nats.StreamConfig{Name: "B"}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		toSend := 1000000
		start := time.Now()
		for range toSend {
			if _, err = js.PublishAsync("B", msg); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		select {
		case <-js.PublishAsyncComplete():
			if ne := atomic.LoadUint32(&asyncErrs); ne > 0 {
				t.Fatalf("Got unexpected errors publishing")
			}
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		tt := time.Since(start)
		fmt.Printf("Took %v to send %d msgs\n", tt, toSend)
		fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
	})
}

func TestPublishAsyncRetry(t *testing.T) {
	tests := []struct {
		name     string
		pubOpts  []nats.PubOpt
		ackError error
		pubErr   error
	}{
		{
			name: "retry until stream is ready",
			pubOpts: []nats.PubOpt{
				nats.RetryAttempts(10),
				nats.RetryWait(100 * time.Millisecond),
			},
		},
		{
			name: "fail after max retries",
			pubOpts: []nats.PubOpt{
				nats.RetryAttempts(2),
				nats.RetryWait(50 * time.Millisecond),
			},
			ackError: nats.ErrNoResponders,
		},
		{
			name:     "no retries",
			pubOpts:  nil,
			ackError: nats.ErrNoResponders,
		},
		{
			name: "invalid retry attempts",
			pubOpts: []nats.PubOpt{
				nats.RetryAttempts(-1),
			},
			pubErr: nats.ErrInvalidArg,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			withJSServer(t, func(t *testing.T, nc *nats.Conn) {
				js, err := nc.JetStream(nats.PublishAsyncMaxPending(1))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				test.pubOpts = append(test.pubOpts, nats.StallWait(1*time.Nanosecond))
				ack, err := js.PublishAsync("foo", []byte("hello"), test.pubOpts...)
				if !errors.Is(err, test.pubErr) {
					t.Fatalf("Expected error: %v; got: %v", test.pubErr, err)
				}
				if err != nil {
					return
				}
				errs := make(chan error, 1)
				go func() {
					time.Sleep(300 * time.Millisecond)
					if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"foo"}}); err != nil {
						errs <- err
					}
				}()
				select {
				case <-ack.Ok():
				case err := <-ack.Err():
					if test.ackError != nil {
						if !errors.Is(err, test.ackError) {
							t.Fatalf("Expected error: %v; got: %v", test.ackError, err)
						}
					} else {
						t.Fatalf("Unexpected ack error: %v", err)
					}
				case err := <-errs:
					t.Fatalf("Error creating stream: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatalf("Timeout waiting for ack")
				}
			})
		})
	}
}

func TestJetStreamCleanupPublisher(t *testing.T) {
	t.Run("cleanup js publisher", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, nc *nats.Conn) {
			js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"FOO"}}); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			numSubs := nc.NumSubscriptions()
			if _, err := js.PublishAsync("FOO", []byte("hello")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			select {
			case <-js.PublishAsyncComplete():
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			if numSubs+1 != nc.NumSubscriptions() {
				t.Fatalf("Expected an additional subscription after publish, got %d", nc.NumSubscriptions())
			}

			js.CleanupPublisher()

			if numSubs != nc.NumSubscriptions() {
				t.Fatalf("Expected subscriptions to be back to original count")
			}
		})
	})

	t.Run("cleanup js publisher, cancel pending acks", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, nc *nats.Conn) {
			cbErr := make(chan error, 10)
			js, err := nc.JetStream(nats.PublishAsyncErrHandler(func(js nats.JetStream, m *nats.Msg, err error) {
				cbErr <- err
			}))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if _, err := js.AddStream(&nats.StreamConfig{Name: "TEST", Subjects: []string{"FOO"}, NoAck: true}); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			numSubs := nc.NumSubscriptions()

			var acks []nats.PubAckFuture
			for range 10 {
				ack, err := js.PublishAsync("FOO", []byte("hello"))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				acks = append(acks, ack)
			}

			asyncComplete := js.PublishAsyncComplete()
			select {
			case <-asyncComplete:
				t.Fatalf("Should not complete, NoAck is set")
			case <-time.After(200 * time.Millisecond):
			}

			if numSubs+1 != nc.NumSubscriptions() {
				t.Fatalf("Expected an additional subscription after publish, got %d", nc.NumSubscriptions())
			}

			js.CleanupPublisher()

			if numSubs != nc.NumSubscriptions() {
				t.Fatalf("Expected subscriptions to be back to original count")
			}

			select {
			case <-asyncComplete:
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive completion signal")
			}

			for _, ack := range acks {
				select {
				case err := <-ack.Err():
					if !errors.Is(err, nats.ErrJetStreamPublisherClosed) {
						t.Fatalf("Expected JetStreamContextClosed error, got %v", err)
					}
				case <-ack.Ok():
					t.Fatalf("Expected error on the ack future")
				case <-time.After(200 * time.Millisecond):
					t.Fatalf("Expected an error on the ack future")
				}
			}

			for range 10 {
				select {
				case err := <-cbErr:
					if !errors.Is(err, nats.ErrJetStreamPublisherClosed) {
						t.Fatalf("Expected JetStreamContextClosed error, got %v", err)
					}
				case <-time.After(200 * time.Millisecond):
					t.Fatalf("Expected errors to be passed from the async handler")
				}
			}
		})
	})
}

func TestJetStreamPublishExpectZero(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test", "foo", "bar"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub, err := nc.SubscribeSync("foo")
		if err != nil {
			t.Errorf("Error: %s", err)
		}

		_, err = js.Publish("foo", []byte("bar"),
			nats.ExpectLastSequence(0),
			nats.ExpectLastSequencePerSubject(0),
		)
		if err != nil {
			t.Errorf("Error: %v", err)
		}

		rawMsg, err := js.GetMsg("TEST", 1)
		if err != nil {
			t.Fatalf("Error: %s", err)
		}
		hdr, ok := rawMsg.Header["Nats-Expected-Last-Sequence"]
		if !ok {
			t.Fatal("Missing header")
		}
		got := hdr[0]
		expected := "0"
		if got != expected {
			t.Fatalf("Expected %v, got: %v", expected, got)
		}
		hdr, ok = rawMsg.Header["Nats-Expected-Last-Subject-Sequence"]
		if !ok {
			t.Fatal("Missing header")
		}
		got = hdr[0]
		expected = "0"
		if got != expected {
			t.Fatalf("Expected %v, got: %v", expected, got)
		}

		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			t.Fatalf("Error: %s", err)
		}
		hdr, ok = msg.Header["Nats-Expected-Last-Sequence"]
		if !ok {
			t.Fatal("Missing header")
		}
		got = hdr[0]
		expected = "0"
		if got != expected {
			t.Fatalf("Expected %v, got: %v", expected, got)
		}
		hdr, ok = msg.Header["Nats-Expected-Last-Subject-Sequence"]
		if !ok {
			t.Fatal("Missing header")
		}
		got = hdr[0]
		expected = "0"
		if got != expected {
			t.Fatalf("Expected %v, got: %v", expected, got)
		}
	})
}

func TestJetStreamBindConsumer(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.AddStream(nil); err == nil {
			t.Fatalf("Unexpected success")
		}
		si, err := js.AddStream(&nats.StreamConfig{Name: "foo"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}

		for range 25 {
			js.Publish("foo", []byte("hi"))
		}

		_, err = js.SubscribeSync("foo", nats.Bind("", ""))
		if err != nats.ErrStreamNameRequired {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.SubscribeSync("foo", nats.Bind("foo", ""))
		if err != nats.ErrConsumerNameRequired {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.SubscribeSync("foo", nats.Bind("foo", "push"))
		if err == nil || !errors.Is(err, nats.ErrConsumerNotFound) {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.PullSubscribe("foo", "pull", nats.Bind("foo", "pull"))
		if err == nil || !errors.Is(err, nats.ErrConsumerNotFound) {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:        "push",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverSubject: nats.NewInbox(),
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub, err := js.SubscribeSync("foo", nats.Bind("foo", "push"))
		if err != nil {
			t.Fatal(err)
		}
		_, err = js.SubscribeSync("foo", nats.Durable("push2"), nats.Bind("foo", "push"))
		if err == nil || !strings.Contains(err.Error(), `nats: duplicate consumer names (push2 and push)`) {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.SubscribeSync("foo", nats.BindStream("foo"), nats.Bind("foo2", "push"))
		if err == nil || !strings.Contains(err.Error(), `nats: duplicate stream name (foo and foo2)`) {
			t.Fatalf("Unexpected error: %v", err)
		}
		sub.Unsubscribe()

		checkConsInactive := func() {
			t.Helper()
			checkFor(t, time.Second, 15*time.Millisecond, func() error {
				ci, _ := js.ConsumerInfo("foo", "push")
				if ci != nil && !ci.PushBound {
					return nil
				}
				return fmt.Errorf("Consumer %q still active", "push")
			})
		}
		checkConsInactive()

		sub, err = js.SubscribeSync("foo", nats.BindStream("foo"), nats.Bind("foo", "push"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.SubscribeSync("foo", nats.Durable("push"))
		if err == nil || !strings.Contains(err.Error(), "already bound") {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.QueueSubscribeSync("foo", "wq", nats.Durable("push"))
		if err == nil || !strings.Contains(err.Error(), "without a deliver group") {
			t.Fatalf("Unexpected error: %v", err)
		}
		sub.Unsubscribe()
		checkConsInactive()

		_, err = js.QueueSubscribeSync("foo", "wq1", nats.Durable("qpush"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.SubscribeSync("foo", nats.Durable("qpush"))
		if err == nil || !strings.Contains(err.Error(), "cannot create a subscription for a consumer with a deliver group") {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.QueueSubscribeSync("foo", "wq2", nats.Durable("qpush"))
		if err == nil || !strings.Contains(err.Error(), "cannot create a queue subscription") {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:   "pull",
			AckPolicy: nats.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.PullSubscribe("foo", "pull", nats.Bind("foo", "pull"))
		if err != nil {
			t.Fatal(err)
		}

		_, err = js.PullSubscribe("foo", "push", nats.Bind("foo", "push"))
		if err != nats.ErrPullSubscribeToPushConsumer {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.SubscribeSync("foo", nats.Bind("foo", "pull"))
		if err != nats.ErrPullSubscribeRequired {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub1, err := js.SubscribeSync("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cinfo, err := sub1.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}

		_, err = js.SubscribeSync("foo", nats.Bind("foo", cinfo.Name))
		if err == nil || !strings.Contains(err.Error(), "already bound") {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub2, err := js.QueueSubscribeSync("foo", "wq3")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for range 25 {
			msg, err := sub2.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Error on NextMsg: %v", err)
			}
			msg.AckSync()
		}
		cinfo, _ = sub2.ConsumerInfo()
		sub3, err := js.QueueSubscribeSync("foo", "wq3", nats.Bind("foo", cinfo.Name))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for range 100 {
			js.Publish("foo", []byte("new"))
		}
		if _, err := sub3.NextMsg(time.Second); err != nil {
			t.Fatalf("Second member failed to get a message: %v", err)
		}
	})
}

func TestJetStreamDomain(t *testing.T) {
	t.Skip("DIVERGENCE: original sets root `jetstream: { domain: ABC }`; testservice's default template already renders a `jetstream:` block, so an additional WithTopLevel('jetstream: { domain: ABC }') produces two coexisting blocks and the domain is lost (same blocker noted on TestKeyValueMirrorCrossDomains and TestAccountInfo 'server with limits set'). Recommended resolution: upstream a `WithJetStreamDomain(string)` option in testservice that injects `domain:` into the rendered jetstream block.")
}

func TestJetStreamMaxMsgsPerSubject(t *testing.T) {
	const subjectMax = 5
	msc := nats.StreamConfig{
		Name:              "TEST",
		Subjects:          []string{"foo", "bar", "baz.*"},
		Storage:           nats.MemoryStorage,
		MaxMsgsPerSubject: subjectMax,
	}
	fsc := msc
	fsc.Storage = nats.FileStorage

	cases := []struct {
		name    string
		mconfig *nats.StreamConfig
	}{
		{"MemoryStore", &msc},
		{"FileStore", &fsc},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			withJSServer(t, func(t *testing.T, nc *nats.Conn) {
				js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				_, err = js.AddStream(c.mconfig)
				if err != nil {
					t.Fatalf("Unexpected error adding stream: %v", err)
				}
				defer js.DeleteStream(c.mconfig.Name)

				pubAndCheck := func(subj string, num int, expectedNumMsgs uint64) {
					t.Helper()
					for range num {
						if _, err = js.Publish(subj, []byte("TSLA")); err != nil {
							t.Fatalf("Unexpected publish error: %v", err)
						}
					}
					si, err := js.StreamInfo(c.mconfig.Name)
					if err != nil {
						t.Fatalf("Unexpected error: %v", err)
					}
					if si.State.Msgs != expectedNumMsgs {
						t.Fatalf("Expected %d msgs, got %d", expectedNumMsgs, si.State.Msgs)
					}
				}

				pubAndCheck("foo", 1, 1)
				pubAndCheck("foo", 4, 5)
				pubAndCheck("foo", 2, 5)
				pubAndCheck("baz.22", 5, 10)
				pubAndCheck("baz.33", 5, 15)
				pubAndCheck("baz.22", 5, 15)
				pubAndCheck("baz.33", 5, 15)

				if err := js.PurgeStream(c.mconfig.Name); err != nil {
					t.Fatalf("Unexpected purge error: %v", err)
				}
				pubAndCheck("foo", 1, 1)
				pubAndCheck("foo", 4, 5)
				pubAndCheck("baz.22", 5, 10)
				pubAndCheck("baz.33", 5, 15)
			})
		})
	}
}

func TestJetStreamDrainFailsToDeleteConsumer(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, true)
	t.Cleanup(func() { inst.Destroy(t) })

	errCh := make(chan error, 1)
	nc := dialInstance(t, inst, nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case errCh <- err:
		default:
		}
	}))
	c.WaitForJetStream(t, nc)

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("hi"))

	blockCh := make(chan struct{})
	sub, err := js.Subscribe("foo", func(m *nats.Msg) {
		<-blockCh
	}, nats.Durable("dur"))
	if err != nil {
		t.Fatalf("Error subscribing: %v", err)
	}

	sub.Drain()

	if err := js.DeleteConsumer("TEST", "dur"); err != nil {
		t.Fatalf("Error deleting consumer: %v", err)
	}

	close(blockCh)

	select {
	case err := <-errCh:
		if !strings.Contains(err.Error(), "consumer not found") {
			t.Fatalf("Unexpected async error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Did not get async error")
	}
}

func TestJetStreamDomainInPubAck(t *testing.T) {
	t.Skip("DIVERGENCE: original sets root `jetstream: { domain: HUB }` so the PubAck reports Domain=\"HUB\". testservice's default template already renders a `jetstream:` block; adding another via WithTopLevel produces two blocks and the configured domain is lost (PubAck Domain is empty). Same blocker as TestJetStreamDomain / TestKeyValueMirrorCrossDomains. Recommended resolution: upstream a `WithJetStreamDomain(string)` option in testservice.")
}

func TestJetStreamStreamAndConsumerDescription(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		streamDesc := "stream description"
		si, err := js.AddStream(&nats.StreamConfig{
			Name:        "TEST",
			Description: streamDesc,
			Subjects:    []string{"foo"},
		})
		if err != nil {
			t.Fatalf("Error adding stream: %v", err)
		}
		if si.Config.Description != streamDesc {
			t.Fatalf("Invalid description: %q vs %q", streamDesc, si.Config.Description)
		}

		consDesc := "consumer description"
		ci, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			Description:    consDesc,
			Durable:        "dur",
			DeliverSubject: "bar",
		})
		if err != nil {
			t.Fatalf("Error adding consumer: %v", err)
		}
		if ci.Config.Description != consDesc {
			t.Fatalf("Invalid description: %q vs %q", consDesc, ci.Config.Description)
		}
	})
}

func TestJetStreamMsgSubjectRewrite(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo"},
		}); err != nil {
			t.Fatalf("Error adding stream: %v", err)
		}

		sub, err := nc.SubscribeSync(nats.NewInbox())
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		if _, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
			DeliverSubject: sub.Subject,
			DeliverPolicy:  nats.DeliverAllPolicy,
		}); err != nil {
			t.Fatalf("Error adding consumer: %v", err)
		}

		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}

		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Did not get message: %v", err)
		}
		if msg.Subject != "foo" {
			t.Fatalf("Subject should be %q, got %q", "foo", msg.Subject)
		}
		if string(msg.Data) != "msg" {
			t.Fatalf("Unexpected data: %q", msg.Data)
		}
	})
}

func TestJetStreamPullSubscribeFetchContext(t *testing.T) {
	c := newTester(t)
	inst := c.CreateCluster(t, 3, true, clientAdvertiseOpt(t))
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	c.WaitForJetStream(t, nc)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subject := "WQ"
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     subject,
		Replicas: 3,
	})
	if err != nil {
		t.Fatal(err)
	}

	sendMsgs := func(t *testing.T, totalMsgs int) {
		t.Helper()
		for i := range totalMsgs {
			payload := fmt.Sprintf("i:%d", i)
			if _, err := js.Publish(subject, []byte(payload)); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		}
	}
	expected := 10
	sendMsgs(t, expected)

	sub, err := js.PullSubscribe(subject, "batch-ctx")
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	t.Run("ctx background", func(t *testing.T) {
		_, err = sub.Fetch(expected, nats.Context(context.Background()))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != nats.ErrNoDeadlineContext {
			t.Errorf("Expected context deadline error, got: %v", err)
		}
	})

	t.Run("ctx canceled", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		cancel()

		_, err = sub.Fetch(expected, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.Canceled {
			t.Errorf("Expected context deadline error, got: %v", err)
		}

		ctx, cancel = context.WithCancel(context.Background())
		cancel()

		_, err = sub.Fetch(expected, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.Canceled {
			t.Errorf("Expected context deadline error, got: %v", err)
		}
	})

	t.Run("ctx timeout", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		msgs, err := sub.Fetch(expected, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		got := len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Error(err)
		}
		if info.NumAckPending != expected {
			t.Errorf("Expected %d pending acks, got: %d", expected, info.NumAckPending)
		}

		for _, msg := range msgs {
			msg.AckSync()
		}

		info, err = sub.ConsumerInfo()
		if err != nil {
			t.Error(err)
		}
		if info.NumAckPending > 0 {
			t.Errorf("Expected no pending acks, got: %d", info.NumAckPending)
		}

		ctx, cancel = context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()

		_, err = sub.Fetch(1, nats.Context(ctx))
		if err != context.DeadlineExceeded {
			t.Errorf("Expected deadline exceeded fetching next message, got: %v", err)
		}

		expected = 5
		sendMsgs(t, expected)

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		msgs, err = sub.Fetch(1, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Expected to receive a single message, got: %d", len(msgs))
		}
		for _, msg := range msgs {
			msg.Ack()
		}

		expected = 4
		msgs, err = sub.Fetch(expected, nats.Context(ctx))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		got = len(msgs)
		if got != expected {
			t.Fatalf("Got %v messages, expected at least: %v", got, expected)
		}
		for _, msg := range msgs {
			msg.AckSync()
		}

		info, err = sub.ConsumerInfo()
		if err != nil {
			t.Error(err)
		}
		if info.NumAckPending > 0 {
			t.Errorf("Expected no pending acks, got: %d", info.NumAckPending)
		}
	})

	t.Run("ctx with cancel", func(t *testing.T) {
		js, err = nc.JetStream(nats.MaxWait(2 * time.Second))
		if err != nil {
			t.Fatal(err)
		}

		sub, err := js.PullSubscribe(subject, "batch-cancel-ctx")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		total := info.NumPending

		msgs, err := sub.Fetch(1, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Expected a message, got: %d", len(msgs))
		}
		for _, msg := range msgs {
			msg.AckSync()
		}

		expected := int(total - 1)
		msgs, err = sub.Fetch(expected, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != expected {
			t.Fatalf("Expected %d messages, got: %d", expected, len(msgs))
		}
		for _, msg := range msgs {
			msg.AckSync()
		}

		_, err = sub.Fetch(expected, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.DeadlineExceeded {
			t.Fatalf("Expected deadline exceeded fetching next message, got: %v", err)
		}

		if ctx.Err() != nil {
			t.Fatalf("Expected no errors in original cancellation context, got: %v", ctx.Err())
		}

		sendMsgs(t, 5)

		var pending uint64 = 4
		msgs, err = sub.Fetch(1, nats.Context(ctx))
		if err != nil {
			t.Fatal(err)
		}
		if len(msgs) != 1 {
			t.Fatalf("Expected a message, got: %d", len(msgs))
		}
		for _, msg := range msgs {
			msg.AckSync()
		}

		cancel()

		_, err = sub.Fetch(1, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.Canceled {
			t.Fatalf("Expected deadline exceeded fetching next message, got: %v", err)
		}

		info, err = sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		total = info.NumPending
		if total != pending {
			t.Errorf("Expected %d pending messages, got: %d", pending, total)
		}
	})

	t.Run("MaxWait timeout should return nats error", func(t *testing.T) {
		_, err := sub.Fetch(1, nats.MaxWait(1*time.Nanosecond))
		if !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("Expect ErrTimeout, got err=%#v", err)
		}
	})

	t.Run("Context timeout should return context error", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Nanosecond)
		defer cancel()
		_, err := sub.Fetch(1, nats.Context(ctx))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expect context.DeadlineExceeded, got err=%#v", err)
		}
	})
}

func TestJetStreamSubscribeContextCancel(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo", "bar", "baz", "foo.*"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		toSend := 100
		for range toSend {
			js.Publish("bar", []byte("foo"))
		}

		t.Run("cancel unsubscribes and deletes ephemeral", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ch := make(chan *nats.Msg, 100)
			sub, err := js.Subscribe("bar", func(msg *nats.Msg) {
				ch <- msg

				if len(ch) >= 50 {
					cancel()
				}
			}, nats.Context(ctx))
			if err != nil {
				t.Fatal(err)
			}

			select {
			case <-ctx.Done():
			case <-time.After(3 * time.Second):
				t.Fatal("Timed out waiting for context to be canceled")
			}

			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				info, err := sub.ConsumerInfo()
				if err != nil && err == nats.ErrConsumerNotFound {
					return nil
				}
				return fmt.Errorf("Consumer still active, got: %v (info=%+v)", err, info)
			})

			got := len(ch)
			expected := 50
			if got < expected {
				t.Errorf("Expected to receive at least %d messages, got: %d", expected, got)
			}
		})

		t.Run("unsubscribe cancels child context", func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			sub, err := js.Subscribe("bar", func(msg *nats.Msg) {}, nats.Context(ctx))
			if err != nil {
				t.Fatal(err)
			}
			if err := sub.Unsubscribe(); err != nil {
				t.Fatal(err)
			}

			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				info, err := sub.ConsumerInfo()
				if err != nil && err == nats.ErrConsumerNotFound {
					return nil
				}
				return fmt.Errorf("Consumer still active, got: %v (info=%+v)", err, info)
			})
		})
	})
}
