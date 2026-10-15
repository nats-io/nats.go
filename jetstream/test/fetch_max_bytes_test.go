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
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestConsumerFetchMaxBytes(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := "pull"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			for _, test := range []struct {
				name       string
				batch      int
				maxBytes   int
				want       int
				exact      bool
				headers    bool
				useContext bool
			}{
				{name: "message limit first", batch: 2, maxBytes: 1 << 20, want: 2},
				{name: "byte limit first", batch: 5, maxBytes: 2500, want: 2},
				{name: "exact byte limit", batch: 5, want: 2, exact: true},
				{name: "message exceeds byte limit", batch: 5, maxBytes: 1, want: 0},
				{name: "headers count toward byte limit", batch: 5, maxBytes: 2500, want: 1, headers: true},
				{name: "with context", batch: 5, maxBytes: 2500, want: 2, useContext: true},
			} {
				t.Run(test.name, func(t *testing.T) {
					withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
						ctx := newTesterCtx(t, 5*time.Second)
						s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST", Subjects: []string{"events"}})
						if err != nil {
							t.Fatal(err)
						}
						var c jetstream.Consumer
						if ordered {
							c, err = s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
						} else {
							c, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{
								Name: "con", AckPolicy: jetstream.AckExplicitPolicy, MaxRequestBatch: 5,
							})
						}
						if err != nil {
							t.Fatal(err)
						}
						maxBytes := test.maxBytes
						if test.exact {
							maxBytes = 2 * deliveredMsgSize(t, js, c, "events", make([]byte, 1000))
						}
						for i := 0; i < 5; i++ {
							msg := &nats.Msg{Subject: "events", Data: bytes.Repeat([]byte{byte(i)}, 1000)}
							if test.headers {
								msg.Header = nats.Header{"X-Test": []string{string(bytes.Repeat([]byte{'h'}, 1000))}}
							}
							if _, err := js.PublishMsg(ctx, msg); err != nil {
								t.Fatal(err)
							}
						}
						opts := []jetstream.FetchOpt{jetstream.FetchMaxBytes(maxBytes)}
						if test.useContext {
							opts = append(opts, jetstream.FetchContext(ctx))
						} else {
							opts = append(opts, jetstream.FetchMaxWait(time.Second))
						}
						msgs, err := c.Fetch(test.batch, opts...)
						if err != nil {
							t.Fatal(err)
						}
						var got int
						for msg := range msgs.Messages() {
							if msg.Data()[0] != byte(got) {
								t.Fatalf("Expected message %d; got %d", got, msg.Data()[0])
							}
							if !ordered {
								if err := msg.DoubleAck(ctx); err != nil {
									t.Fatal(err)
								}
							}
							got++
						}
						if msgs.Error() != nil {
							t.Fatal(msgs.Error())
						}
						if got != test.want {
							t.Fatalf("Expected %d messages; got %d", test.want, got)
						}
						// The next fetch must receive every message excluded by the limit.
						rest, err := c.Fetch(5-got, jetstream.FetchMaxWait(time.Second))
						if err != nil {
							t.Fatal(err)
						}
						for msg := range rest.Messages() {
							if msg.Data()[0] != byte(got) {
								t.Fatalf("Expected remaining message %d; got %d", got, msg.Data()[0])
							}
							got++
						}
						if rest.Error() != nil {
							t.Fatal(rest.Error())
						}
						if got != 5 {
							t.Fatalf("Expected all 5 messages across both fetches; got %d", got)
						}
					})
				})
			}
		})
	}
}

func TestConsumerFetchMaxBytesInvalidOptions(t *testing.T) {
	for _, ordered := range []bool{false, true} {
		name := "pull"
		if ordered {
			name = "ordered"
		}
		t.Run(name, func(t *testing.T) {
			withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
				ctx := newTesterCtx(t, 5*time.Second)
				s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST", Subjects: []string{"events"}})
				if err != nil {
					t.Fatal(err)
				}
				var c jetstream.Consumer
				if ordered {
					c, err = s.OrderedConsumer(ctx, jetstream.OrderedConsumerConfig{})
				} else {
					c, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
				}
				if err != nil {
					t.Fatal(err)
				}
				if _, err := js.Publish(ctx, "events", []byte("hello")); err != nil {
					t.Fatal(err)
				}
				for _, maxBytes := range []int{-1, 0, 1, 1000} {
					opts := []jetstream.FetchOpt{jetstream.FetchMaxBytes(maxBytes), jetstream.FetchMaxWait(time.Millisecond)}
					if maxBytes <= 0 {
						if _, err := c.Fetch(1, opts...); !errors.Is(err, jetstream.ErrInvalidOption) {
							t.Errorf("Fetch with byte limit %d: expected ErrInvalidOption; got %v", maxBytes, err)
						}
					}
					// Reject even when FetchBytes' own limit is zero or equals the option.
					for _, limit := range []int{0, 1000} {
						if _, err := c.FetchBytes(limit, opts...); !errors.Is(err, jetstream.ErrInvalidOption) {
							t.Errorf("FetchBytes(%d) with byte limit %d: expected ErrInvalidOption; got %v", limit, maxBytes, err)
						}
					}
					if _, err := c.Next(opts...); !errors.Is(err, jetstream.ErrInvalidOption) {
						t.Errorf("Next with byte limit %d: expected ErrInvalidOption; got %v", maxBytes, err)
					}
				}
				// Rejected options must not consume messages or prevent a valid fetch.
				msgs, err := c.Fetch(1, jetstream.FetchMaxBytes(1), jetstream.FetchMaxBytes(1000), jetstream.FetchMaxWait(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				var got int
				for msg := range msgs.Messages() {
					if string(msg.Data()) != "hello" {
						t.Fatalf("Unexpected message: %q", msg.Data())
					}
					got++
				}
				if got != 1 || msgs.Error() != nil {
					t.Fatalf("Expected one message; got %d, error %v", got, msgs.Error())
				}
			})
		})
	}
}

func TestPullConsumerFetchMaxBytesAckPending(t *testing.T) {
	withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
		ctx := newTesterCtx(t, 5*time.Second)
		s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST", Subjects: []string{"events"}})
		if err != nil {
			t.Fatal(err)
		}
		c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy, MaxAckPending: 2, MaxRequestBatch: 2})
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 3; i++ {
			if _, err := js.Publish(ctx, "events", []byte("hello")); err != nil {
				t.Fatal(err)
			}
		}
		msgs, err := c.Fetch(2, jetstream.FetchMaxBytes(1<<20), jetstream.FetchMaxWait(3*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		var batch []jetstream.Msg
		deadline := time.NewTimer(time.Second)
		defer deadline.Stop()
	loop:
		for {
			select {
			case msg, ok := <-msgs.Messages():
				if !ok {
					break loop
				}
				batch = append(batch, msg)
			case <-deadline.C:
				t.Fatal("Fetch did not complete at the message limit before acknowledgements")
			}
		}
		if len(batch) != 2 || msgs.Error() != nil {
			t.Fatalf("Expected 2 messages; got %d, error %v", len(batch), msgs.Error())
		}
		for _, msg := range batch {
			if err := msg.DoubleAck(ctx); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := c.Next(jetstream.FetchMaxWait(time.Second)); err != nil {
			t.Fatalf("Remaining message was not available: %v", err)
		}
	})
}

func TestPullConsumerFetchBytesPreservesByteOnlyLimit(t *testing.T) {
	for _, test := range []struct {
		name      string
		ackPolicy jetstream.AckPolicy
	}{
		{name: "no acknowledgements", ackPolicy: jetstream.AckNonePolicy},
		{name: "unlimited pending acknowledgements", ackPolicy: jetstream.AckExplicitPolicy},
	} {
		t.Run(test.name, func(t *testing.T) {
			withJSServer(t, func(t *testing.T, _ *nats.Conn, js jetstream.JetStream) {
				ctx := newTesterCtx(t, 5*time.Second)
				s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST", Subjects: []string{"events"}})
				if err != nil {
					t.Fatal(err)
				}
				config := jetstream.ConsumerConfig{AckPolicy: test.ackPolicy}
				if test.ackPolicy == jetstream.AckExplicitPolicy {
					config.MaxAckPending = -1
				}
				c, err := s.CreateOrUpdateConsumer(ctx, config)
				if err != nil {
					t.Fatal(err)
				}
				// FetchBytes must not acquire the default continuous-consumption
				// message limit (500) when it is meant to be limited only by bytes.
				for i := 0; i < 501; i++ {
					if _, err := js.Publish(ctx, "events", []byte("hello")); err != nil {
						t.Fatal(err)
					}
				}
				msgs, err := c.FetchBytes(1<<20, jetstream.FetchMaxWait(500*time.Millisecond))
				if err != nil {
					t.Fatal(err)
				}
				var got int
				for range msgs.Messages() {
					got++
				}
				if got != 501 || msgs.Error() != nil {
					t.Fatalf("Expected all 501 messages; got %d, error %v", got, msgs.Error())
				}
			})
		})
	}
}

func BenchmarkConsumerFetchByteLimits(b *testing.B) {
	client := newTester(b)
	inst := client.CreateServer(b, true)
	b.Cleanup(func() { inst.Destroy(b) })
	nc := dialInstance(b, inst)
	js, err := jetstream.New(nc)
	if err != nil {
		b.Fatal(err)
	}
	ctx := context.Background()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "TEST", Subjects: []string{"events"}})
	if err != nil {
		b.Fatal(err)
	}
	c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
	if err != nil {
		b.Fatal(err)
	}
	for _, test := range []struct {
		name  string
		fetch func() (jetstream.MessageBatch, error)
	}{
		{name: "FetchBytes", fetch: func() (jetstream.MessageBatch, error) {
			return c.FetchBytes(32<<20, jetstream.FetchMaxWait(time.Millisecond))
		}},
		{name: "FetchMaxBytes", fetch: func() (jetstream.MessageBatch, error) {
			return c.Fetch(500, jetstream.FetchMaxBytes(32<<20), jetstream.FetchMaxWait(time.Millisecond))
		}},
	} {
		b.Run(test.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				msgs, err := test.fetch()
				if err != nil {
					b.Fatal(err)
				}
				for range msgs.Messages() {
					b.Fatal("Expected an empty stream")
				}
				if msgs.Error() != nil {
					b.Fatal(msgs.Error())
				}
			}
		})
	}
}
