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

//go:build internal_testing

package test

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// TestOrderedPushConsumerSequenceGaps is the parity port of
// TestJetStreamOrderedConsumer in test/js_internal_test.go. It uses
// nc.AddMsgFilter (only available with the internal_testing build tag) to
// drop client-side messages and force sequence gaps, which exercises the
// new ordered push consumer's reset-and-recover path under realistic loss
// patterns. The sync-subscriber variant is omitted because
// OrderedPushConsumer is callback-only.
func TestOrderedPushConsumerSequenceGaps(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	defer nc.Close()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:     "OBJECT",
		Subjects: []string{"a"},
		Storage:  jetstream.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("CreateStream: %v", err)
	}

	// Use a start time so all consumer recreations resume from the same point.
	startTime := time.Now()

	// Build a ~1MB asset, chunked.
	msg := make([]byte, 1024*1024)
	rand.Read(msg)
	msg = []byte(base64.StdEncoding.EncodeToString(msg))
	mlen, sum := len(msg), sha256.Sum256(msg)

	const chunkSize = 1024
	for i := 0; i < mlen; i += chunkSize {
		end := i + chunkSize
		if end > mlen {
			end = mlen
		}
		m := nats.NewMsg("a")
		m.Data = msg[i:end]
		m.Header.Set("data", "true")
		if _, err := js.PublishMsgAsync(m); err != nil {
			t.Fatalf("PublishMsgAsync: %v", err)
		}
	}
	if _, err := js.PublishAsync("a", nil); err != nil {
		t.Fatalf("PublishAsync eof: %v", err)
	}

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatal("Did not receive publish completion signal")
	}

	testConsumer := func(t *testing.T) {
		t.Helper()
		var received uint32
		var mu sync.Mutex
		var rmsg []byte
		done := make(chan struct{}, 1)

		c, err := s.OrderedPushConsumer(ctx, jetstream.OrderedPushConsumerConfig{
			DeliverPolicy: jetstream.DeliverByStartTimePolicy,
			OptStartTime:  &startTime,
			IdleHeartbeat: 250 * time.Millisecond,
		})
		if err != nil {
			t.Fatalf("OrderedPushConsumer: %v", err)
		}
		cc, err := c.Consume(func(m jetstream.Msg) {
			if len(m.Data()) == 0 {
				select {
				case done <- struct{}{}:
				default:
				}
				return
			}
			atomic.AddUint32(&received, 1)
			mu.Lock()
			rmsg = append(rmsg, m.Data()...)
			mu.Unlock()
		})
		if err != nil {
			t.Fatalf("Consume: %v", err)
		}
		defer cc.Stop()

		select {
		case <-done:
			cc.Stop()
			<-cc.Closed()
			mu.Lock()
			defer mu.Unlock()
			if rsum := sha256.Sum256(rmsg); rsum != sum {
				t.Fatalf("Reassembled object does not match (received %d chunks)", atomic.LoadUint32(&received))
			}
		case <-time.After(30 * time.Second):
			mu.Lock()
			defer mu.Unlock()
			t.Fatalf("Did not receive EOF; got %d chunks (~%d bytes)",
				atomic.LoadUint32(&received), len(rmsg))
		}
	}

	t.Run("baseline (no loss)", testConsumer)

	t.Run("single message loss (~10%, then filter removes itself)", func(t *testing.T) {
		nc.AddMsgFilter("a", func(m *nats.Msg) *nats.Msg {
			if rand.Intn(100) <= 10 && m.Header.Get("data") != "" {
				nc.RemoveMsgFilter("a")
				return nil
			}
			return m
		})
		testConsumer(t)
	})

	t.Run("first message only loss", func(t *testing.T) {
		nc.AddMsgFilter("a", func(m *nats.Msg) *nats.Msg {
			if meta, err := m.Metadata(); err == nil {
				if meta.Sequence.Consumer == 1 {
					nc.RemoveMsgFilter("a")
					return nil
				}
			}
			return m
		})
		testConsumer(t)
	})

	t.Run("last messages only loss", func(t *testing.T) {
		// Forces resets very close to EOF, exercising the recovery-near-end
		// path that earlier surfaced a real bug in cursor/serial race.
		streamInfo, err := s.Info(ctx)
		if err != nil {
			t.Fatal(err)
		}
		lastSeq := streamInfo.State.LastSeq
		nc.AddMsgFilter("a", func(m *nats.Msg) *nats.Msg {
			if meta, err := m.Metadata(); err == nil {
				if meta.Sequence.Stream >= lastSeq-1 {
					nc.RemoveMsgFilter("a")
					return nil
				}
			}
			return m
		})
		testConsumer(t)
	})
}
