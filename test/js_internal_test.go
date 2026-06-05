// Copyright 2023-2026 The NATS Authors
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
	"crypto/sha256"
	"encoding/base64"
	"fmt"
	"math/rand"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

// Need access to internals for loss testing.
func TestJetStreamOrderedConsumer(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "OBJECT",
			Subjects: []string{"a"},
			Storage:  nats.MemoryStorage,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Will be used as start time to validate proper reset to sequence on retries.
		// Use the server's own clock via StreamInfo to avoid host/container clock skew.
		si0, err := js.StreamInfo("OBJECT")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// startTime is just before the first publish — use the server's "now"
		// via Created time of the stream (which is right after creation).
		startTime := si0.Created

		// Create a sample asset.
		msg := make([]byte, 1024*1024)
		rand.Read(msg)
		msg = []byte(base64.StdEncoding.EncodeToString(msg))
		mlen, sum := len(msg), sha256.Sum256(msg)

		// Now send into the stream as chunks.
		const chunkSize = 1024
		for i := 0; i < mlen; i += chunkSize {
			var chunk []byte
			if mlen-i <= chunkSize {
				chunk = msg[i:]
			} else {
				chunk = msg[i : i+chunkSize]
			}
			msg := nats.NewMsg("a")
			msg.Data = chunk
			msg.Header.Set("data", "true")
			js.PublishMsgAsync(msg)
		}
		js.PublishAsync("a", nil) // eof

		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		// Do some tests on simple misconfigurations first.
		_, err = js.SubscribeSync("a", nats.OrderedConsumer(), nats.Durable("dlc"))
		if err == nil || !strings.Contains(err.Error(), "ordered consumer") {
			t.Fatalf("Expected an error, got %v", err)
		}

		_, err = js.SubscribeSync("a", nats.OrderedConsumer(), nats.AckExplicit())
		if err == nil || !strings.Contains(err.Error(), "ordered consumer") {
			t.Fatalf("Expected an error, got %v", err)
		}

		_, err = js.SubscribeSync("a", nats.OrderedConsumer(), nats.MaxDeliver(10))
		if err == nil || !strings.Contains(err.Error(), "ordered consumer") {
			t.Fatalf("Expected an error, got %v", err)
		}

		_, err = js.SubscribeSync("a", nats.OrderedConsumer(), nats.DeliverSubject("some.subject"))
		if err == nil || !strings.Contains(err.Error(), "ordered consumer") {
			t.Fatalf("Expected an error, got %v", err)
		}

		si, err := js.StreamInfo("OBJECT")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		testConsumer := func() {
			t.Helper()
			var received uint32
			var rmsg []byte
			done := make(chan bool, 1)

			cb := func(m *nats.Msg) {
				if len(m.Data) == 0 {
					done <- true
					return
				}
				atomic.AddUint32(&received, 1)
				rmsg = append(rmsg, m.Data...)
			}
			sub, err := js.Subscribe("a", cb, nats.OrderedConsumer(), nats.IdleHeartbeat(250*time.Millisecond), nats.StartTime(startTime))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()

			select {
			case <-done:
				if rsum := sha256.Sum256(rmsg); rsum != sum {
					t.Fatalf("Objects do not match")
				}
			case <-time.After(5 * time.Second):
				t.Fatalf("Did not receive all chunks, only %d of %d total", atomic.LoadUint32(&received), si.State.Msgs-1)
			}
		}

		testSyncConsumer := func() {
			t.Helper()
			var received int
			var rmsg []byte

			sub, err := js.SubscribeSync("a", nats.OrderedConsumer(), nats.IdleHeartbeat(250*time.Millisecond), nats.StartTime(startTime))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()

			var done bool
			expires := time.Now().Add(5 * time.Second)
			for time.Now().Before(expires) {
				m, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if len(m.Data) == 0 {
					done = true
					break
				}
				received++
				rmsg = append(rmsg, m.Data...)
			}
			if !done {
				t.Fatalf("Did not receive all chunks, only %d of %d total", received, si.State.Msgs-1)
			}
			if rsum := sha256.Sum256(rmsg); rsum != sum {
				t.Fatalf("Objects do not match")
			}
		}

		testConsumer()
		testSyncConsumer()

		// Now introduce some loss.
		singleLoss := func(m *nats.Msg) *nats.Msg {
			if rand.Intn(100) <= 10 && m.Header.Get("data") != "" {
				nc.RemoveMsgFilter("a")
				return nil
			}
			return m
		}
		nc.AddMsgFilter("a", singleLoss)
		testConsumer()
		nc.AddMsgFilter("a", singleLoss)
		testSyncConsumer()

		multiLoss := func(m *nats.Msg) *nats.Msg {
			if rand.Intn(100) <= 10 && m.Header.Get("data") != "" {
				return nil
			}
			return m
		}
		nc.AddMsgFilter("a", multiLoss)
		testConsumer()
		testSyncConsumer()

		firstOnly := func(m *nats.Msg) *nats.Msg {
			if meta, err := m.Metadata(); err == nil {
				if meta.Sequence.Consumer == 1 {
					nc.RemoveMsgFilter("a")
					return nil
				}
			}
			return m
		}
		nc.AddMsgFilter("a", firstOnly)
		testConsumer()
		nc.AddMsgFilter("a", firstOnly)
		testSyncConsumer()

		lastOnly := func(m *nats.Msg) *nats.Msg {
			if meta, err := m.Metadata(); err == nil {
				if meta.Sequence.Stream >= si.State.LastSeq-1 {
					nc.RemoveMsgFilter("a")
					return nil
				}
			}
			return m
		}
		nc.AddMsgFilter("a", lastOnly)
		testConsumer()
		nc.AddMsgFilter("a", lastOnly)
		testSyncConsumer()
	})
}

func TestJetStreamOrderedConsumerWithAutoUnsub(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "OBJECT",
			Subjects: []string{"a"},
			Storage:  nats.MemoryStorage,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		count := int32(0)
		sub, err := js.Subscribe("a", func(m *nats.Msg) {
			atomic.AddInt32(&count, 1)
		}, nats.OrderedConsumer(), nats.IdleHeartbeat(250*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub.AutoUnsubscribe(10)

		dm := 0
		singleLoss := func(m *nats.Msg) *nats.Msg {
			if m.Header.Get("data") != "" {
				dm++
				if dm == 5 {
					nc.RemoveMsgFilter("a")
					return nil
				}
			}
			return m
		}
		nc.AddMsgFilter("a", singleLoss)

		for i := 0; i < 20; i++ {
			msg := nats.NewMsg("a")
			msg.Data = []byte(fmt.Sprintf("msg_%d", i+1))
			msg.Header.Set("data", "true")
			js.PublishMsgAsync(msg)
		}

		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(time.Second):
			t.Fatalf("Did not receive completion signal")
		}

		deadline := time.Now().Add(time.Second)
		ok := false
		for time.Now().Before(deadline) {
			if !sub.IsValid() {
				ok = true
				break
			}
		}
		if !ok {
			t.Fatalf("Subscription still valid")
		}

		time.Sleep(500 * time.Millisecond)

		if n := atomic.LoadInt32(&count); n != 10 {
			t.Fatalf("Sub should have received only 10 messages, got %v", n)
		}

		inMsgs := nc.Stats().InMsgs

		// Use a separate connection (against the same testservice instance) to
		// publish — mirror the original test's nc2 path.
		nc2 := dialInstance(t, inst)
		js2, err := nc2.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js2.Publish("a", []byte("should not be received"))

		newInMsgs := nc.Stats().InMsgs
		if inMsgs != newInMsgs {
			t.Fatal("Seems that AUTO-UNSUB was not properly handled")
		}
	})
}

func TestJetStreamSubscribeReconnect(t *testing.T) {
	// Custom ReconnectHandler needed, so build manually rather than using
	// withJSServer (which dials with default options).
	c := newTester(t)
	jsInst := c.CreateServer(t, true)
	t.Cleanup(func() { jsInst.Destroy(t) })

	rch := make(chan struct{}, 1)
	nc, err := nats.Connect(jsInst.Servers[0].URL,
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectHandler(func(_ *nats.Conn) {
			select {
			case rch <- struct{}{}:
			default:
			}
		}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()
	c.WaitForJetStream(t, nc)

	js, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
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

	sub, err := js.SubscribeSync("foo", nats.Durable("bar"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	sendAndReceive := func(msgContent string) {
		t.Helper()
		var ok bool
		var err error
		for i := 0; i < 5; i++ {
			if _, err = js.Publish("foo", []byte(msgContent)); err != nil {
				time.Sleep(250 * time.Millisecond)
				continue
			}
			ok = true
			break
		}
		if !ok {
			t.Fatalf("Error on publish: %v", err)
		}
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatal("Did not get message")
		}
		if string(msg.Data) != msgContent {
			t.Fatalf("Unexpected content: %q", msg.Data)
		}
		if err := msg.AckSync(); err != nil {
			t.Fatalf("Error on ack: %v", err)
		}
	}

	sendAndReceive("msg1")

	// Cause a disconnect via CloseTCPConn (internal_testing helper).
	nc.CloseTCPConn()

	select {
	case <-rch:
	case <-time.After(time.Second):
		t.Fatal("Did not reconnect")
	}

	sendAndReceive("msg2")
}

func TestJetStreamFlowControlStalled(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"a"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.SubscribeSync("a",
			nats.DeliverSubject("ds"),
			nats.Durable("dur"),
			nats.IdleHeartbeat(200*time.Millisecond),
			nats.EnableFlowControl()); err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		jsCtrlFC := 2
		fcLoss := func(m *nats.Msg) *nats.Msg {
			if _, ctrlType := nats.IsJSControlMessage(m); ctrlType == jsCtrlFC {
				return nil
			}
			return m
		}
		nc.AddMsgFilter("ds", fcLoss)

		checkSub, err := nc.SubscribeSync("$JS.FC.>")
		if err != nil {
			t.Fatalf("Error on sub: %v", err)
		}

		payload := make([]byte, 100*1024)
		for i := 0; i < 250; i++ {
			nc.Publish("a", payload)
		}

		if _, err := checkSub.NextMsg(2 * time.Second); err != nil {
			t.Fatal("Library did not send FC")
		}
	})
}
