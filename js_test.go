// Copyright 2012-2021 The NATS Authors
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

package nats

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

import (
	"crypto/sha256"
	"encoding/base64"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return natsserver.RunServer(&opts)
}

// Need access to internals for loss testing.
func TestJetStreamOrderedConsumer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&StreamConfig{
		Name:     "OBJECT",
		Subjects: []string{"a"},
		Storage:  MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

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
		js.PublishAsync("a", chunk)
	}
	js.PublishAsync("a", nil) // eof

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Do some tests on simple misconfigurations first.
	// For ordered delivery a couple of things need to be set properly.
	// Can't be durable or have ack policy that is not ack none or max deliver set.
	_, err = js.SubscribeSync("a", OrderedConsumer(), Durable("dlc"))
	if err == nil || !strings.Contains(err.Error(), "ordered consumer") {
		t.Fatalf("Expected an error, got %v", err)
	}

	_, err = js.SubscribeSync("a", OrderedConsumer(), AckExplicit())
	if err == nil || !strings.Contains(err.Error(), "ordered consumer") {
		t.Fatalf("Expected an error, got %v", err)
	}

	_, err = js.SubscribeSync("a", OrderedConsumer(), MaxDeliver(10))
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

		cb := func(m *Msg) {
			// Check for eof
			if len(m.Data) == 0 {
				done <- true
				return
			}
			atomic.AddUint32(&received, 1)
			rmsg = append(rmsg, m.Data...)
		}
		// OrderedConsumer does not need HB, it sets it on its own, but for test we override which is ok.
		sub, err := js.Subscribe("a", cb, OrderedConsumer(), IdleHeartbeat(250*time.Millisecond))
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
			t.Fatalf("Did not receive all chunks, only %d of %d total", atomic.LoadUint32(&received), si.State.Msgs)
		}
	}

	testSyncConsumer := func() {
		t.Helper()
		var received int
		var rmsg []byte

		// OrderedConsumer does not need HB, it sets it on its own, but for test we override which is ok.
		sub, err := js.SubscribeSync("a", OrderedConsumer(), IdleHeartbeat(250*time.Millisecond))
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
			t.Fatalf("Did not receive all chunks, only %d of %d total", received, si.State.Msgs)
		}
		if rsum := sha256.Sum256(rmsg); rsum != sum {
			t.Fatalf("Objects do not match")
		}
	}

	// Now run normal test.
	testConsumer()
	testSyncConsumer()

	// Now introduce some loss.
	singleLoss := func(m *Msg) *Msg {
		if rand.Intn(100) <= 10 {
			nc.removeMsgFilter("a")
			return nil
		}
		return m
	}
	nc.addMsgFilter("a", singleLoss)
	testConsumer()
	testSyncConsumer()

	multiLoss := func(m *Msg) *Msg {
		if rand.Intn(100) <= 10 {
			return nil
		}
		return m
	}
	nc.addMsgFilter("a", multiLoss)
	testConsumer()
	testSyncConsumer()

	firstOnly := func(m *Msg) *Msg {
		if meta, err := m.Metadata(); err == nil {
			if meta.Sequence.Consumer == 1 {
				nc.removeMsgFilter("a")
				return nil
			}
		}
		return m
	}
	nc.addMsgFilter("a", firstOnly)
	testConsumer()
	testSyncConsumer()

	lastOnly := func(m *Msg) *Msg {
		if meta, err := m.Metadata(); err == nil {
			if meta.Sequence.Stream >= si.State.LastSeq {
				nc.removeMsgFilter("a")
				return nil
			}
		}
		return m
	}
	nc.addMsgFilter("a", lastOnly)
	testConsumer()
	testSyncConsumer()
}

func TestJetStreamOrderedConsumerWithErrors(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// For capturing errors.
	errCh := make(chan error, 1)
	nc.SetErrorHandler(func(_ *Conn, _ *Subscription, err error) {
		errCh <- err
	})

	// Create a sample asset.
	mlen := 128 * 1024
	msg := make([]byte, mlen)

	createStream := func() {
		t.Helper()
		_, err = js.AddStream(&StreamConfig{
			Name:     "OBJECT",
			Subjects: []string{"a"},
			Storage:  MemoryStorage,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Now send into the stream as chunks.
		const chunkSize = 256
		for i := 0; i < mlen; i += chunkSize {
			var chunk []byte
			if mlen-i <= chunkSize {
				chunk = msg[i:]
			} else {
				chunk = msg[i : i+chunkSize]
			}
			js.PublishAsync("a", chunk)
		}
		select {
		case <-js.PublishAsyncComplete():
		case <-time.After(time.Second):
			t.Fatalf("Did not receive completion signal")
		}
	}

	type asset int
	const (
		deleteStream asset = iota
		deleteConsumer
	)

	testSubError := func(a asset) {
		t.Helper()
		// Again here the IdleHeartbeat is not required, just overriding top shorten test time.
		sub, err := js.SubscribeSync("a", OrderedConsumer(), IdleHeartbeat(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()

		// Since we are sync we will be paused here due to flow control.
		time.Sleep(100 * time.Millisecond)
		// Now delete the asset and make sure we get an error.
		switch a {
		case deleteStream:
			if err := js.DeleteStream("OBJECT"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		case deleteConsumer:
			// We need to grab our consumer name.
			ci, err := sub.ConsumerInfo()
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err := js.DeleteConsumer("OBJECT", ci.Name); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}
		// Make sure we get an error.
		select {
		case err := <-errCh:
			if err != ErrConsumerNotActive {
				t.Fatalf("Got wrong error, wanted %v, got %v", ErrConsumerNotActive, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive err message as expected")
		}
	}

	createStream()
	testSubError(deleteStream)

	createStream()
	testSubError(deleteConsumer)
}

// We want to make sure we do the right thing with lots of concurrent durable consumer requests.
// One should win and the others should share the delivery subject with the first one who wins.
func TestJetStreamConcurrentDurablePushConsumers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create stream.
	_, err = js.AddStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create 10 durables concurrently.
	subs := make(chan *Subscription, 10)
	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, _ := js.SubscribeSync("foo", Durable("dlc"))
			subs <- sub
		}()
	}
	// Wait for all the consumers.
	wg.Wait()
	close(subs)

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Consumers != 1 {
		t.Fatalf("Expected exactly one consumer, got %d", si.State.Consumers)
	}

	// Now send one message and make sure all subs get it.
	js.Publish("foo", []byte("Hello"))
	time.Sleep(250 * time.Millisecond) // Allow time for delivery.

	for sub := range subs {
		pending, _, _ := sub.Pending()
		if pending != 1 {
			t.Fatalf("Expected each durable to receive 1 msg, this sub got %d", pending)
		}
	}
}
