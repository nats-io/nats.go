// Copyright 2012-2023 The NATS Authors
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
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
)

func client(t *testing.T, s *server.Server, opts ...Option) *Conn {
	t.Helper()
	nc, err := Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return nc
}

func jsClient(t *testing.T, s *server.Server, opts ...Option) (*Conn, JetStreamContext) {
	t.Helper()
	nc := client(t, s, opts...)
	js, err := nc.JetStream(MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return natsserver.RunServer(&opts)
}

func RunServerWithConfig(configFile string) (*server.Server, *server.Options) {
	return natsserver.RunServerWithConfig(configFile)
}

func createConfFile(t *testing.T, content []byte) string {
	t.Helper()
	conf, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	fName := conf.Name()
	conf.Close()
	if err := os.WriteFile(fName, content, 0666); err != nil {
		os.Remove(fName)
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != _EMPTY_ {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

// Need access to internals for loss testing.
func TestJetStreamOrderedConsumer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error
	_, err = js.AddStream(&StreamConfig{
		Name:     "OBJECT",
		Subjects: []string{"a"},
		Storage:  MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Will be used as start time to validate proper reset to sequence on retries.
	startTime := time.Now()

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
		msg := NewMsg("a")
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

	_, err = js.SubscribeSync("a", OrderedConsumer(), DeliverSubject("some.subject"))
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
		sub, err := js.Subscribe("a", cb, OrderedConsumer(), IdleHeartbeat(250*time.Millisecond), StartTime(startTime))
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

		// OrderedConsumer does not need HB, it sets it on its own, but for test we override which is ok.
		sub, err := js.SubscribeSync("a", OrderedConsumer(), IdleHeartbeat(250*time.Millisecond), StartTime(startTime))
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

	// Now run normal test.
	testConsumer()
	testSyncConsumer()

	// Now introduce some loss.
	singleLoss := func(m *Msg) *Msg {
		if rand.Intn(100) <= 10 && m.Header.Get("data") != _EMPTY_ {
			nc.removeMsgFilter("a")
			return nil
		}
		return m
	}
	nc.addMsgFilter("a", singleLoss)
	testConsumer()
	nc.addMsgFilter("a", singleLoss)
	testSyncConsumer()

	multiLoss := func(m *Msg) *Msg {
		if rand.Intn(100) <= 10 && m.Header.Get("data") != _EMPTY_ {
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
	nc.addMsgFilter("a", firstOnly)
	testSyncConsumer()

	lastOnly := func(m *Msg) *Msg {
		if meta, err := m.Metadata(); err == nil {
			if meta.Sequence.Stream >= si.State.LastSeq-1 {
				nc.removeMsgFilter("a")
				return nil
			}
		}
		return m
	}
	nc.addMsgFilter("a", lastOnly)
	testConsumer()
	nc.addMsgFilter("a", lastOnly)
	testSyncConsumer()
}

func TestJetStreamOrderedConsumerDeleteAssets(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

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

	t.Run("remove stream, expect error", func(t *testing.T) {
		createStream()

		sub, err := js.SubscribeSync("a", OrderedConsumer(), IdleHeartbeat(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()

		// Since we are sync we will be paused here due to flow control.
		time.Sleep(100 * time.Millisecond)
		// Now delete the asset and make sure we get an error.
		if err := js.DeleteStream("OBJECT"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Make sure we get an error.
		select {
		case err := <-errCh:
			if !errors.Is(err, ErrStreamNotFound) {
				t.Fatalf("Got wrong error, wanted %v, got %v", ErrStreamNotFound, err)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive err message as expected")
		}
	})

	t.Run("remove consumer, expect it to be recreated", func(t *testing.T) {
		createStream()

		createConsSub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.OBJECT.*.a")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer createConsSub.Unsubscribe()
		// Again here the IdleHeartbeat is not required, just overriding top shorten test time.
		sub, err := js.SubscribeSync("a", OrderedConsumer(), IdleHeartbeat(200*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()

		createConsMsg, err := createConsSub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !strings.Contains(string(createConsMsg.Data), `"stream_name":"OBJECT"`) {
			t.Fatalf("Invalid message on create consumer subject: %q", string(createConsMsg.Data))
		}

		time.Sleep(100 * time.Millisecond)
		ci, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		consName := ci.Name

		if err := js.DeleteConsumer("OBJECT", consName); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		createConsMsg, err = createConsSub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if !strings.Contains(string(createConsMsg.Data), `"stream_name":"OBJECT"`) {
			t.Fatalf("Invalid message on create consumer subject: %q", string(createConsMsg.Data))
		}

		time.Sleep(100 * time.Millisecond)
		ci, err = sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		newConsName := ci.Name
		if consName == newConsName {
			t.Fatalf("Consumer should be recreated, but consumer name is the same")
		}
	})
}

func TestJetStreamOrderedConsumerWithAutoUnsub(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&StreamConfig{
		Name:     "OBJECT",
		Subjects: []string{"a"},
		Storage:  MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	count := int32(0)
	sub, err := js.Subscribe("a", func(m *Msg) {
		atomic.AddInt32(&count, 1)
	}, OrderedConsumer(), IdleHeartbeat(250*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Ask to auto-unsub after 10 messages.
	sub.AutoUnsubscribe(10)

	// Set a message filter that will drop 1 message
	dm := 0
	singleLoss := func(m *Msg) *Msg {
		if m.Header.Get("data") != _EMPTY_ {
			dm++
			if dm == 5 {
				nc.removeMsgFilter("a")
				return nil
			}
		}
		return m
	}
	nc.addMsgFilter("a", singleLoss)

	// Now produce 20 messages
	for i := 0; i < 20; i++ {
		msg := NewMsg("a")
		msg.Data = []byte(fmt.Sprintf("msg_%d", i+1))
		msg.Header.Set("data", "true")
		js.PublishMsgAsync(msg)
	}

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Wait for the subscription to be marked as invalid
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

	// Wait a bit to make sure we are not receiving more than expected,
	// and give a chance for the server to process the auto-unsub
	// protocol.
	time.Sleep(500 * time.Millisecond)

	if n := atomic.LoadInt32(&count); n != 10 {
		t.Fatalf("Sub should have received only 10 messages, got %v", n)
	}

	// Now capture the in msgs count for the connection
	inMsgs := nc.Stats().InMsgs

	// Send one more message and this count should not increase if the
	// server had properly processed the auto-unsub after the
	// reset of the ordered consumer. Use a different connection
	// to send.
	nc2, js2 := jsClient(t, s)
	defer nc2.Close()

	js2.Publish("a", []byte("should not be received"))

	newInMsgs := nc.Stats().InMsgs
	if inMsgs != newInMsgs {
		t.Fatal("Seems that AUTO-UNSUB was not properly handled")
	}
}

// We want to make sure we do the right thing with lots of concurrent queue durable consumer requests.
// One should win and the others should share the delivery subject with the first one who wins.
func TestJetStreamConcurrentQueueDurablePushConsumers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create stream.
	_, err = js.AddStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now create 10 durables concurrently.
	subs := make([]*Subscription, 0, 10)
	var wg sync.WaitGroup
	mx := &sync.Mutex{}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sub, _ := js.QueueSubscribeSync("foo", "bar")
			mx.Lock()
			subs = append(subs, sub)
			mx.Unlock()
		}()
	}
	// Wait for all the consumers.
	wg.Wait()

	si, err := js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if si.State.Consumers != 1 {
		t.Fatalf("Expected exactly one consumer, got %d", si.State.Consumers)
	}

	// Now send some messages and make sure they are distributed.
	total := 1000
	for i := 0; i < total; i++ {
		js.Publish("foo", []byte("Hello"))
	}

	timeout := time.Now().Add(2 * time.Second)
	got := 0
	for time.Now().Before(timeout) {
		got = 0
		for _, sub := range subs {
			pending, _, _ := sub.Pending()
			// If a single sub has the total, then probably something is not right.
			if pending == total {
				t.Fatalf("A single member should not have gotten all messages")
			}
			got += pending
		}
		if got == total {
			// We are done!
			return
		}
	}
	t.Fatalf("Expected %v messages, got only %v", total, got)
}

func TestJetStreamSubscribeReconnect(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	rch := make(chan struct{}, 1)
	nc, err := Connect(s.ClientURL(),
		ReconnectWait(50*time.Millisecond),
		ReconnectHandler(func(_ *Conn) {
			select {
			case rch <- struct{}{}:
			default:
			}
		}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.SubscribeSync("foo", Durable("bar"))
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

	// Cause a disconnect...
	nc.mu.Lock()
	nc.conn.Close()
	nc.mu.Unlock()

	// Wait for reconnect
	select {
	case <-rch:
	case <-time.After(time.Second):
		t.Fatal("Did not reconnect")
	}

	// Make sure we can send and receive the msg
	sendAndReceive("msg2")
}

func TestJetStreamAckTokens(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	now := time.Now()
	for _, test := range []struct {
		name     string
		expected *MsgMetadata
		str      string
		end      string
		err      bool
	}{
		{
			"valid token size but not js ack",
			nil,
			"1.2.3.4.5.6.7.8.9",
			"",
			true,
		},
		{
			"valid token size but not js ack",
			nil,
			"1.2.3.4.5.6.7.8.9.10.11.12",
			"",
			true,
		},
		{
			"invalid token size",
			nil,
			"$JS.ACK.3.4.5.6.7.8",
			"",
			true,
		},
		{
			"invalid token size",
			nil,
			"$JS.ACK.3.4.5.6.7.8.9.10",
			"",
			true,
		},
		{
			"v1 style",
			&MsgMetadata{
				Stream:       "TEST",
				Consumer:     "cons",
				NumDelivered: 1,
				Sequence: SequencePair{
					Stream:   2,
					Consumer: 3,
				},
				Timestamp:  now,
				NumPending: 4,
			},
			"",
			"",
			false,
		},
		{
			"v2 style no domain with hash",
			&MsgMetadata{
				Stream:       "TEST",
				Consumer:     "cons",
				NumDelivered: 1,
				Sequence: SequencePair{
					Stream:   2,
					Consumer: 3,
				},
				Timestamp:  now,
				NumPending: 4,
			},
			"_.ACCHASH.",
			".abcde",
			false,
		},
		{
			"v2 style with domain and hash",
			&MsgMetadata{
				Domain:       "HUB",
				Stream:       "TEST",
				Consumer:     "cons",
				NumDelivered: 1,
				Sequence: SequencePair{
					Stream:   2,
					Consumer: 3,
				},
				Timestamp:  now,
				NumPending: 4,
			},
			"HUB.ACCHASH.",
			".abcde",
			false,
		},
		{
			"more than 12 tokens",
			&MsgMetadata{
				Domain:       "HUB",
				Stream:       "TEST",
				Consumer:     "cons",
				NumDelivered: 1,
				Sequence: SequencePair{
					Stream:   2,
					Consumer: 3,
				},
				Timestamp:  now,
				NumPending: 4,
			},
			"HUB.ACCHASH.",
			".abcde.ghijk.lmnop",
			false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			msg := NewMsg("foo")
			msg.Sub = sub
			if test.err {
				msg.Reply = test.str
			} else {
				msg.Reply = fmt.Sprintf("$JS.ACK.%sTEST.cons.1.2.3.%v.4%s", test.str, now.UnixNano(), test.end)
			}

			meta, err := msg.Metadata()
			if test.err {
				if err == nil || meta != nil {
					t.Fatalf("Expected error for content: %q, got meta=%+v err=%v", test.str, meta, err)
				}
				// Expected error, we are done
				return
			}
			if err != nil {
				t.Fatalf("Expected: %+v with reply: %q, got error %v", test.expected, msg.Reply, err)
			}
			if meta.Timestamp.UnixNano() != now.UnixNano() {
				t.Fatalf("Timestamp is bad: %v vs %v", now.UnixNano(), meta.Timestamp.UnixNano())
			}
			meta.Timestamp = time.Time{}
			test.expected.Timestamp = time.Time{}
			if !reflect.DeepEqual(test.expected, meta) {
				t.Fatalf("Expected %+v, got %+v", test.expected, meta)
			}
		})
	}
}

func TestJetStreamFlowControlStalled(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"a"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.SubscribeSync("a",
		DeliverSubject("ds"),
		Durable("dur"),
		IdleHeartbeat(200*time.Millisecond),
		EnableFlowControl()); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	// Drop all incoming FC control messages.
	fcLoss := func(m *Msg) *Msg {
		if _, ctrlType := isJSControlMessage(m); ctrlType == jsCtrlFC {
			return nil
		}
		return m
	}
	nc.addMsgFilter("ds", fcLoss)

	// Have a subscription on the FC subject to make sure that the library
	// respond to the requests for un-stall
	checkSub, err := nc.SubscribeSync("$JS.FC.>")
	if err != nil {
		t.Fatalf("Error on sub: %v", err)
	}

	// Publish bunch of messages.
	payload := make([]byte, 100*1024)
	for i := 0; i < 250; i++ {
		nc.Publish("a", payload)
	}

	// Now wait that we respond to a stalled FC
	if _, err := checkSub.NextMsg(2 * time.Second); err != nil {
		t.Fatal("Library did not send FC")
	}
}

func TestJetStreamTracing(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctr := 0
	js, err := nc.JetStream(&ClientTrace{
		RequestSent: func(subj string, payload []byte) {
			ctr++
			if subj != "$JS.API.STREAM.CREATE.X" {
				t.Fatalf("Expected sent trace to %s: got: %s", "$JS.API.STREAM.CREATE.X", subj)
			}
		},
		ResponseReceived: func(subj string, payload []byte, hdr Header) {
			ctr++
			if subj != "$JS.API.STREAM.CREATE.X" {
				t.Fatalf("Expected received trace to %s: got: %s", "$JS.API.STREAM.CREATE.X", subj)
			}
		},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&StreamConfig{Name: "X"})
	if err != nil {
		t.Fatalf("add stream failed: %s", err)
	}
	if ctr != 2 {
		t.Fatalf("did not receive all trace events: %d", ctr)
	}
}

func TestJetStreamExpiredPullRequests(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	sub, err := js.PullSubscribe("foo", "bar", PullMaxWaiting(2))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	// Make sure that we reject batch < 1
	if _, err := sub.Fetch(0); err == nil {
		t.Fatal("Expected error, did not get one")
	}
	if _, err := sub.Fetch(-1); err == nil {
		t.Fatal("Expected error, did not get one")
	}

	// Send 2 fetch requests
	for i := 0; i < 2; i++ {
		if _, err = sub.Fetch(1, MaxWait(15*time.Millisecond)); err == nil {
			t.Fatalf("Expected error, got none")
		}
	}
	// Wait before the above expire
	time.Sleep(50 * time.Millisecond)
	batches := []int{1, 10}
	for _, bsz := range batches {
		start := time.Now()
		_, err = sub.Fetch(bsz, MaxWait(250*time.Millisecond))
		dur := time.Since(start)
		if err == nil || dur < 50*time.Millisecond {
			t.Fatalf("Expected error and wait for 250ms, got err=%v and dur=%v", err, dur)
		}
	}
}

func TestJetStreamSyncSubscribeWithMaxAckPending(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.JetStreamLimits.MaxAckPending = 123
	s := natsserver.RunServer(&opts)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&StreamConfig{Name: "MAX_ACK_PENDING", Subjects: []string{"foo"}}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// By default, the sync subscription will be created with a MaxAckPending equal
	// to the internal sync queue len, which is 64K. So that should error out
	// and make sure we get the actual limit

	checkSub := func(pull bool) {
		var sub *Subscription
		var err error
		if pull {
			_, err = js.PullSubscribe("foo", "bar")
		} else {
			_, err = js.SubscribeSync("foo")
		}
		if err == nil || !strings.Contains(err.Error(), "system limit of 123") {
			t.Fatalf("Unexpected error: %v", err)
		}

		// But it should work if we use MaxAckPending() with lower value
		if pull {
			sub, err = js.PullSubscribe("foo", "bar", MaxAckPending(64))
		} else {
			sub, err = js.SubscribeSync("foo", MaxAckPending(64))
		}
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		sub.Unsubscribe()
	}
	checkSub(false)
	checkSub(true)
}

func TestJetStreamClusterPlacement(t *testing.T) {
	// There used to be a test here that would not work because it would require
	// all servers in the cluster to know about each other tags. So we will simply
	// verify that if a stream is configured with placement and tags, the proper
	// "stream create" request is sent.
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	sub, err := nc.SubscribeSync(fmt.Sprintf("$JS.API."+apiStreamCreateT, "TEST"))
	if err != nil {
		t.Fatalf("Error on sub: %v", err)
	}
	js.AddStream(&StreamConfig{
		Name: "TEST",
		Placement: &Placement{
			Tags: []string{"my_tag"},
		},
	})
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error getting stream create request: %v", err)
	}
	var req StreamConfig
	if err := json.Unmarshal(msg.Data, &req); err != nil {
		t.Fatalf("Unmarshal error: %v", err)
	}
	if req.Placement == nil {
		t.Fatal("Expected placement, did not get it")
	}
	if n := len(req.Placement.Tags); n != 1 {
		t.Fatalf("Expected 1 tag, got %v", n)
	}
	if v := req.Placement.Tags[0]; v != "my_tag" {
		t.Fatalf("Unexpected tag: %q", v)
	}
}

func TestJetStreamConvertDirectMsgResponseToMsg(t *testing.T) {
	// This test checks the conversion of a "direct get message" response
	// to a JS message based on the content of specific NATS headers.
	// It is very specific to the order headers retrieval is made in
	// convertDirectGetMsgResponseToMsg(), so it may need adjustment
	// if changes are made there.

	msg := NewMsg("inbox")

	check := func(errTxt string) {
		t.Helper()
		m, err := convertDirectGetMsgResponseToMsg("test", msg)
		if err == nil || !strings.Contains(err.Error(), errTxt) {
			t.Fatalf("Expected error contain %q, got %v", errTxt, err)
		}
		if m != nil {
			t.Fatalf("Expected nil message, got %v", m)
		}
	}

	check("should have headers")

	msg.Header.Set(statusHdr, noMessagesSts)
	check(ErrMsgNotFound.Error())

	msg.Header.Set(statusHdr, reqTimeoutSts)
	check("unable to get message")

	msg.Header.Set(descrHdr, "some error text")
	check("some error text")

	msg.Header.Del(statusHdr)
	msg.Header.Del(descrHdr)
	msg.Header.Set("some", "header")
	check("missing stream")

	msg.Header.Set(JSStream, "test")
	check("missing sequence")

	msg.Header.Set(JSSequence, "abc")
	check("invalid sequence")

	msg.Header.Set(JSSequence, "1")
	check("missing timestamp")

	msg.Header.Set(JSTimeStamp, "aaaaaaaaa bbbbbbbbbbbb cccccccccc ddddddddddd eeeeeeeeee ffffff")
	check("invalid timestamp")

	msg.Header.Set(JSTimeStamp, "2006-01-02 15:04:05.999999999 +0000 UTC")
	check("missing subject")

	msg.Header.Set(JSSubject, "foo")
	r, err := convertDirectGetMsgResponseToMsg("test", msg)
	if err != nil {
		t.Fatalf("Error during convert: %v", err)
	}
	if r.Subject != "foo" {
		t.Fatalf("Expected subject to be 'foo', got %q", r.Subject)
	}
	if r.Sequence != 1 {
		t.Fatalf("Expected sequence to be 1, got %v", r.Sequence)
	}
	if r.Time.UnixNano() != 0xFC4A4D639917BFF {
		t.Fatalf("Invalid timestamp: %v", r.Time.UnixNano())
	}
	if r.Header.Get("some") != "header" {
		t.Fatalf("Wrong header: %v", r.Header)
	}
}

func TestJetStreamConsumerMemoryStorage(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	s := natsserver.RunServer(&opts)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AddStream(&StreamConfig{Name: "STR", Subjects: []string{"foo"}}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// Pull ephemeral consumer with memory storage.
	sub, err := js.PullSubscribe("foo", "", ConsumerMemoryStorage())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	consInfo, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	if !consInfo.Config.MemoryStorage {
		t.Fatalf("Expected memory storage to be %v, got %+v", true, consInfo.Config.MemoryStorage)
	}

	// Create a sync subscription with an in-memory ephemeral consumer.
	sub, err = js.SubscribeSync("foo", ConsumerMemoryStorage())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	consInfo, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	if !consInfo.Config.MemoryStorage {
		t.Fatalf("Expected memory storage to be %v, got %+v", true, consInfo.Config.MemoryStorage)
	}

	// Async subscription with an in-memory ephemeral consumer.
	cb := func(msg *Msg) {}
	sub, err = js.Subscribe("foo", cb, ConsumerMemoryStorage())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	consInfo, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	if !consInfo.Config.MemoryStorage {
		t.Fatalf("Expected memory storage to be %v, got %+v", true, consInfo.Config.MemoryStorage)
	}
}

func TestJetStreamStreamInfoWithSubjectDetails(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Publish on enough subjects to exercise the pagination
	payload := make([]byte, 10)
	for i := 0; i < 100001; i++ {
		_, err := js.Publish(fmt.Sprintf("test.%d", i), payload)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// Check that passing a filter returns the subject details
	result, err := js.StreamInfo("TEST", &StreamInfoRequest{SubjectsFilter: ">"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.State.Subjects) != 100001 {
		t.Fatalf("expected 100001 subjects in the stream, but got %d instead", len(result.State.Subjects))
	}

	// Check that passing no filter does not return any subject details
	result, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if len(result.State.Subjects) != 0 {
		t.Fatalf("expected 0 subjects details from StreamInfo, but got %d instead", len(result.State.Subjects))
	}
}

func TestStreamNameBySubject(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range []struct {
		name       string
		streamName string
		err        error
	}{

		{name: "valid wildcard lookup", streamName: "test.*", err: nil},
		{name: "valid explicit lookup", streamName: "test.a", err: nil},
		{name: "lookup on not existing stream", streamName: "not.existing", err: ErrNoMatchingStream},
	} {

		stream, err := js.StreamNameBySubject(test.streamName)
		if err != test.err {
			t.Fatalf("expected %v, got %v", test.err, err)
		}

		if stream != "TEST" && err == nil {
			t.Fatalf("returned stream name should be 'TEST'")
		}
	}
}

func TestJetStreamTransform(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.AddStream(&StreamConfig{
		Name:             "ORIGIN",
		Subjects:         []string{"test"},
		SubjectTransform: &SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
		Storage:          MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	err = nc.Publish("test", []byte("1"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&StreamConfig{
		Subjects: []string{},
		Name:     "SOURCING",
		Sources:  []*StreamSource{{Name: "ORIGIN", SubjectTransforms: []SubjectTransformConfig{{Source: ">", Destination: "fromtest.>"}}}},
		Storage:  MemoryStorage,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create a sync subscription with an in-memory ephemeral consumer.
	sub, err := js.SubscribeSync("fromtest.>", ConsumerMemoryStorage(), BindStream("SOURCING"))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if m.Subject != "fromtest.transformed.test" {
		t.Fatalf("the subject of the message doesn't match the expected fromtest.transformed.test: %s", m.Subject)
	}

}
