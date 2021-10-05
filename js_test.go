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
	"fmt"
	"io/ioutil"
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
	conf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	fName := conf.Name()
	conf.Close()
	if err := ioutil.WriteFile(fName, content, 0666); err != nil {
		os.Remove(fName)
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
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
			t.Fatalf("Did not receive all chunks, only %d of %d total", atomic.LoadUint32(&received), si.State.Msgs-1)
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

// We want to make sure we do the right thing with lots of concurrent queue durable consumer requests.
// One should win and the others should share the delivery subject with the first one who wins.
func TestJetStreamConcurrentQueueDurablePushConsumers(t *testing.T) {
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
			sub, _ := js.QueueSubscribeSync("foo", "bar")
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

	// Now send some messages and make sure they are distributed.
	total := 1000
	for i := 0; i < total; i++ {
		js.Publish("foo", []byte("Hello"))
	}

	timeout := time.Now().Add(2 * time.Second)
	got := 0
	for time.Now().Before(timeout) {
		got = 0
		for sub := range subs {
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
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

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
	payload := make([]byte, 1024)
	for i := 0; i < 250; i++ {
		nc.Publish("a", payload)
	}

	// Now wait that we respond to a stalled FC
	if _, err := checkSub.NextMsg(2 * time.Second); err != nil {
		t.Fatal("Library did not send FC")
	}
}

func TestJetStreamExpiredPullRequests(t *testing.T) {
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
