// Copyright 2020 The NATS Authors
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
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestJetStreamNotEnabled(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	if _, err := nc.JetStream(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestJetStreamNotAccountEnabled(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
			},
			IU: {
				users: [ {user: rip, password: bar} ]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	if _, err := nc.JetStream(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestJetStreamPublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Make sure we get a proper failure when no stream is present.
	_, err = js.Publish("foo", []byte("Hello JS"))
	if err != nats.ErrNoStreamResponse {
		t.Fatalf("Expected a no stream error but got %v", err)
	}

	// Create the stream using our client API.
	si, err := js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"test", "foo", "bar"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Double check that file-based storage is default.
	if si.Config.Storage != nats.FileStorage {
		t.Fatalf("Expected FileStorage as default, got %v", si.Config.Storage)
	}

	// Lookup the stream for testing.
	_, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	var pa *nats.PubAck
	expect := func(seq, nmsgs uint64) {
		t.Helper()
		if seq > 0 && pa == nil {
			t.Fatalf("Missing pubAck to test sequence %d", seq)
		}
		if pa != nil {
			if pa.Stream != "TEST" {
				t.Fatalf("Wrong stream name, expected %q, got %q", "TEST", pa.Stream)
			}
			if seq > 0 && pa.Sequence != seq {
				t.Fatalf("Wrong stream sequence, expected %d, got %d", seq, pa.Sequence)
			}
		}

		stream, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("stream lookup failed: %v", err)
		}
		if stream.State.Msgs != nmsgs {
			t.Fatalf("Expected %d messages, got %d", nmsgs, stream.State.Msgs)
		}
	}

	msg := []byte("Hello JS")

	// Basic publish like NATS core.
	pa, err = js.Publish("foo", msg)
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(1, 1)

	// Test stream expectation.
	pa, err = js.Publish("foo", msg, nats.ExpectStream("ORDERS"))
	if err == nil || !strings.Contains(err.Error(), "stream does not match") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Test last sequence expectation.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(10))
	if err == nil || !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Messages should have been rejected.
	expect(0, 1)

	// Send in a stream with a msgId
	pa, err = js.Publish("foo", msg, nats.MsgId("ZZZ"))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(2, 2)

	// Send in the same message with same msgId.
	pa, err = js.Publish("foo", msg, nats.MsgId("ZZZ"))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	if pa.Sequence != 2 {
		t.Fatalf("Expected sequence of 2, got %d", pa.Sequence)
	}
	if !pa.Duplicate {
		t.Fatalf("Expected duplicate to be set")
	}
	expect(2, 2)

	// Now try to send one in with the wrong last msgId.
	pa, err = js.Publish("foo", msg, nats.ExpectLastMsgId("AAA"))
	if err == nil || !strings.Contains(err.Error(), "wrong last msg") {
		t.Fatalf("Expected an error, got %v", err)
	}
	// Make sure expected sequence works.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(22))
	if err == nil || !strings.Contains(err.Error(), "wrong last sequence") {
		t.Fatalf("Expected an error, got %v", err)
	}
	expect(0, 2)

	// This should work ok.
	pa, err = js.Publish("foo", msg, nats.ExpectLastSequence(2))
	if err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
	expect(3, 3)

	// Now test context and timeouts.
	// Both set should fail.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = js.Publish("foo", msg, nats.AckWait(time.Second), nats.Context(ctx))
	if err != nats.ErrContextAndTimeout {
		t.Fatalf("Expected %q, got %q", nats.ErrContextAndTimeout, err)
	}

	// Create dummy listener for timeout and context tests.
	sub, _ := nc.SubscribeSync("baz")
	defer sub.Unsubscribe()

	_, err = js.Publish("baz", msg, nats.AckWait(time.Nanosecond))
	if err != nats.ErrTimeout {
		t.Fatalf("Expected %q, got %q", nats.ErrTimeout, err)
	}

	go cancel()
	_, err = js.Publish("baz", msg, nats.Context(ctx))
	if err != context.Canceled {
		t.Fatalf("Expected %q, got %q", context.Canceled, err)
	}
}

func TestJetStreamSubscribe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectConsumers := func(t *testing.T, expected int) []*nats.ConsumerInfo {
		t.Helper()
		cl := js.NewConsumerLister("TEST")
		if !cl.Next() {
			if err := cl.Err(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			t.Fatalf("Unexpected consumer lister next")
		}
		p := cl.Page()
		if len(p) != expected {
			t.Fatalf("Expected %d consumers, got: %d", expected, len(p))
		}
		if err := cl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		return p
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "foo.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Lookup the stream for testing.
	_, err = js.StreamInfo("TEST")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	msg := []byte("Hello JS")

	// Basic publish like NATS core.
	js.Publish("foo", msg)

	q := make(chan *nats.Msg, 4)

	// Now create a simple ephemeral consumer.
	sub, err := js.Subscribe("foo", func(m *nats.Msg) {
		q <- m
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	select {
	case m := <-q:
		if _, err := m.MetaData(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the messages in time")
	}

	// Now do same but sync.
	sub, err = js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	waitForPending := func(t *testing.T, n int) {
		t.Helper()
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	waitForPending(t, 1)

	toSend := 10
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	done := make(chan bool, 1)
	var received int
	sub, err = js.Subscribe("bar", func(m *nats.Msg) {
		received++
		if received == toSend {
			done <- true
		}
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 3)
	defer sub.Unsubscribe()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive all of the messages in time")
	}

	// If we are here we have received all of the messages.
	// We hang the ConsumerInfo option off of the subscription, so we use that to check status.
	info, _ := sub.ConsumerInfo()
	if info.Config.AckPolicy != nats.AckExplicitPolicy {
		t.Fatalf("Expected ack explicit policy, got %q", info.Config.AckPolicy)
	}
	if info.Delivered.Consumer != uint64(toSend) {
		t.Fatalf("Expected to have received all %d messages, got %d", toSend, info.Delivered.Consumer)
	}
	// Make sure we auto-ack'd
	if info.AckFloor.Consumer != uint64(toSend) {
		t.Fatalf("Expected to have ack'd all %d messages, got ack floor of %d", toSend, info.AckFloor.Consumer)
	}
	sub.Unsubscribe()
	expectConsumers(t, 2)

	// Now create a sync subscriber that is durable.
	dname := "derek"
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectConsumers(t, 3)

	// Make sure we registered as a durable.
	if info, _ := sub.ConsumerInfo(); info.Config.Durable != dname {
		t.Fatalf("Expected durable name to be set to %q, got %q", dname, info.Config.Durable)
	}
	deliver := sub.Subject

	// Remove subscription, but do not delete consumer.
	sub.Drain()
	nc.Flush()
	expectConsumers(t, 3)

	// Reattach using the same consumer.
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if deliver != sub.Subject {
		t.Fatal("Expected delivery subject to be the same after reattach")
	}
	expectConsumers(t, 3)

	// Cleanup the consumer to be able to create again with a different delivery subject.
	// this should be the same as `sub.Unsubscribe()'.
	js.DeleteConsumer("TEST", dname)
	expectConsumers(t, 2)

	// Create again and make sure that works and that we attach to the same durable with different delivery.
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectConsumers(t, 3)

	if deliver == sub.Subject {
		t.Fatalf("Expected delivery subject to be different then %q", deliver)
	}
	deliver = sub.Subject

	// Now test that we can attach to an existing durable.
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	if deliver != sub.Subject {
		t.Fatalf("Expected delivery subject to be the same when attaching, got different")
	}

	// New QueueSubscribeSync with the same durable name will not
	// create new consumers.
	sub, err = js.QueueSubscribeSync("foo", "v0", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	waitForPending(t, 0)
	expectConsumers(t, 3)

	// QueueSubscribeSync with a wrong subject from the previous consumer
	// is an error.
	_, err = js.QueueSubscribeSync("bar", "v0", nats.Durable(dname))
	if err == nil {
		t.Fatalf("Unexpected success")
	}

	// QueueSubscribeSync with a different durable name will receive
	// the messages.
	qsubDurable := nats.Durable(dname + "-qsub")
	sub, err = js.QueueSubscribeSync("bar", "v0", qsubDurable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	waitForPending(t, 10)
	expectConsumers(t, 4)

	// Now try pull based subscribers.

	// Check some error conditions first.
	if _, err := js.Subscribe("bar", func(m *nats.Msg) {}, nats.Pull(1)); err != nats.ErrPullModeNotAllowed {
		t.Fatalf("Expected an error trying to do PullMode on callback based subscriber, got %v", err)
	}

	batch := 5
	sub, err = js.SubscribeSync("bar", nats.Durable("rip"), nats.Pull(batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// The first batch if available should be delivered and queued up.
	waitForPending(t, batch)

	if info, _ := sub.ConsumerInfo(); info.NumAckPending != batch || info.NumPending != uint64(batch) {
		t.Fatalf("Expected %d pending ack, and %d still waiting to be delivered, got %d and %d", batch, batch, info.NumAckPending, info.NumPending)
	}

	// Now go ahead and consume these and ack, but not ack+next.
	for i := 0; i < batch; i++ {
		m, err := sub.NextMsg(10 * time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		m.Respond(nats.AckAck)
	}
	if info, _ := sub.ConsumerInfo(); info.AckFloor.Consumer != uint64(batch) {
		t.Fatalf("Expected ack floor to be %d, got %d", batch, info.AckFloor.Consumer)
	}

	// Now we are stuck so to speak. So we can unstick the sub by calling poll.
	waitForPending(t, 0)
	sub.Poll()
	waitForPending(t, batch)
	sub.Drain()

	// Now test attaching to a pull based durable.

	// Test that if we are attaching that the subjects will match up. rip from
	// above was created with a filtered subject of bar, so this should fail.
	_, err = js.SubscribeSync("baz", nats.Durable("rip"), nats.Pull(batch))
	if err != nats.ErrSubjectMismatch {
		t.Fatalf("Expected a %q error but got %q", nats.ErrSubjectMismatch, err)
	}

	// Queue up 10 more messages.
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	sub, err = js.SubscribeSync("bar", nats.Durable("rip"), nats.Pull(batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	waitForPending(t, batch)

	info, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.NumAckPending != batch*2 || info.NumPending != uint64(toSend-batch) {
		t.Fatalf("Expected ack pending of %d and pending to be %d, got %d %d", batch*2, toSend-batch, info.NumAckPending, info.NumPending)
	}

	// Create a new pull based consumer.
	batch = 1
	msgs := make(chan *nats.Msg, 100)
	sub, err = js.ChanSubscribe("baz", msgs, nats.Durable("dlc"), nats.Pull(batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Since this sub is on 'baz' no messages are waiting for us to start.
	waitForPending(t, 0)

	// Now send in 10 messages to baz.
	for i := 0; i < toSend; i++ {
		js.Publish("baz", msg)
	}
	// We should get 1 queued up.
	waitForPending(t, batch)

	for received := 0; received < toSend; {
		select {
		case m := <-msgs:
			received++
			// This will do the AckNext version since it knows we are pull based.
			m.Ack()
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for messages")
		}
	}

	// Prevent invalid durable names
	if _, err := js.SubscribeSync("baz", nats.Durable("test.durable")); err != nats.ErrInvalidDurableName {
		t.Fatalf("Expected invalid durable name error")
	}

	ackWait := 1 * time.Millisecond
	sub, err = js.SubscribeSync("bar", nats.Durable("ack-wait"), nats.AckWait(ackWait))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	info, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.Config.AckWait != ackWait {
		t.Errorf("Expected %v, got %v", ackWait, info.Config.AckWait)
	}
}

func TestJetStreamAckPending_Pull(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
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

	const totalMsgs = 3
	for i := 0; i < totalMsgs; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	sub, err := js.SubscribeSync("foo",
		nats.Durable("dname-pull-ack-wait"),
		nats.AckWait(100*time.Millisecond),
		nats.MaxDeliver(5),
		nats.MaxAckPending(3),
		nats.Pull(15),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 3 messages delivered 5 times.
	expected := 15
	timeout := time.Now().Add(2 * time.Second)
	pending := 0
	for time.Now().Before(timeout) {
		if pending, _, _ = sub.Pending(); pending >= expected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pending < expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}

	got := info.NumRedelivered
	expected = 3
	if got < expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.NumAckPending
	expected = 3
	if got < expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.NumWaiting
	expected = 0
	if got != expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = int(info.NumPending)
	expected = 0
	if got != expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.Config.MaxAckPending
	expected = 3
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	got = info.Config.MaxDeliver
	expected = 5
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	acks := map[int]int{}

	ackPending := 3
	timeout = time.Now().Add(2 * time.Second)
	for time.Now().Before(timeout) {
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := info.NumAckPending, ackPending; got > 0 && got != want {
			t.Fatalf("unexpected num ack pending: got=%d, want=%d", got, want)
		}

		// Continue to ack all messages until no more pending.
		pending, _, _ = sub.Pending()
		if pending == 0 {
			break
		}

		m, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Error getting next message: %v", err)
		}

		if err := m.AckSync(); err != nil {
			t.Fatalf("Error on ack message: %v", err)
		}

		meta, err := m.MetaData()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		acks[int(meta.Stream)]++

		if ackPending != 0 {
			ackPending--
		}
		if int(meta.Pending) != ackPending {
			t.Errorf("Expected %v, got %v", ackPending, meta.Pending)
		}
	}

	got = len(acks)
	expected = 3
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, got)
	}

	expected = 5
	for _, got := range acks {
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
	}

	_, err = sub.NextMsg(100 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Errorf("Expected timeout, got: %v", err)
	}
}

func TestJetStreamAckPending_Push(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
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

	const totalMsgs = 3
	for i := 0; i < totalMsgs; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	sub, err := js.SubscribeSync("foo",
		nats.Durable("dname-wait"),
		nats.AckWait(100*time.Millisecond),
		nats.MaxDeliver(5),
		nats.MaxAckPending(3),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	// 3 messages delivered 5 times.
	expected := 15
	timeout := time.Now().Add(2 * time.Second)
	pending := 0
	for time.Now().Before(timeout) {
		if pending, _, _ = sub.Pending(); pending >= expected {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if pending < expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	info, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}

	got := info.NumRedelivered
	expected = 3
	if got < expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.NumAckPending
	expected = 3
	if got < expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.NumWaiting
	expected = 0
	if got != expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = int(info.NumPending)
	expected = 0
	if got != expected {
		t.Errorf("Expected %v, got: %v", expected, got)
	}

	got = info.Config.MaxAckPending
	expected = 3
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	got = info.Config.MaxDeliver
	expected = 5
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, pending)
	}

	acks := map[int]int{}

	ackPending := 3
	timeout = time.Now().Add(2 * time.Second)
	for time.Now().Before(timeout) {
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		if got, want := info.NumAckPending, ackPending; got > 0 && got != want {
			t.Fatalf("unexpected num ack pending: got=%d, want=%d", got, want)
		}

		// Continue to ack all messages until no more pending.
		pending, _, _ = sub.Pending()
		if pending == 0 {
			break
		}

		m, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Error getting next message: %v", err)
		}

		if err := m.AckSync(); err != nil {
			t.Fatalf("Error on ack message: %v", err)
		}

		meta, err := m.MetaData()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		acks[int(meta.Stream)]++

		if ackPending != 0 {
			ackPending--
		}
		if int(meta.Pending) != ackPending {
			t.Errorf("Expected %v, got %v", ackPending, meta.Pending)
		}
	}

	got = len(acks)
	expected = 3
	if got != expected {
		t.Errorf("Expected %v, got %v", expected, got)
	}

	expected = 5
	for _, got := range acks {
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
	}

	_, err = sub.NextMsg(100 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Errorf("Expected timeout, got: %v", err)
	}
}

func TestJetStream_Drain(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)

	nc, err := nats.Connect(s.ClientURL(), nats.ClosedHandler(func(_ *nats.Conn) {
		done()
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"drain"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	total := 500
	for i := 0; i < total; i++ {
		_, err := js.Publish("drain", []byte(fmt.Sprintf("i:%d", i)))
		if err != nil {
			t.Error(err)
		}
	}

	// Create some consumers and ensure that there are no timeouts.
	errCh := make(chan error, 2048)
	createSub := func(name string) (*nats.Subscription, error) {
		return js.Subscribe("drain", func(m *nats.Msg) {
			err := m.AckSync()
			if err != nil {
				errCh <- err
			}
		}, nats.Durable(name), nats.ManualAck())
	}

	subA, err := createSub("A")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subB, err := createSub("B")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subC, err := createSub("C")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	subD, err := createSub("D")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	waitForDelivered := func(t *testing.T, sub *nats.Subscription) {
		t.Helper()
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _ := sub.Delivered(); msgs != 0 {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	waitForDelivered(t, subA)
	waitForDelivered(t, subB)
	waitForDelivered(t, subC)
	waitForDelivered(t, subD)
	nc.Drain()

	select {
	case err := <-errCh:
		t.Fatalf("Error during drain: %+v", err)
	case <-ctx.Done():
		// OK!
	}
}

func TestAckForNonJetStream(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	sub, _ := nc.SubscribeSync("foo")
	nc.PublishRequest("foo", "_INBOX_", []byte("OK"))
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := m.Ack(); err != nil {
		t.Fatalf("Expected no errors, got '%v'", err)
	}
}

func TestJetStreamManagement(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	var si *nats.StreamInfo
	t.Run("create stream", func(t *testing.T) {
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
	})

	for i := 0; i < 25; i++ {
		js.Publish("foo", []byte("hi"))
	}

	t.Run("stream info", func(t *testing.T) {
		si, err = js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}
	})

	t.Run("stream update", func(t *testing.T) {
		if _, err := js.UpdateStream(nil); err == nil {
			t.Fatal("Unexpected success")
		}
		prevMaxMsgs := si.Config.MaxMsgs
		si, err = js.UpdateStream(&nats.StreamConfig{Name: "foo", MaxMsgs: prevMaxMsgs + 100})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" || si.Config.MaxMsgs == prevMaxMsgs {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}
	})

	t.Run("create consumer", func(t *testing.T) {
		ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci == nil || ci.Name != "dlc" || ci.Stream != "foo" {
			t.Fatalf("ConsumerInfo is not correct %+v", ci)
		}

		if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "test.durable"}); err != nats.ErrInvalidDurableName {
			t.Fatalf("Expected invalid durable name error")
		}
	})

	t.Run("consumer info", func(t *testing.T) {
		ci, err := js.ConsumerInfo("foo", "dlc")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ci == nil || ci.Config.Durable != "dlc" {
			t.Fatalf("ConsumerInfo is not correct %+v", si)
		}
	})

	t.Run("list streams", func(t *testing.T) {
		sl := js.NewStreamLister()
		if !sl.Next() {
			if err := sl.Err(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			t.Fatalf("Unexpected stream lister next")
		}
		if p := sl.Page(); len(p) != 1 || p[0].Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", p)
		}
		if err := sl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("list consumers", func(t *testing.T) {
		if cl := js.NewConsumerLister(""); cl.Next() {
			t.Fatalf("Unexpected next ok")
		} else if err := cl.Err(); err == nil {
			if cl.Next() {
				t.Fatalf("Unexpected next ok")
			}
			t.Fatalf("Unexpected nil error")
		}

		cl := js.NewConsumerLister("foo")
		if !cl.Next() {
			if err := cl.Err(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			t.Fatalf("Unexpected consumer lister next")
		}
		if p := cl.Page(); len(p) != 1 || p[0].Stream != "foo" || p[0].Config.Durable != "dlc" {
			t.Fatalf("ConsumerInfo is not correct %+v", p)
		}
		if err := cl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	})

	t.Run("list consumer names", func(t *testing.T) {
		var names []string
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for name := range js.ConsumerNames(ctx, "foo") {
			names = append(names, name)
		}
		if got, want := len(names), 1; got != want {
			t.Fatalf("Unexpected names, got=%d, want=%d", got, want)
		}
	})

	t.Run("delete consumers", func(t *testing.T) {
		if err := js.DeleteConsumer("", ""); err == nil {
			t.Fatalf("Unexpected success")
		}
		if err := js.DeleteConsumer("foo", "dlc"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("purge stream", func(t *testing.T) {
		if err := js.PurgeStream("foo"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si, err := js.StreamInfo("foo"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		} else if si.State.Msgs != 0 {
			t.Fatalf("StreamInfo.Msgs is not correct")
		}
	})

	t.Run("list stream names", func(t *testing.T) {
		var names []string
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for name := range js.StreamNames(ctx) {
			names = append(names, name)
		}
		if got, want := len(names), 1; got != want {
			t.Fatalf("Unexpected names, got=%d, want=%d", got, want)
		}
	})

	t.Run("delete stream", func(t *testing.T) {
		if err := js.DeleteStream(""); err == nil {
			t.Fatal("Unexpected success")
		}
		if err := js.DeleteStream("foo"); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.StreamInfo("foo"); err == nil {
			t.Fatalf("Unexpected success")
		}
	})

	t.Run("fetch account info", func(t *testing.T) {
		info, err := js.AccountInfo()
		if err != nil {
			t.Fatal(err)
		}
		if info.Limits.MaxMemory < 1 {
			t.Errorf("Expected to have memory limits, got: %v", info.Limits.MaxMemory)
		}
		if info.Limits.MaxStore < 1 {
			t.Errorf("Expected to have disk limits, got: %v", info.Limits.MaxMemory)
		}
		if info.Limits.MaxStreams != -1 {
			t.Errorf("Expected to not have stream limits, got: %v", info.Limits.MaxStreams)
		}
		if info.Limits.MaxConsumers != -1 {
			t.Errorf("Expected to not have consumer limits, got: %v", info.Limits.MaxConsumers)
		}
		if info.API.Total != 15 {
			t.Errorf("Expected 15 API calls, got: %v", info.API.Total)
		}
		if info.API.Errors != 1 {
			t.Errorf("Expected 11 API error, got: %v", info.API.Errors)
		}
	})
}

func TestJetStreamManagement_GetMsg(t *testing.T) {
	t.Run("1-node", func(t *testing.T) {
		withJSServer(t, testJetStreamManagement_GetMsg)
	})
	t.Run("3-node", func(t *testing.T) {
		withJSCluster(t, "GET", 3, testJetStreamManagement_GetMsg)
	})
}

func testJetStreamManagement_GetMsg(t *testing.T, srvs ...*jsServer) {
	s := srvs[0]
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 5; i++ {
		msg := nats.NewMsg("foo.A")
		data := fmt.Sprintf("A:%d", i)
		msg.Data = []byte(data)
		msg.Header.Add("X-Nats-Test-Data", data)
		js.PublishMsg(msg)
		js.Publish("foo.B", []byte(fmt.Sprintf("B:%d", i)))
		js.Publish("foo.C", []byte(fmt.Sprintf("C:%d", i)))
	}

	var originalSeq uint64
	t.Run("get message", func(t *testing.T) {
		expected := 5
		msgs := make([]*nats.Msg, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		sub, err := js.Subscribe("foo.C", func(msg *nats.Msg) {
			msgs = append(msgs, msg)
			if len(msgs) == expected {
				cancel()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		<-ctx.Done()
		sub.Unsubscribe()

		got := len(msgs)
		if got != expected {
			t.Fatalf("Expected: %d, got: %d", expected, got)
		}

		msg := msgs[3]
		meta, err := msg.MetaData()
		if err != nil {
			t.Fatal(err)
		}
		originalSeq = meta.Stream

		// Get the same message using JSM.
		fetchedMsg, err := js.GetMsg("foo", originalSeq)
		if err != nil {
			t.Fatal(err)
		}

		expectedData := "C:3"
		if string(fetchedMsg.Data) != expectedData {
			t.Errorf("Expected: %v, got: %v", expectedData, string(fetchedMsg.Data))
		}
	})

	t.Run("get deleted message", func(t *testing.T) {
		err := js.DeleteMsg("foo", originalSeq)
		if err != nil {
			t.Fatal(err)
		}

		si, err := js.StreamInfo("foo")
		if err != nil {
			t.Fatal(err)
		}
		expected := 14
		if int(si.State.Msgs) != expected {
			t.Errorf("Expected %d msgs, got: %d", expected, si.State.Msgs)
		}

		// There should be only 4 messages since one deleted.
		expected = 4
		msgs := make([]*nats.Msg, 0)
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		sub, err := js.Subscribe("foo.C", func(msg *nats.Msg) {
			msgs = append(msgs, msg)

			if len(msgs) == expected {
				cancel()
			}
		})
		if err != nil {
			t.Fatal(err)
		}
		<-ctx.Done()
		sub.Unsubscribe()

		msg := msgs[3]
		meta, err := msg.MetaData()
		if err != nil {
			t.Fatal(err)
		}
		newSeq := meta.Stream

		// First message removed
		if newSeq <= originalSeq {
			t.Errorf("Expected %d to be higher sequence than %d",
				newSeq, originalSeq)
		}

		// Try to fetch the same message which should be gone.
		_, err = js.GetMsg("foo", originalSeq)
		if err == nil || err.Error() != `deleted message` {
			t.Errorf("Expected deleted message error, got: %v", err)
		}
	})

	t.Run("get message with headers", func(t *testing.T) {
		streamMsg, err := js.GetMsg("foo", 4)
		if err != nil {
			t.Fatal(err)
		}
		if streamMsg.Sequence != 4 {
			t.Errorf("Expected %v, got: %v", 4, streamMsg.Sequence)
		}
		expectedMap := map[string][]string{
			"X-Nats-Test-Data": {"A:1"},
		}
		if !reflect.DeepEqual(streamMsg.Header, http.Header(expectedMap)) {
			t.Errorf("Expected %v, got: %v", expectedMap, streamMsg.Header)
		}
	})
}

func TestJetStreamManagement_DeleteMsg(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 5; i++ {
		js.Publish("foo.A", []byte("A"))
		js.Publish("foo.B", []byte("B"))
		js.Publish("foo.C", []byte("C"))
	}

	si, err := js.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	var total uint64 = 15
	if si.State.Msgs != total {
		t.Errorf("Expected %d msgs, got: %d", total, si.State.Msgs)
	}

	expected := 5
	msgs := make([]*nats.Msg, 0)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err := js.Subscribe("foo.C", func(msg *nats.Msg) {
		msgs = append(msgs, msg)
		if len(msgs) == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sub.Unsubscribe()

	got := len(msgs)
	if got != expected {
		t.Fatalf("Expected %d, got %d", expected, got)
	}

	msg := msgs[0]
	meta, err := msg.MetaData()
	if err != nil {
		t.Fatal(err)
	}
	originalSeq := meta.Stream

	err = js.DeleteMsg("foo", originalSeq)
	if err != nil {
		t.Fatal(err)
	}

	si, err = js.StreamInfo("foo")
	if err != nil {
		t.Fatal(err)
	}
	total = 14
	if si.State.Msgs != total {
		t.Errorf("Expected %d msgs, got: %d", total, si.State.Msgs)
	}

	// There should be only 4 messages since one deleted.
	expected = 4
	msgs = make([]*nats.Msg, 0)
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	sub, err = js.Subscribe("foo.C", func(msg *nats.Msg) {
		msgs = append(msgs, msg)

		if len(msgs) == expected {
			cancel()
		}
	})
	if err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	sub.Unsubscribe()

	msg = msgs[0]
	meta, err = msg.MetaData()
	if err != nil {
		t.Fatal(err)
	}
	newSeq := meta.Stream

	// First message removed
	if newSeq <= originalSeq {
		t.Errorf("Expected %d to be higher sequence than %d", newSeq, originalSeq)
	}
}

func TestJetStreamImport(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [ { service: "$JS.API.>" },  { service: "foo" }]
			},
			U: {
				users: [ {user: rip, password: bar} ]
				imports [
					{ service: { subject: "$JS.API.>", account: JS } , to: "dlc.>" }
					{ service: { subject: "foo", account: JS } }
				]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	// Create a stream using JSM.
	ncm, err := nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "foo"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ncm.Close()

	jsm, err := ncm.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	_, err = jsm.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	// Client with the imports.
	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// Since we import with a prefix from above we can use that when creating our JS context.
	js, err := nc.JetStream(nats.APIPrefix("dlc"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg := []byte("Hello JS Import!")

	if _, err = js.Publish("foo", msg); err != nil {
		t.Fatalf("Unexpected publish error: %v", err)
	}
}

func TestJetStreamImportDirectOnly(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: dlc, password: foo} ]
				exports [
					# For the stream publish.
					{ service: "ORDERS" }
					# For the pull based consumer. Response type needed for batchsize > 1
					{ service: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", response: stream }
					# For the push based consumer delivery and ack.
					{ stream: "p.d" }
					{ stream: "p.d3" }
					# For the acks. Service in case we want an ack to our ack.
					{ service: "$JS.ACK.ORDERS.*.>" }
				]
			},
			U: {
				users: [ {user: rip, password: bar} ]
				imports [
					{ service: { subject: "ORDERS", account: JS } , to: "orders" }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", account: JS } }
					{ stream:  { subject: "p.d", account: JS } }
					{ stream:  { subject: "p.d3", account: JS } }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
				]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	// Create a stream using JSM.
	ncm, err := nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "foo"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer ncm.Close()

	jsm, err := ncm.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	// Create a stream using the server directly.
	_, err = jsm.AddStream(&nats.StreamConfig{Name: "ORDERS"})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	// Create a pull based consumer.
	_, err = jsm.AddConsumer("ORDERS", &nats.ConsumerConfig{Durable: "d1", AckPolicy: nats.AckExplicitPolicy})
	if err != nil {
		t.Fatalf("pull consumer create failed: %v", err)
	}

	// Create a push based consumers.
	_, err = jsm.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:        "d2",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "p.d",
	})
	if err != nil {
		t.Fatalf("push consumer create failed: %v", err)
	}

	_, err = jsm.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:        "d3",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "p.d3",
	})
	if err != nil {
		t.Fatalf("push consumer create failed: %v", err)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.DirectOnly())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Now make sure we can send to the stream.
	toSend := 100
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("orders", []byte(fmt.Sprintf("ORDER-%d", i+1))); err != nil {
			t.Fatalf("Unexpected error publishing message %d: %v", i+1, err)
		}
	}

	// Check for correct errors.
	if _, err := js.SubscribeSync("ORDERS"); err != nats.ErrDirectModeRequired {
		t.Fatalf("Expected an error of '%v', got '%v'", nats.ErrDirectModeRequired, err)
	}

	var sub *nats.Subscription

	waitForPending := func(t *testing.T, n int) {
		t.Helper()
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	// Do push based consumer using a regular NATS subscription on the import subject.
	sub, err = nc.SubscribeSync("p.d3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	waitForPending(t, toSend)

	// Can also ack from the regular NATS subscription via the imported subject.
	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Test that can expect an ack of the ack.
		err = m.AckSync()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	// Now pull based consumer.
	batch := 10
	sub, err = js.SubscribeSync("ORDERS", nats.PullDirect("ORDERS", "d1", batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	waitForPending(t, batch)

	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(100 * time.Millisecond)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Tests that acks flow since we need these to do AckNext for this to work.
		err = m.Ack()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}
}

func TestJetStreamCrossAccountMirrorsAndSources(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		no_auth_user: rip
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		accounts {
			JS {
				jetstream: enabled
				users = [ { user: "rip", pass: "pass" } ]
				exports [
					{ service: "$JS.API.CONSUMER.>" } # To create internal consumers to mirror/source.
					{ stream: "RI.DELIVER.SYNC.>" }   # For the mirror/source consumers sending to IA via delivery subject.
				]
			}
			IA {
				jetstream: enabled
				users = [ { user: "dlc", pass: "pass" } ]
				imports [
					{ service: { account: JS, subject: "$JS.API.CONSUMER.>"}, to: "RI.JS.API.CONSUMER.>" }
					{ stream: { account: JS, subject: "RI.DELIVER.SYNC.>"} }
				]
			}
			$SYS { users = [ { user: "admin", pass: "s3cr3t!" } ] }
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc1, err := nats.Connect(s.ClientURL(), nats.UserInfo("rip", "pass"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc1.Close()
	js1, err := nc1.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	_, err = js1.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 2,
	})
	if err != nil {
		t.Fatal(err)
	}

	toSend := 100
	for i := 0; i < toSend; i++ {
		if _, err := js1.Publish("TEST", []byte("OK")); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("dlc", "pass"))
	if err != nil {
		t.Fatal(err)
	}
	defer nc2.Close()
	js2, err := nc1.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	checkMsgCount := func(t *testing.T, js nats.JetStreamManager, timeout time.Duration, want int) {
		t.Helper()

		deadline := time.Now().Add(timeout)
		var loopErr error
		for time.Now().Before(deadline) {
			si, err := js2.StreamInfo("MY_MIRROR_TEST")
			if err != nil {
				loopErr = err
				continue
			}
			loopErr = nil

			if got := int(si.State.Msgs); got != want {
				loopErr = fmt.Errorf("Unexpected msg count, got %d, want %d", got, want)
				continue
			}
			loopErr = nil
			break
		}
		if loopErr != nil {
			t.Fatal(loopErr)
		}
	}

	_, err = js2.AddStream(&nats.StreamConfig{
		Name:    "MY_MIRROR_TEST",
		Storage: nats.FileStorage,
		Mirror: &nats.StreamSource{
			Name: "TEST",
			External: &nats.ExternalStream{
				APIPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.MIRRORS",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkMsgCount(t, js2, 2*time.Second, toSend)

	_, err = js2.AddStream(&nats.StreamConfig{
		Name:    "MY_SOURCE_TEST",
		Storage: nats.FileStorage,
		Sources: []*nats.StreamSource{
			&nats.StreamSource{
				Name: "TEST",
				External: &nats.ExternalStream{
					APIPrefix:     "RI.JS.API",
					DeliverPrefix: "RI.DELIVER.SYNC.SOURCES",
				},
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkMsgCount(t, js2, 2*time.Second, toSend)
}

func TestJetStreamAutoMaxAckPending(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{Name: "foo"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 10_000

	msg := []byte("Hello")
	for i := 0; i < toSend; i++ {
		// Use plain NATS here for speed.
		nc.Publish("foo", msg)
	}
	nc.Flush()

	// Create a consumer.
	msgs := make(chan *nats.Msg, 500)
	sub, err := js.ChanSubscribe("foo", msgs)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectedMaxAck, _, _ := sub.PendingLimits()

	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.MaxAckPending != expectedMaxAck {
		t.Fatalf("Expected MaxAckPending to be set to %d, got %d", expectedMaxAck, ci.Config.MaxAckPending)
	}

	waitForPending := func(n int) {
		timeout := time.Now().Add(2 * time.Second)
		for time.Now().Before(timeout) {
			if msgs, _, _ := sub.Pending(); msgs == n {
				return
			}
			time.Sleep(10 * time.Millisecond)
		}
		msgs, _, _ := sub.Pending()
		t.Fatalf("Expected to receive %d messages, but got %d", n, msgs)
	}

	waitForPending(expectedMaxAck)
	// We do it twice to make sure it does not go over.
	waitForPending(expectedMaxAck)

	// Now make sure we can consume them all with no slow consumers etc.
	for i := 0; i < toSend; i++ {
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error receiving %d: %v", i+1, err)
		}
		m.Ack()
	}
}

func TestJetStreamInterfaces(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	var js nats.JetStream
	var jsm nats.JetStreamManager
	var jsctx nats.JetStreamContext

	// JetStream that can publish/subscribe but cannot manage streams.
	js, err = nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	js.Publish("foo", []byte("hello"))

	// JetStream context that can manage streams/consumers but cannot produce messages.
	jsm, err = nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsm.AddStream(&nats.StreamConfig{Name: "FOO"})

	// JetStream context that can both manage streams/consumers
	// as well as publish/subscribe.
	jsctx, err = nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsctx.AddStream(&nats.StreamConfig{Name: "BAR"})
	jsctx.Publish("bar", []byte("hello world"))

	publishMsg := func(js nats.JetStream, payload []byte) {
		js.Publish("foo", payload)
	}
	publishMsg(js, []byte("hello world"))
}

// WIP(dlc) - This is in support of stall based tests and processing.
func TestJetStreamPullBasedStall(t *testing.T) {
	t.SkipNow()

	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: enabled
		no_auth_user: pc
		accounts: {
			JS: {
				jetstream: enabled
				users: [ {user: pc, password: foo} ]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create a stream.
	if _, err = js.AddStream(&nats.StreamConfig{Name: "STALL"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.StreamInfo("STALL")
	if err != nil {
		t.Fatalf("stream lookup failed: %v", err)
	}

	msg := []byte("Hello JS!")
	toSend := 100_000
	for i := 0; i < toSend; i++ {
		// Use plain NATS here for speed.
		nc.Publish("STALL", msg)
	}
	nc.Flush()

	batch := 100
	msgs := make(chan *nats.Msg, batch-2)
	sub, err := js.ChanSubscribe("STALL", msgs, nats.Durable("dlc"), nats.Pull(batch))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()

	for received := 0; received < toSend; {
		select {
		case m := <-msgs:
			received++
			meta, _ := m.MetaData()
			if meta.Consumer != uint64(received) {
				t.Fatalf("Missed something, wanted %d but got %d", received, meta.Consumer)
			}
			m.Ack()
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for messages, last received was %d", received)
		}
	}
}

func TestJetStreamSubscribe_DeliverPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var publishTime time.Time

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		if i == 5 {
			publishTime = time.Now()
		}
		js.Publish("foo", []byte(payload))
	}

	for _, test := range []struct {
		name     string
		subopt   nats.SubOpt
		expected int
	}{
		{
			"deliver.all", nats.DeliverAll(), 10,
		},
		{
			"deliver.last", nats.DeliverLast(), 1,
		},
		{
			"deliver.new", nats.DeliverNew(), 0,
		},
		{
			"deliver.starttime", nats.StartTime(publishTime), 5,
		},
		{
			"deliver.startseq", nats.StartSequence(6), 5,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			got := 0
			sub, err := js.Subscribe("foo", func(m *nats.Msg) {
				got++
				if got == test.expected {
					cancel()
				}
			}, test.subopt)

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			<-ctx.Done()
			sub.Drain()

			if got != test.expected {
				t.Fatalf("Expected %d, got %d", test.expected, got)
			}
		})
	}
}

func TestJetStreamSubscribe_AckPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish("foo", []byte(payload))
	}

	for _, test := range []struct {
		name     string
		subopt   nats.SubOpt
		expected nats.AckPolicy
	}{
		{
			"ack-none", nats.AckNone(), nats.AckNonePolicy,
		},
		{
			"ack-all", nats.AckAll(), nats.AckAllPolicy,
		},
		{
			"ack-explicit", nats.AckExplicit(), nats.AckExplicitPolicy,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			got := 0
			totalMsgs := 10
			sub, err := js.Subscribe("foo", func(m *nats.Msg) {
				got++
				if got == totalMsgs {
					cancel()
				}
			}, test.subopt, nats.Durable(test.name))

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			<-ctx.Done()
			sub.Drain()

			if got != totalMsgs {
				t.Fatalf("Expected %d, got %d", totalMsgs, got)
			}

			ci, err := js.ConsumerInfo("TEST", test.name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci.Config.AckPolicy != test.expected {
				t.Fatalf("Expected %v, got %v", test.expected, ci.Config.AckPolicy)
			}
		})
	}

	checkAcks := func(t *testing.T, sub *nats.Subscription) {
		// Normal Ack
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		meta, err := msg.MetaData()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if meta.Consumer != 1 || meta.Stream != 1 || meta.Delivered != 1 {
			t.Errorf("Unexpected metadata: %v", meta)
		}

		got := string(msg.Data)
		expected := "i:0"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.Ack()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// AckSync
		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:1"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}

		// Give an already canceled context.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		err = msg.AckSync(nats.Context(ctx))
		if err != context.Canceled {
			t.Errorf("Unexpected error: %v", err)
		}

		// Context that not yet canceled.
		ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()

		err = msg.AckSync(nats.Context(ctx))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.AckSync(nats.AckWait(2 * time.Second))
		if err != nats.ErrInvalidJSAck {
			t.Errorf("Unexpected error: %v", err)
		}

		// AckSync default
		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:2"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.AckSync()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		// Nak
		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:3"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		// Skip the message.
		err = msg.Nak()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:4"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.Nak()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:5"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.Term()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		msg, err = sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:6"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.InProgress()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		err = msg.InProgress()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		err = msg.Ack()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	t.Run("js sub ack", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo", nats.Durable("wq2"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		checkAcks(t, sub)
	})

	t.Run("non js sub ack", func(t *testing.T) {
		inbox := nats.NewInbox()
		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Durable:        "wq",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverPolicy:  nats.DeliverAllPolicy,
			DeliverSubject: inbox,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		checkAcks(t, sub)
	})
}

func TestJetStreamSubscribe_AckDup(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("hello"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	pings := make(chan struct{}, 6)
	nc.Subscribe("$JS.ACK.TEST.>", func(msg *nats.Msg) {
		pings <- struct{}{}
	})
	nc.Flush()

	ch := make(chan error, 6)
	_, err = js.Subscribe("foo", func(m *nats.Msg) {
		// Only first ack will be sent, auto ack that will occur after
		// this won't be sent either.
		ch <- m.Ack()

		// Any following acks should fail.
		ch <- m.Ack()
		ch <- m.Nak()
		ch <- m.AckSync()
		ch <- m.Term()
		ch <- m.InProgress()
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	<-ctx.Done()
	ackErr1 := <-ch
	if ackErr1 != nil {
		t.Errorf("Unexpected error: %v", ackErr1)
	}

	for i := 0; i < 5; i++ {
		e := <-ch
		if e != nats.ErrInvalidJSAck {
			t.Errorf("Expected error: %v", e)
		}
	}
	if len(pings) != 1 {
		t.Logf("Expected to receive a single ack, got: %v", len(pings))
	}
}

func TestJetStreamSubscribe_AckDupInProgress(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js.Publish("foo", []byte("hello"))

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	pings := make(chan struct{}, 3)
	nc.Subscribe("$JS.ACK.TEST.>", func(msg *nats.Msg) {
		pings <- struct{}{}
	})
	nc.Flush()

	ch := make(chan error, 3)
	_, err = js.Subscribe("foo", func(m *nats.Msg) {
		// InProgress ACK can be sent any number of times.
		ch <- m.InProgress()
		ch <- m.InProgress()
		ch <- m.Ack()
	}, nats.Durable("WQ"), nats.ManualAck())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	<-ctx.Done()
	ackErr1 := <-ch
	ackErr2 := <-ch
	ackErr3 := <-ch
	if ackErr1 != nil {
		t.Errorf("Unexpected error: %v", ackErr1)
	}
	if ackErr2 != nil {
		t.Errorf("Unexpected error: %v", ackErr2)
	}
	if ackErr3 != nil {
		t.Errorf("Unexpected error: %v", ackErr3)
	}
	if len(pings) != 3 {
		t.Logf("Expected to receive multiple acks, got: %v", len(pings))
	}
}

func TestJetStream_Unsubscribe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchConsumers := func(t *testing.T, expected int) []*nats.ConsumerInfo {
		t.Helper()
		cl := js.NewConsumerLister("foo")
		if !cl.Next() {
			if err := cl.Err(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			t.Fatalf("Unexpected consumer lister next")
		}
		p := cl.Page()
		if len(p) != expected {
			t.Fatalf("Expected %d consumers, got: %d", expected, len(p))
		}
		if err := cl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		return p
	}

	js.Publish("foo.A", []byte("A"))
	js.Publish("foo.B", []byte("B"))
	js.Publish("foo.C", []byte("C"))

	t.Run("consumers deleted on unsubscribe", func(t *testing.T) {
		subA, err := js.SubscribeSync("foo.A")
		if err != nil {
			t.Fatal(err)
		}
		err = subA.Unsubscribe()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		subB, err := js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}
		err = subB.Unsubscribe()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 0)
	})

	t.Run("attached pull consumer deleted on unsubscribe", func(t *testing.T) {
		// Created by JetStreamManagement
		if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:   "wq",
			AckPolicy: nats.AckExplicitPolicy,
			// Need to specify filter subject here otherwise
			// would get messages from foo.A as well.
			FilterSubject: "foo.C",
		}); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		subC, err := js.SubscribeSync("foo.C", nats.Durable("wq"), nats.Pull(1))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 1)

		msg, err := subC.NextMsg(2 * time.Second)
		if err != nil {
			t.Errorf("Unexpected error getting message: %v", err)
		}
		got := string(msg.Data)
		expected := "C"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		subC.Unsubscribe()
		fetchConsumers(t, 0)
	})

	t.Run("ephemeral consumers deleted on drain", func(t *testing.T) {
		subA, err := js.SubscribeSync("foo.A")
		if err != nil {
			t.Fatal(err)
		}
		err = subA.Drain()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 0)
	})

	t.Run("durable consumers not deleted on drain", func(t *testing.T) {
		subB, err := js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}
		err = subB.Drain()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 1)
	})

	t.Run("reattached durable consumers not deleted on drain", func(t *testing.T) {
		subB, err := js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}
		err = subB.Drain()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 1)
	})
}

func TestJetStream_UnsubscribeCloseDrain(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	serverURL := s.ClientURL()
	mc, err := nats.Connect(serverURL)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsm, err := mc.JetStream()
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	_, err = jsm.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchConsumers := func(t *testing.T, expected int) []*nats.ConsumerInfo {
		t.Helper()
		cl := jsm.NewConsumerLister("foo")
		if !cl.Next() {
			if err := cl.Err(); err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			t.Fatalf("Unexpected consumer lister next")
		}
		p := cl.Page()
		if len(p) != expected {
			t.Fatalf("Expected %d consumers, got: %d", expected, len(p))
		}
		if err := cl.Err(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		return p
	}

	t.Run("conn drain deletes ephemeral consumers", func(t *testing.T) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		nc, err := nats.Connect(serverURL, nats.ClosedHandler(func(_ *nats.Conn) {
			cancel()
		}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.SubscribeSync("foo.C")
		if err != nil {
			t.Fatal(err)
		}

		// sub.Drain() or nc.Drain() does not delete the durable consumers,
		// just makes client go away.  Ephemerals will get deleted though.
		nc.Drain()
		<-ctx.Done()
		fetchConsumers(t, 0)
	})

	jsm.Publish("foo.A", []byte("A.1"))
	jsm.Publish("foo.B", []byte("B.1"))
	jsm.Publish("foo.C", []byte("C.1"))

	t.Run("conn close does not delete any consumer", func(t *testing.T) {
		nc, err := nats.Connect(serverURL)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.SubscribeSync("foo.A")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		subB, err := js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		resp, err := subB.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		got := string(resp.Data)
		expected := "B.1"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
		fetchConsumers(t, 2)

		// There will be still all consumers since nc.Close
		// does not delete ephemeral consumers.
		nc.Close()
		fetchConsumers(t, 2)
	})

	jsm.Publish("foo.A", []byte("A.2"))
	jsm.Publish("foo.B", []byte("B.2"))
	jsm.Publish("foo.C", []byte("C.2"))

	t.Run("reattached durables consumers can be deleted with unsubscribe", func(t *testing.T) {
		nc, err := nats.Connect(serverURL)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		fetchConsumers(t, 2)

		// The durable interest remains so have to attach now,
		// otherwise would get a stream already used error.
		subB, err := js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}

		// No new consumers created since reattached to the same one.
		fetchConsumers(t, 2)

		resp, err := subB.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		got := string(resp.Data)
		expected := "B.2"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}

		jsm.Publish("foo.B", []byte("B.3"))

		// Attach again to the same subject with the durable.
		dupSub, err := js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}

		// The same durable is already used, so this dup durable
		// subscription won't receive the message.
		_, err = dupSub.NextMsg(1 * time.Second)
		if err == nil {
			t.Fatalf("Expected error: %v", err)
		}

		// Original sub can still receive the same message.
		resp, err = subB.NextMsg(1 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		got = string(resp.Data)
		expected = "B.3"
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
		// Delete durable consumer.
		err = subB.Unsubscribe()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		err = dupSub.Unsubscribe()
		if err == nil {
			t.Fatalf("Unexpected success")
		}
		if err.Error() != `consumer not found` {
			t.Errorf("Expected consumer not found error, got: %v", err)
		}

		// Remains an ephemeral consumer that did not get deleted
		// when Close() was called.
		fetchConsumers(t, 1)
	})
}

func TestJetStream_UnsubscribeDeleteNoPermissions(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
		no_auth_user: guest
		accounts: {
			JS: {   # User should not be able to delete consumer.
				jetstream: enabled
				users: [ {user: guest, password: "", permissions: {
					publish: { deny: "$JS.API.CONSUMER.DELETE.>" }
				}}]
			}
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	if config := s.JetStreamConfig(); config != nil {
		defer os.RemoveAll(config.StoreDir)
	}

	errCh := make(chan error, 2)
	nc, err := nats.Connect(s.ClientURL(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errCh <- err
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	js.AddStream(&nats.StreamConfig{
		Name: "foo",
	})
	js.Publish("foo", []byte("test"))

	sub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}

	_, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatal(err)
	}

	// Should fail due to lack of permissions.
	err = sub.Unsubscribe()
	if err == nil {
		t.Errorf("Unexpected success attempting to delete consumer without permissions")
	}

	select {
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for permissions error")
	case err = <-errCh:
		if !strings.Contains(err.Error(), `Permissions Violation for Publish to "$JS.API.CONSUMER.DELETE`) {
			t.Error("Expected permissions violation error")
		}
	}
}

type jsServer struct {
	*server.Server
	myopts  *server.Options
	restart sync.Mutex
}

// Restart can be used to start again a server
// using the same listen address as before.
func (srv *jsServer) Restart() {
	srv.restart.Lock()
	defer srv.restart.Unlock()
	srv.Server = natsserver.RunServer(srv.myopts)
}

func setupJSClusterWithSize(t *testing.T, clusterName string, size int) []*jsServer {
	t.Helper()
	nodes := make([]*jsServer, size)
	opts := make([]*server.Options, 0)

	getAddr := func() (string, string, int) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			panic(err)
		}
		defer l.Close()

		addr := l.Addr()
		host := addr.(*net.TCPAddr).IP.String()
		port := addr.(*net.TCPAddr).Port
		l.Close()
		time.Sleep(100 * time.Millisecond)
		return addr.String(), host, port
	}

	routes := []string{}
	for i := 0; i < size; i++ {
		o := natsserver.DefaultTestOptions
		o.JetStream = true
		o.ServerName = fmt.Sprintf("NODE_%d", i)
		tdir, err := ioutil.TempDir(os.TempDir(), fmt.Sprintf("%s_%s-", o.ServerName, clusterName))
		if err != nil {
			t.Fatal(err)
		}
		o.StoreDir = tdir
		o.Cluster.Name = clusterName
		_, host1, port1 := getAddr()
		o.Host = host1
		o.Port = port1

		addr2, host2, port2 := getAddr()
		o.Cluster.Host = host2
		o.Cluster.Port = port2
		o.Tags = []string{o.ServerName}
		routes = append(routes, fmt.Sprintf("nats://%s", addr2))
		opts = append(opts, &o)
	}

	routesStr := server.RoutesFromStr(strings.Join(routes, ","))

	for i, o := range opts {
		o.Routes = routesStr
		nodes[i] = &jsServer{Server: natsserver.RunServer(o), myopts: o}
	}

	// Wait until JS is ready.
	srvA := nodes[0]
	nc, err := nats.Connect(srvA.ClientURL())
	if err != nil {
		t.Error(err)
	}
	waitForJSReady(t, nc)

	return nodes
}

func withJSServer(t *testing.T, tfn func(t *testing.T, srvs ...*jsServer)) {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	s := &jsServer{Server: RunServerWithOptions(opts), myopts: &opts}

	defer func() {
		if config := s.JetStreamConfig(); config != nil {
			os.RemoveAll(config.StoreDir)
		}
		s.Shutdown()
	}()
	tfn(t, s)
}

func withJSCluster(t *testing.T, clusterName string, size int, tfn func(t *testing.T, srvs ...*jsServer)) {
	t.Helper()

	nodes := setupJSClusterWithSize(t, clusterName, size)
	defer func() {
		// Ensure that they get shutdown and remove their state.
		for _, node := range nodes {
			node.restart.Lock()
			if config := node.JetStreamConfig(); config != nil {
				os.RemoveAll(config.StoreDir)
			}
			node.restart.Unlock()
			node.Shutdown()
		}
	}()
	tfn(t, nodes...)
}

func withJSClusterAndStream(t *testing.T, clusterName string, size int, stream *nats.StreamConfig, tfn func(t *testing.T, subject string, srvs ...*jsServer)) {
	t.Helper()

	withJSCluster(t, clusterName, size, func(t *testing.T, nodes ...*jsServer) {
		srvA := nodes[0]
		nc, err := nats.Connect(srvA.ClientURL())
		if err != nil {
			t.Error(err)
		}

		var jsm nats.JetStreamManager
		jsm, err = nc.JetStream()
		if err != nil {
			t.Fatal(err)
		}

		timeout := time.Now().Add(10 * time.Second)
		for time.Now().Before(timeout) {
			_, err = jsm.AddStream(stream)
			if err != nil {
				t.Logf("WARN: Got error while trying to create stream: %v", err)
				// Backoff for a bit until cluster and resources ready.
				time.Sleep(500 * time.Millisecond)
				continue
			}
			break
		}
		if err != nil {
			t.Fatalf("Unexpected error creating stream: %v", err)
		}

		tfn(t, stream.Name, nodes...)
	})
}

func waitForJSReady(t *testing.T, nc *nats.Conn) {
	var err error
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		_, err = nc.JetStream()
		if err != nil {
			// Backoff for a bit until cluster ready.
			time.Sleep(250 * time.Millisecond)
			continue
		}
		return
	}
	t.Fatalf("Timeout waiting for JS to be ready: %v", err)
}

func TestJetStream_ClusterPlacement(t *testing.T) {
	size := 3

	t.Run("default cluster", func(t *testing.T) {
		cluster := "PLC1"
		withJSCluster(t, cluster, size, func(t *testing.T, nodes ...*jsServer) {
			srvA := nodes[0]
			nc, err := nats.Connect(srvA.ClientURL())
			if err != nil {
				t.Error(err)
			}

			js, err := nc.JetStream()
			if err != nil {
				t.Fatal(err)
			}

			stream := &nats.StreamConfig{
				Name: "TEST",
				Placement: &nats.Placement{
					Tags: []string{"NODE_0"},
				},
			}

			_, err = js.AddStream(stream)
			if err != nil {
				t.Errorf("Unexpected error placing stream: %v", err)
			}
		})
	})

	t.Run("known cluster", func(t *testing.T) {
		cluster := "PLC2"
		withJSCluster(t, cluster, size, func(t *testing.T, nodes ...*jsServer) {
			srvA := nodes[0]
			nc, err := nats.Connect(srvA.ClientURL())
			if err != nil {
				t.Error(err)
			}

			js, err := nc.JetStream()
			if err != nil {
				t.Fatal(err)
			}

			stream := &nats.StreamConfig{
				Name: "TEST",
				Placement: &nats.Placement{
					Cluster: cluster,
					Tags:    []string{"NODE_0"},
				},
			}

			_, err = js.AddStream(stream)
			if err != nil {
				t.Errorf("Unexpected error placing stream: %v", err)
			}
		})
	})

	t.Run("unknown cluster", func(t *testing.T) {
		cluster := "PLC3"
		withJSCluster(t, cluster, size, func(t *testing.T, nodes ...*jsServer) {
			srvA := nodes[0]
			nc, err := nats.Connect(srvA.ClientURL())
			if err != nil {
				t.Error(err)
			}

			js, err := nc.JetStream()
			if err != nil {
				t.Fatal(err)
			}

			stream := &nats.StreamConfig{
				Name: "TEST",
				Placement: &nats.Placement{
					Cluster: "UNKNOWN",
				},
			}

			_, err = js.AddStream(stream)
			if err == nil {
				t.Error("Unexpected success creating stream in unknown cluster")
			}
			expected := `insufficient resources`
			if err != nil && err.Error() != expected {
				t.Errorf("Expected %q error, got: %v", expected, err)
			}
		})
	})
}

func TestJetStreamStreamMirror(t *testing.T) {
	withJSCluster(t, "MIRROR", 3, testJetStreamMirror_Source)
}

func testJetStreamMirror_Source(t *testing.T, nodes ...*jsServer) {
	srvA := nodes[0]
	nc, err := nats.Connect(srvA.ClientURL())
	if err != nil {
		t.Error(err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatal(err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "origin",
		Placement: &nats.Placement{
			Tags: []string{"NODE_0"},
		},
		Storage:  nats.MemoryStorage,
		Replicas: 1,
	})
	if err != nil {
		t.Fatalf("Unexpected error creating stream: %v", err)
	}

	totalMsgs := 10
	for i := 0; i < totalMsgs; i++ {
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

		// Stored message sequences start at 1
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

		// Stored message sequences start at 1
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

		// Make an update
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

		got = len(updated.Sources)
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}

		got = int(updated.Config.MaxMsgs)
		expected = int(config.MaxMsgs)
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
	})
}

func TestJetStream_ClusterReconnect(t *testing.T) {
	n := 3
	replicas := []int{1, 3}

	t.Run("pull sub", func(t *testing.T) {
		for _, r := range replicas {
			t.Run(fmt.Sprintf("n=%d r=%d", n, r), func(t *testing.T) {
				stream := &nats.StreamConfig{
					Name:     fmt.Sprintf("foo-r%d", r),
					Replicas: r,
				}
				withJSClusterAndStream(t, fmt.Sprintf("PULLR%d", r), n, stream, testJetStream_ClusterReconnectPullSubscriber)
			})
		}
	})

	t.Run("pull qsub", func(t *testing.T) {
		for _, r := range replicas {
			t.Run(fmt.Sprintf("n=%d r=%d", n, r), func(t *testing.T) {
				stream := &nats.StreamConfig{
					Name:     fmt.Sprintf("foo-qr%d", r),
					Replicas: r,
				}
				withJSClusterAndStream(t, fmt.Sprintf("QPULLR%d", r), n, stream, testJetStream_ClusterReconnectPullQueueSubscriber)
			})
		}
	})

	t.Run("sub durable", func(t *testing.T) {
		for _, r := range replicas {
			t.Run(fmt.Sprintf("n=%d r=%d", n, r), func(t *testing.T) {
				stream := &nats.StreamConfig{
					Name:     fmt.Sprintf("quux-r%d", r),
					Replicas: r,
				}
				withJSClusterAndStream(t, fmt.Sprintf("SUBR%d", r), n, stream, testJetStream_ClusterReconnectDurablePushSubscriber)
			})
		}
	})

	t.Run("qsub durable", func(t *testing.T) {
		r := 3
		t.Run(fmt.Sprintf("n=%d r=%d", n, r), func(t *testing.T) {
			stream := &nats.StreamConfig{
				Name:     fmt.Sprintf("bar-r%d", r),
				Replicas: r,
			}
			withJSClusterAndStream(t, fmt.Sprintf("QSUBR%d", r), n, stream, testJetStream_ClusterReconnectDurableQueueSubscriber)
		})
	})
}

func testJetStream_ClusterReconnectPullSubscriber(t *testing.T, subject string, srvs ...*jsServer) {
	var (
		recvd         int
		srvA          = srvs[0]
		totalMsgs     = 20
		durable       = nats.Durable("d1")
		reconnected   = make(chan struct{}, 2)
		reconnectDone bool
	)
	nc, err := nats.Connect(srvA.ClientURL(),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			reconnected <- struct{}{}

			// Bring back the server after the reconnect event.
			if !reconnectDone {
				reconnectDone = true
				srvA.Restart()
			}
		}),
	)
	if err != nil {
		t.Error(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		_, err := js.Publish(subject, []byte(payload))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	sub, err := js.SubscribeSync(subject, durable, nats.Pull(1))
	if err != nil {
		t.Error(err)
	}

	for i := 10; i < totalMsgs; i++ {
		payload := fmt.Sprintf("i:%d", i)
		_, err := js.Publish(subject, []byte(payload))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

NextMsg:
	for recvd < totalMsgs {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for messages, expected: %d, got: %d", totalMsgs, recvd)
		default:
		}

		pending, _, _ := sub.Pending()
		if pending == 0 {
			err = sub.Poll()
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		}

		// Server will shutdown after a couple of messages which will result
		// in empty messages with an status unavailable error.
		msg, err := sub.NextMsg(2 * time.Second)
		if err == nats.ErrNoResponders || err == nats.ErrTimeout {
			// Backoff before asking for more messages.
			time.Sleep(100 * time.Millisecond)
			continue NextMsg
		} else if err != nil {
			t.Errorf("Unexpected error: %v", err)
			continue NextMsg
		}

		if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
			t.Fatal("Got 503 JetStream API message!")
		}

		got := string(msg.Data)
		expected := fmt.Sprintf("i:%d", recvd)
		if got != expected {
			// Missed a message, but continue checking for the rest.
			recvd++
			t.Logf("WARN: Expected %v, got: %v", expected, got)
		}

		// Add a few retries since there can be errors during the reconnect.
		timeout := time.Now().Add(5 * time.Second)
	RetryAck:
		for time.Now().Before(timeout) {
			err = msg.AckSync()
			if err != nil {
				// During the reconnection, both of these errors can occur.
				if err == nats.ErrNoResponders || err == nats.ErrTimeout {
					// Wait for reconnection event to occur to continue.
					select {
					case <-reconnected:
						continue RetryAck
					case <-time.After(100 * time.Millisecond):
						continue RetryAck
					case <-ctx.Done():
						t.Fatal("Timed out waiting for reconnect")
					}
				}

				t.Errorf("Unexpected error: %v", err)
				continue RetryAck
			}
			break RetryAck
		}
		recvd++

		// Shutdown the server after a couple of messages.
		if recvd == 2 {
			srvA.Shutdown()
		}
	}
}

func testJetStream_ClusterReconnectDurableQueueSubscriber(t *testing.T, subject string, srvs ...*jsServer) {
	var (
		srvA          = srvs[0]
		srvB          = srvs[1]
		srvC          = srvs[2]
		totalMsgs     = 20
		reconnected   = make(chan struct{})
		reconnectDone bool
	)
	nc, err := nats.Connect(srvA.ClientURL(),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			reconnected <- struct{}{}

			// Bring back the server after the reconnect event.
			if !reconnectDone {
				reconnectDone = true
				srvA.Restart()
			}
		}),
	)
	if err != nil {
		t.Error(err)
	}

	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish(subject, []byte(payload))
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	msgs := make(chan *nats.Msg, totalMsgs)

	// Create some queue subscribers.
	srvAClientURL := srvA.ClientURL()
	srvBClientURL := srvB.ClientURL()
	srvCClientURL := srvC.ClientURL()
	for i := 0; i < 5; i++ {
		expected := totalMsgs
		dname := "dur"
		_, err = js.QueueSubscribe(subject, "wg", func(m *nats.Msg) {
			msgs <- m

			count := len(msgs)
			switch {
			case count == 2:
				// Do not ack and wait for redelivery on reconnect.
				srvA.Shutdown()
				return
			case count == 11:
				// Do another Shutdown of the server we are connected with.
				switch nc.ConnectedUrl() {
				case srvAClientURL:
					srvA.Shutdown()
				case srvBClientURL:
					srvB.Shutdown()
				case srvCClientURL:
					srvC.Shutdown()
				default:
				}
				return
			case count == expected:
				done()
			}

			err := m.AckSync()
			if err != nil {
				// During the reconnection, both of these errors can occur.
				if err == nats.ErrNoResponders || err == nats.ErrTimeout {
					// Wait for reconnection event to occur to continue.
					select {
					case <-reconnected:
						return
					case <-time.After(1 * time.Second):
						return
					case <-ctx.Done():
						return
					}
				}
			}
		}, nats.Durable(dname), nats.ManualAck())

		if err != nil && err != nats.ErrTimeout {
			t.Error(err)
		}
	}

	// Check for persisted messages, this could fail a few times.
	var stream *nats.StreamInfo
	timeout := time.Now().Add(5 * time.Second)
	for time.Now().Before(timeout) {
		stream, err = js.StreamInfo(subject)
		if err == nats.ErrTimeout {
			time.Sleep(100 * time.Millisecond)
			continue
		} else if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		break
	}
	if stream == nil {
		t.Logf("WARN: Failed to get stream info: %v", err)
	}

	var failedPubs int
	for i := 10; i < totalMsgs; i++ {
		var published bool
		payload := fmt.Sprintf("i:%d", i)
		timeout = time.Now().Add(5 * time.Second)

	Retry:
		for time.Now().Before(timeout) {
			_, err = js.Publish(subject, []byte(payload))

			// Skip temporary errors.
			if err == nats.ErrNoStreamResponse || err == nats.ErrTimeout {
				time.Sleep(100 * time.Millisecond)
				continue Retry
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			published = true
			break Retry
		}

		if !published {
			failedPubs++
		}
	}

	<-ctx.Done()

	// Drain to allow AckSync response to be received.
	nc.Drain()

	got := len(msgs)
	if got != totalMsgs {
		t.Logf("WARN: Expected %v, got: %v", totalMsgs, got)
	}
	if got < totalMsgs-failedPubs {
		t.Errorf("Expected %v, got: %v", totalMsgs-failedPubs, got)
	}
}

func testJetStream_ClusterReconnectDurablePushSubscriber(t *testing.T, subject string, srvs ...*jsServer) {
	var (
		srvA          = srvs[0]
		srvB          = srvs[1]
		srvC          = srvs[2]
		totalMsgs     = 20
		reconnected   = make(chan struct{})
		reconnectDone bool
	)
	nc, err := nats.Connect(srvA.ClientURL(),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			reconnected <- struct{}{}

			// Bring back the server after the reconnect event.
			if !reconnectDone {
				reconnectDone = true
				srvA.Restart()
			}
		}),
	)
	if err != nil {
		t.Error(err)
	}

	// Drain to allow Ack responses to be published.
	defer nc.Drain()

	js, err := nc.JetStream()
	if err != nil {
		t.Error(err)
	}

	// Initial burst of messages.
	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish(subject, []byte(payload))
	}

	// For now just confirm that do receive all messages across restarts.
	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

	recvd := make(chan *nats.Msg, totalMsgs)
	expected := totalMsgs
	_, err = js.Subscribe(subject, func(m *nats.Msg) {
		recvd <- m

		if len(recvd) == expected {
			done()
		}
	}, nats.Durable("sd1"))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	timeout := time.Now().Add(3 * time.Second)
	for time.Now().Before(timeout) {
		if len(recvd) >= 2 {
			// Restart the first server.
			srvA.Shutdown()
			break
		}
	}

	// Wait for reconnect or timeout.
	select {
	case <-reconnected:
	case <-time.After(2 * time.Second):
		t.Error("Timeout waiting for reconnect")
	}

	for i := 10; i < totalMsgs; i++ {
		payload := fmt.Sprintf("i:%d", i)
		timeout := time.Now().Add(5 * time.Second)
	Retry:
		for time.Now().Before(timeout) {
			_, err = js.Publish(subject, []byte(payload))
			if err == nats.ErrNoStreamResponse || err == nats.ErrTimeout {
				// Temporary error.
				time.Sleep(100 * time.Millisecond)
				continue Retry
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			break Retry
		}
	}

	srvBClientURL := srvB.ClientURL()
	srvCClientURL := srvC.ClientURL()
	timeout = time.Now().Add(3 * time.Second)
	for time.Now().Before(timeout) {
		if len(recvd) >= 5 {
			// Do another Shutdown of the server we are connected with.
			switch nc.ConnectedUrl() {
			case srvBClientURL:
				srvB.Shutdown()
			case srvCClientURL:
				srvC.Shutdown()
			default:
			}

			break
		}
	}
	<-ctx.Done()

	got := len(recvd)
	if got != totalMsgs {
		t.Logf("WARN: Expected %v, got: %v", totalMsgs, got)
	}
}

func testJetStream_ClusterReconnectPullQueueSubscriber(t *testing.T, subject string, srvs ...*jsServer) {
	var (
		recvd         = make(map[string]int)
		recvdQ        = make(map[int][]*nats.Msg)
		srvA          = srvs[0]
		totalMsgs     = 20
		durable       = nats.Durable("d1")
		reconnected   = make(chan struct{}, 2)
		reconnectDone bool
	)
	nc, err := nats.Connect(srvA.ClientURL(),
		nats.ReconnectHandler(func(nc *nats.Conn) {
			reconnected <- struct{}{}

			// Bring back the server after the reconnect event.
			if !reconnectDone {
				reconnectDone = true
				srvA.Restart()
			}
		}),
	)
	if err != nil {
		t.Error(err)
	}
	defer nc.Close()

	js, err := nc.JetStream()
	if err != nil {
		t.Error(err)
	}

	for i := 0; i < 10; i++ {
		payload := fmt.Sprintf("i:%d", i)
		_, err := js.Publish(subject, []byte(payload))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	subs := make([]*nats.Subscription, 0)

	for i := 0; i < 5; i++ {
		sub, err := js.QueueSubscribeSync(subject, "wq", durable, nats.Pull(1))
		if err != nil {
			t.Fatal(err)
		}
		subs = append(subs, sub)
	}

	for i := 10; i < totalMsgs; i++ {
		payload := fmt.Sprintf("i:%d", i)
		_, err := js.Publish(subject, []byte(payload))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
	}

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()

NextMsg:
	for len(recvd) < totalMsgs {
		select {
		case <-ctx.Done():
			t.Fatalf("Timeout waiting for messages, expected: %d, got: %d", totalMsgs, len(recvd))
		default:
		}

		for qsub, sub := range subs {
			if pending, _, _ := sub.Pending(); pending == 0 {
				err = sub.Poll()
				if err != nil {
					t.Errorf("Unexpected error: %v", err)
				}
			}

			// Server will shutdown after a couple of messages which will result
			// in empty messages with an status unavailable error.
			msg, err := sub.NextMsg(2 * time.Second)
			if err == nats.ErrNoResponders || err == nats.ErrTimeout {
				// Backoff before asking for more messages.
				time.Sleep(100 * time.Millisecond)
				continue NextMsg
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
				continue NextMsg
			}
			recvd[string(msg.Data)]++
			recvdQ[qsub] = append(recvdQ[qsub], msg)

			// Add a few retries since there can be errors during the reconnect.
			timeout := time.Now().Add(5 * time.Second)
		RetryAck:
			for time.Now().Before(timeout) {
				err = msg.AckSync()
				if err != nil {
					// During the reconnection, both of these errors can occur.
					if err == nats.ErrNoResponders || err == nats.ErrTimeout {
						// Wait for reconnection event to occur to continue.
						select {
						case <-reconnected:
							continue RetryAck
						case <-time.After(100 * time.Millisecond):
							continue RetryAck
						case <-ctx.Done():
							t.Fatal("Timed out waiting for reconnect")
						}
					}

					t.Errorf("Unexpected error: %v", err)
					continue RetryAck
				}
				break RetryAck
			}

			// Shutdown the server after a couple of messages.
			if len(recvd) == 2 {
				srvA.Shutdown()
			}
		}
	}

	// Confirm the number of messages.
	for i := 0; i < totalMsgs; i++ {
		msg := fmt.Sprintf("i:%d", i)
		count, ok := recvd[msg]
		if !ok {
			t.Errorf("Missing message %v", msg)
		} else if count != 1 {
			t.Logf("WARN: Expected to receive a single message, got: %v", count)
		}
	}

	// Expect all qsubs to receive at least a message.
	for _, msgs := range recvdQ {
		if len(msgs) < 1 {
			t.Errorf("Expected queue sub to receive at least one message")
		}
	}
}
