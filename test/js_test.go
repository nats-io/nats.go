// Copyright 2020-2022 The NATS Authors
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
	"crypto/rand"
	"errors"
	"fmt"
	"io/ioutil"
	mrand "math/rand"
	"net"
	"os"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	natsserver "github.com/nats-io/nats-server/v2/test"
)

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

func TestJetStreamNotEnabled(t *testing.T) {
	s := RunServerOnPort(-1)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AccountInfo(); err != nats.ErrJetStreamNotEnabled {
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
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.AccountInfo(); err != nats.ErrJetStreamNotEnabled {
		t.Fatalf("Did not get the proper error, got %v", err)
	}
}

func TestJetStreamPublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

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

	// Using PublishMsg API and accessing directly the Header map.
	msg2 := nats.NewMsg("foo")
	msg2.Header[nats.ExpectedLastSeqHdr] = []string{"10"}
	pa, err = js.PublishMsg(msg2)
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

	// JetStream Headers are case-sensitive right now,
	// so this will not activate the check.
	msg3 := nats.NewMsg("foo")
	msg3.Header["nats-expected-last-sequence"] = []string{"4"}
	pa, err = js.PublishMsg(msg3)
	if err != nil {
		t.Fatalf("Expected an error, got %v", err)
	}
	expect(4, 4)

	// Now test context and timeouts.
	// Both set should fail.
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	_, err = js.Publish("foo", msg, nats.AckWait(time.Second), nats.Context(ctx))
	if err != nats.ErrContextAndTimeout {
		t.Fatalf("Expected %q, got %q", nats.ErrContextAndTimeout, err)
	}

	// Create dummy listener for timeout and context tests.
	sub, err := nc.SubscribeSync("baz")
	if err != nil {
		t.Fatal(err)
	}
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

	// Test ExpectLastSequencePerSubject. Just make sure that we set the header.
	sub, err = nc.SubscribeSync("test")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	js.Publish("test", []byte("msg"), nats.ExpectLastSequencePerSubject(1))
	m, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error on next msg: %v", err)
	}
	if m.Header.Get(nats.ExpectedLastSubjSeqHdr) != "1" {
		t.Fatalf("Header ExpectLastSequencePerSubject not set: %+v", m.Header)
	}
}

func TestJetStreamSubscribe(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	expectConsumers := func(t *testing.T, expected int) {
		t.Helper()
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			var infos []*nats.ConsumerInfo
			for info := range js.ConsumersInfo("TEST") {
				infos = append(infos, info)
			}
			if len(infos) != expected {
				return fmt.Errorf("Expected %d consumers, got: %d", expected, len(infos))
			}
			return nil
		})
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

	// If stream name is not specified, then the subject is required.
	if _, err := js.SubscribeSync(""); err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Check that if stream name is present, then technically the subject does not have to.
	sub, err := js.SubscribeSync("", nats.BindStream("TEST"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub.Unsubscribe()

	// Check that Queue subscribe with HB or FC fails.
	_, err = js.QueueSubscribeSync("foo", "wq", nats.IdleHeartbeat(time.Second))
	if err == nil || !strings.Contains(err.Error(), "heartbeat") {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.QueueSubscribeSync("foo", "wq", nats.EnableFlowControl())
	if err == nil || !strings.Contains(err.Error(), "flow control") {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Check that Queue subscribe without durable name requires queue name
	// to not have "." in the name.
	_, err = js.QueueSubscribeSync("foo", "bar.baz")
	if err != nats.ErrInvalidDurableName {
		t.Fatalf("Unexpected error: %v", err)
	}

	msg := []byte("Hello JS")

	// Basic publish like NATS core.
	js.Publish("foo", msg)

	q := make(chan *nats.Msg, 4)

	checkSub, err := nc.SubscribeSync("ivan")
	if err != nil {
		t.Fatalf("Error on sub: %v", err)
	}

	// Now create a simple ephemeral consumer.
	sub1, err := js.Subscribe("foo", func(m *nats.Msg) {
		q <- m
	}, nats.DeliverSubject("ivan"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub1.Unsubscribe()

	select {
	case m := <-q:
		if _, err := m.Metadata(); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := checkSub.NextMsg(time.Second); err != nil {
			t.Fatal("Wrong deliver subject")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive the messages in time")
	}

	// Now do same but sync.
	sub2, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub2.Unsubscribe()

	waitForPending := func(t *testing.T, sub *nats.Subscription, n int) {
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

	waitForPending(t, sub2, 1)

	toSend := 10
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	done := make(chan bool, 1)
	var received int
	sub3, err := js.Subscribe("bar", func(m *nats.Msg) {
		received++
		if received == toSend {
			done <- true
		}
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 3)
	defer sub3.Unsubscribe()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive all of the messages in time")
	}

	// If we are here we have received all of the messages.
	// We hang the ConsumerInfo option off of the subscription, so we use that to check status.
	info, _ := sub3.ConsumerInfo()
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
	sub3.Unsubscribe()
	sub2.Unsubscribe()
	sub1.Unsubscribe()
	expectConsumers(t, 0)

	// Now create a sync subscriber that is durable.
	dname := "derek"
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectConsumers(t, 1)

	// Make sure we registered as a durable.
	info, _ = sub.ConsumerInfo()
	if info.Config.Durable != dname {
		t.Fatalf("Expected durable name to be set to %q, got %q", dname, info.Config.Durable)
	}
	deliver := info.Config.DeliverSubject

	// Drain subscription, this will delete the consumer.
	go func() {
		time.Sleep(250 * time.Millisecond)
		for {
			if _, err := sub.NextMsg(500 * time.Millisecond); err != nil {
				return
			}
		}
	}()
	sub.Drain()
	nc.Flush()
	expectConsumers(t, 0)

	// This will recreate a new instance.
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info, err := sub.ConsumerInfo(); err != nil || info.Config.DeliverSubject == deliver {
		t.Fatal("Expected delivery subject to be different")
	}
	expectConsumers(t, 1)

	// Subscribing again with same subject and durable name is an error.
	if _, err := js.SubscribeSync("foo", nats.Durable(dname)); err == nil {
		t.Fatal("Unexpected success")
	}
	expectConsumers(t, 1)

	// Delete the durable.
	sub.Unsubscribe()
	expectConsumers(t, 0)

	// Create again and make sure that works.
	sub, err = js.SubscribeSync("foo", nats.Durable(dname))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectConsumers(t, 1)

	if deliver == sub.Subject {
		t.Fatalf("Expected delivery subject to be different then %q", deliver)
	}
	sub.Unsubscribe()
	expectConsumers(t, 0)

	// Create a queue group on "bar" with no explicit durable name, which
	// means that the queue name will be used as the durable name.
	sub1, err = js.QueueSubscribeSync("bar", "v0")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub1.Unsubscribe()
	waitForPending(t, sub1, 10)
	expectConsumers(t, 1)

	// Since the above JS consumer is created on subject "bar", trying to
	// add a member to the same group but on subject "baz" should fail.
	if _, err = js.QueueSubscribeSync("baz", "v0"); err == nil {
		t.Fatal("Unexpected success")
	}

	// If the queue group is different, but we try to attach to the existing
	// JS consumer that is created for group "v0", then this should fail.
	if _, err = js.QueueSubscribeSync("bar", "v1", nats.Durable("v0")); err == nil {
		t.Fatal("Unexpected success")
	}
	// However, if a durable name is specified, creating a queue sub with
	// the same queue name is ok, but will feed from a different JS consumer.
	sub2, err = js.QueueSubscribeSync("bar", "v0", nats.Durable("otherQueueDurable"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub2.Unsubscribe()
	waitForPending(t, sub2, 10)
	expectConsumers(t, 2)

	sub1.Unsubscribe()
	sub2.Unsubscribe()
	expectConsumers(t, 0)

	// Now try pull based subscribers.

	// Check some error conditions first.
	if _, err := js.Subscribe("bar", nil); err != nats.ErrBadSubscription {
		t.Fatalf("Expected an error trying to create subscriber with nil callback, got %v", err)
	}

	// Since v2.7.0, we can create pull consumers with ephemerals.
	sub, err = js.PullSubscribe("bar", "")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	sub.Unsubscribe()
	// Can't specify DeliverSubject for pull subscribers
	_, err = js.PullSubscribe("bar", "foo", nats.DeliverSubject("baz"))
	if err != nats.ErrPullSubscribeToPushConsumer {
		t.Fatalf("Unexpected error: %v", err)
	}
	// If stream name is not specified, need the subject.
	_, err = js.PullSubscribe("", "rip")
	if err == nil || !strings.Contains(err.Error(), "required") {
		t.Fatalf("Unexpected error: %v", err)
	}
	// If stream provided, it should be ok.
	sub, err = js.PullSubscribe("", "rip", nats.BindStream("TEST"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub.Unsubscribe()

	batch := 5
	sub, err = js.PullSubscribe("bar", "rip")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 1)

	// The first batch if available should be delivered and queued up.
	bmsgs, err := sub.Fetch(batch)
	if err != nil {
		t.Fatal(err)
	}

	if info, _ := sub.ConsumerInfo(); info.NumAckPending != batch || info.NumPending != uint64(batch) {
		t.Fatalf("Expected %d pending ack, and %d still waiting to be delivered, got %d and %d", batch, batch, info.NumAckPending, info.NumPending)
	}

	// Now go ahead and consume these and ack, but not ack+next.
	for i := 0; i < batch; i++ {
		m := bmsgs[i]
		err = m.Ack()
		if err != nil {
			t.Fatal(err)
		}
	}
	if info, _ := sub.ConsumerInfo(); info.AckFloor.Consumer != uint64(batch) {
		t.Fatalf("Expected ack floor to be %d, got %d", batch, info.AckFloor.Consumer)
	}
	waitForPending(t, sub, 0)

	// Make a request for 10 but should only receive a few.
	bmsgs, err = sub.Fetch(10, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}
	got := len(bmsgs)
	expected := 5
	if got != expected {
		t.Errorf("Expected: %v, got: %v", expected, got)
	}

	for _, msg := range bmsgs {
		msg.Ack()
	}

	// Now test attaching to a pull based durable.

	// Test that if we are attaching that the subjects will match up. rip from
	// above was created with a filtered subject of bar, so this should fail.
	_, err = js.PullSubscribe("baz", "rip")
	if err != nats.ErrSubjectMismatch {
		t.Fatalf("Expected a %q error but got %q", nats.ErrSubjectMismatch, err)
	}

	// Queue up 10 more messages.
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	sub, err = js.PullSubscribe("bar", "rip")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	// No new JS consumer was created.
	expectConsumers(t, 1)

	// Fetch messages a couple of times.
	expected = 5
	bmsgs, err = sub.Fetch(expected, nats.MaxWait(2*time.Second))
	if err != nil {
		t.Fatal(err)
	}

	got = len(bmsgs)
	if got != expected {
		t.Errorf("Expected: %v, got: %v", expected, got)
	}

	info, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if info.NumAckPending != batch || info.NumPending != uint64(toSend-batch) {
		t.Fatalf("Expected ack pending of %d and pending to be %d, got %d %d", batch, toSend-batch, info.NumAckPending, info.NumPending)
	}

	// Pull subscriptions can't use NextMsg variants.
	if _, err := sub.NextMsg(time.Second); err != nats.ErrTypeSubscription {
		t.Fatalf("Expected error %q, got %v", nats.ErrTypeSubscription, err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := sub.NextMsgWithContext(ctx); err != nats.ErrTypeSubscription {
		t.Fatalf("Expected error %q, got %v", nats.ErrTypeSubscription, err)
	}
	cancel()

	// Prevent invalid durable names
	if _, err := js.SubscribeSync("baz", nats.Durable("test.durable")); err != nats.ErrInvalidDurableName {
		t.Fatalf("Expected invalid durable name error")
	}

	ackWait := 1 * time.Millisecond
	sub, err = js.SubscribeSync("bar", nats.Durable("ack-wait"), nats.AckWait(ackWait))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 2)

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

	// Add Stream and Consumer name to metadata.
	sub, err = js.SubscribeSync("bar", nats.Durable("consumer-name"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectConsumers(t, 3)
	m, err := sub.NextMsg(1 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	meta, err := m.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	if meta.Stream != "TEST" {
		t.Fatalf("Unexpected stream name, got: %v", meta.Stream)
	}
	if meta.Consumer != "consumer-name" {
		t.Fatalf("Unexpected consumer name, got: %v", meta.Consumer)
	}

	qsubDurable := nats.Durable("qdur-chan")
	mch := make(chan *nats.Msg, 16536)
	sub, err = js.ChanQueueSubscribe("bar", "v1", mch, qsubDurable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	expectConsumers(t, 4)

	var a, b *nats.MsgMetadata
	select {
	case msg := <-mch:
		meta, err := msg.Metadata()
		if err != nil {
			t.Error(err)
		}
		a = meta
	case <-time.After(2 * time.Second):
		t.Errorf("Timeout waiting for message")
	}

	mch2 := make(chan *nats.Msg, 16536)
	sub, err = js.ChanQueueSubscribe("bar", "v1", mch2, qsubDurable)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer sub.Unsubscribe()
	// Not a new JS consumer
	expectConsumers(t, 4)

	// Publish more messages so that at least one is received by
	// the channel queue subscriber.
	for i := 0; i < toSend; i++ {
		js.Publish("bar", msg)
	}

	select {
	case msg := <-mch2:
		meta, err := msg.Metadata()
		if err != nil {
			t.Error(err)
		}
		b = meta
	case <-time.After(2 * time.Second):
		t.Errorf("Timeout waiting for message")
	}
	if reflect.DeepEqual(a, b) {
		t.Errorf("Expected to receive different messages in stream")
	}

	// Both ChanQueueSubscribers use the same consumer.
	expectConsumers(t, 4)

	sub, err = js.SubscribeSync("foo", nats.InactiveThreshold(-100*time.Millisecond))
	if err == nil || !strings.Contains(err.Error(), "invalid InactiveThreshold") {
		t.Fatalf("Expected error about invalid option, got %v", err)
	}

	// Create an ephemeral with a lower inactive threshold
	sub, err = js.SubscribeSync("foo", nats.InactiveThreshold(50*time.Millisecond))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	ci, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Error on consumer info: %v", err)
	}
	name := ci.Name
	nc.Close()

	time.Sleep(150 * time.Millisecond)

	nc, js = jsClient(t, s)
	defer nc.Close()

	if ci, err := js.ConsumerInfo("TEST", name); err == nil {
		t.Fatalf("Expected no consumer to exist, got %+v", ci)
	}
}

func TestJetStreamAckPending_Pull(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	const totalMsgs = 4
	for i := 0; i < totalMsgs; i++ {
		if _, err := js.Publish("foo", []byte(fmt.Sprintf("msg %d", i))); err != nil {
			t.Fatal(err)
		}
	}

	ackPendingLimit := 3
	sub, err := js.PullSubscribe("foo", "dname-pull-ack-wait", nats.MaxAckPending(ackPendingLimit))
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Unsubscribe()

	var msgs []*nats.Msg
	for i := 0; i < ackPendingLimit; i++ {
		ms, err := sub.Fetch(1)
		if err != nil {
			t.Fatalf("Error on fetch: %v", err)
		}
		msgs = append(msgs, ms...)
	}

	// Since we don't ack, the next fetch should time out because the server
	// won't send new ones until we ack some.
	if _, err := sub.Fetch(1, nats.MaxWait(250*time.Millisecond)); err != nats.ErrTimeout {
		t.Fatalf("Expected timeout, got: %v", err)
	}
	// Ack one message, then we should be able to get the next
	msgs[0].Ack()
	if _, err := sub.Fetch(1); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamAckPending_Push(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

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

		meta, err := m.Metadata()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		acks[int(meta.Sequence.Stream)]++

		if ackPending != 0 {
			ackPending--
		}
		if int(meta.NumPending) != ackPending {
			t.Errorf("Expected %v, got %v", ackPending, meta.NumPending)
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
	defer shutdownJSServerAndRemoveStorage(t, s)

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
	defer shutdownJSServerAndRemoveStorage(t, s)

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
	conf := createConfFile(t, []byte(`
                listen: 127.0.0.1:-1
                jetstream: enabled
                accounts: {
                  A {
                    users: [{ user: "foo" }]
                    jetstream: { max_mem: 64MB, max_file: 64MB }
                  }
                }
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s, nats.UserInfo("foo", ""))
	defer nc.Close()

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

	var err error
	t.Run("stream not found", func(t *testing.T) {
		si, err = js.StreamInfo("bar")
		if !errors.Is(err, nats.ErrStreamNotFound) {
			t.Fatalf("Expected error: %v, got: %v", nats.ErrStreamNotFound, err)
		}
		if si != nil {
			t.Fatalf("StreamInfo should be nil %+v", si)
		}
	})

	t.Run("stream info", func(t *testing.T) {
		si, err = js.StreamInfo("foo")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}
	})

	t.Run("create bad stream", func(t *testing.T) {
		_, err := js.AddStream(&nats.StreamConfig{Name: "foo.invalid"})
		if err != nats.ErrInvalidStreamName {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("bad stream info", func(t *testing.T) {
		_, err := js.StreamInfo("foo.invalid")
		if err != nats.ErrInvalidStreamName {
			t.Fatalf("Unexpected error: %v", err)
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

	t.Run("create consumer on missing stream", func(t *testing.T) {
		_, err = js.AddConsumer("missing", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
		if err != nats.ErrStreamNotFound {
			t.Fatalf("Expected stream not found error, got: %v", err)
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

	t.Run("consumer not found", func(t *testing.T) {
		ci, err := js.ConsumerInfo("foo", "cld")
		if !errors.Is(err, nats.ErrConsumerNotFound) {
			t.Fatalf("Expected error: %v, got: %v", nats.ErrConsumerNotFound, err)
		}
		if ci != nil {
			t.Fatalf("ConsumerInfo should be nil %+v", ci)
		}
	})

	t.Run("list streams", func(t *testing.T) {
		var infos []*nats.StreamInfo
		for info := range js.StreamsInfo() {
			infos = append(infos, info)
		}
		if len(infos) != 1 || infos[0].Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", infos)
		}
	})

	t.Run("list consumers", func(t *testing.T) {
		var infos []*nats.ConsumerInfo
		for info := range js.ConsumersInfo("") {
			infos = append(infos, info)
		}
		if len(infos) != 0 {
			t.Fatalf("ConsumerInfo is not correct %+v", infos)
		}

		infos = infos[:0]
		for info := range js.ConsumersInfo("foo") {
			infos = append(infos, info)
		}
		if len(infos) != 1 || infos[0].Stream != "foo" || infos[0].Config.Durable != "dlc" {
			t.Fatalf("ConsumerInfo is not correct %+v", infos)
		}
	})

	t.Run("list consumer names", func(t *testing.T) {
		var names []string
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		for name := range js.ConsumerNames("foo", nats.Context(ctx)) {
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

	t.Run("update consumer", func(t *testing.T) {
		ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:        "update_push_consumer",
			DeliverSubject: "bar",
			AckPolicy:      nats.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Currently, server supports these fields:
		// description, ack_wait, max_deliver, sample_freq, max_ack_pending, max_waiting and headers_only
		expected := ci.Config
		expected.Description = "my description"
		expected.AckWait = 2 * time.Second
		expected.MaxDeliver = 1
		expected.SampleFrequency = "30"
		expected.MaxAckPending = 10
		expected.HeadersOnly = true

		// Check that stream name is required
		_, err = js.UpdateConsumer("", &expected)
		if err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected stream name required error, got %v", err)
		}
		// Check that durable name is required
		expected.Durable = ""
		_, err = js.UpdateConsumer("foo", &expected)
		if err != nats.ErrInvalidDurableName {
			t.Fatalf("Expected consumer name required error, got %v", err)
		}
		expected.Durable = "update_push_consumer"

		// Check that configuration is required
		_, err = js.UpdateConsumer("foo", nil)
		if err != nats.ErrConsumerConfigRequired {
			t.Fatalf("Expected consumer configuration required error, got %v", err)
		}

		// Now check that update works and expected fields have been updated
		ci, err = js.UpdateConsumer("foo", &expected)
		if err != nil {
			t.Fatalf("Error on update: %v", err)
		}
		if !reflect.DeepEqual(ci.Config, expected) {
			t.Fatalf("Expected config to be %+v, got %+v", expected, ci.Config)
		}

		// Now check with pull consumer
		ci, err = js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:    "update_pull_consumer",
			AckPolicy:  nats.AckExplicitPolicy,
			MaxWaiting: 1,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// Currently, server supports these fields:
		// description, ack_wait, max_deliver, sample_freq, max_ack_pending, max_waiting and headers_only
		expected = ci.Config
		expected.Description = "my description"
		expected.AckWait = 2 * time.Second
		expected.MaxDeliver = 1
		expected.SampleFrequency = "30"
		expected.MaxAckPending = 10
		expected.MaxWaiting = 20
		expected.HeadersOnly = true
		expected.MaxRequestBatch = 10
		expected.MaxRequestExpires = 2 * time.Second
		ci, err = js.UpdateConsumer("foo", &expected)
		if err != nil {
			t.Fatalf("Error on update: %v", err)
		}
		if !reflect.DeepEqual(ci.Config, expected) {
			t.Fatalf("Expected config to be %+v, got %+v", expected, ci.Config)
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
		for name := range js.StreamNames(nats.Context(ctx)) {
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
		if info.Limits.MaxMemory != 67108864 {
			t.Errorf("Expected to have memory limits, got: %v", info.Limits.MaxMemory)
		}
		if info.Limits.MaxStore != 67108864 {
			t.Errorf("Expected to have disk limits, got: %v", info.Limits.MaxMemory)
		}
		if info.Limits.MaxStreams != -1 {
			t.Errorf("Expected to not have stream limits, got: %v", info.Limits.MaxStreams)
		}
		if info.Limits.MaxConsumers != -1 {
			t.Errorf("Expected to not have consumer limits, got: %v", info.Limits.MaxConsumers)
		}
		if info.API.Total == 0 {
			t.Errorf("Expected some API calls, got: %v", info.API.Total)
		}
		if info.API.Errors == 0 {
			t.Errorf("Expected some API error, got: %v", info.API.Errors)
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
	nc, js := jsClient(t, s.Server)
	defer nc.Close()

	var err error

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
		msg.Header = nats.Header{
			"X-NATS-Key": []string{"123"},
		}
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
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatal(err)
		}
		originalSeq = meta.Sequence.Stream

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
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatal(err)
		}
		newSeq := meta.Sequence.Stream

		// First message removed
		if newSeq <= originalSeq {
			t.Errorf("Expected %d to be higher sequence than %d",
				newSeq, originalSeq)
		}

		// Try to fetch the same message which should be gone.
		_, err = js.GetMsg("foo", originalSeq)
		if err == nil || err != nats.ErrMsgNotFound {
			t.Errorf("Expected no message found error, got: %v", err)
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
			"X-NATS-Key":       {"123"},
		}
		if !reflect.DeepEqual(streamMsg.Header, nats.Header(expectedMap)) {
			t.Errorf("Expected %v, got: %v", expectedMap, streamMsg.Header)
		}

		sub, err := js.SubscribeSync("foo.A", nats.StartSequence(4))
		if err != nil {
			t.Fatal(err)
		}
		msg, err := sub.NextMsg(2 * time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(msg.Header, nats.Header(expectedMap)) {
			t.Errorf("Expected %v, got: %v", expectedMap, msg.Header)
		}
	})
}

func TestJetStreamManagement_DeleteMsg(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

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
	meta, err := msg.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	originalSeq := meta.Sequence.Stream

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
	meta, err = msg.Metadata()
	if err != nil {
		t.Fatal(err)
	}
	newSeq := meta.Sequence.Stream

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
	defer shutdownJSServerAndRemoveStorage(t, s)

	// Create a stream using JSM.
	ncm, jsm := jsClient(t, s, nats.UserInfo("dlc", "foo"))
	defer ncm.Close()

	var err error

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
					# For now have to expose the API to enable JS context across account.
					{ service: "$JS.API.INFO" }
					# For the stream publish.
					{ service: "ORDERS" }
					# For the pull based consumer. Response type needed for batchsize > 1
					{ service: "$JS.API.CONSUMER.INFO.ORDERS.d1", response: stream }
					{ service: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", response: stream }
					# For the push based consumer delivery and ack.
					{ stream: "p.d" }
					{ stream: "p.d3" }
					# For the acks. Service in case we want an ack to our ack.
					{ service: "$JS.ACK.ORDERS.*.>" }

					# Allow lookup of stream to be able to bind from another account.
					{ service: "$JS.API.CONSUMER.INFO.ORDERS.d4", response: stream }
					{ stream: "p.d4" }
				]
			},
			U: {
				users: [ { user: rip, password: bar } ]
				imports [
					{ service: { subject: "$JS.API.INFO", account: JS } }
					{ service: { subject: "ORDERS", account: JS } , to: "orders" }
					# { service: { subject: "$JS.API.CONSUMER.INFO.ORDERS.d1", account: JS } }
					{ service: { subject: "$JS.API.CONSUMER.INFO.ORDERS.d4", account: JS } }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", account: JS } }
					{ stream:  { subject: "p.d", account: JS } }
					{ stream:  { subject: "p.d3", account: JS } }
					{ stream:  { subject: "p.d4", account: JS } }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
				]
			},
			V: {
				users: [ {
					user: v,
					password: quux,
					permissions: { publish: {deny: ["$JS.API.CONSUMER.INFO.ORDERS.d1"]} }
				} ]
				imports [
					{ service: { subject: "$JS.API.INFO", account: JS } }
					{ service: { subject: "ORDERS", account: JS } , to: "orders" }
					{ service: { subject: "$JS.API.CONSUMER.INFO.ORDERS.d1", account: JS } }
					{ service: { subject: "$JS.API.CONSUMER.INFO.ORDERS.d4", account: JS } }
					{ service: { subject: "$JS.API.CONSUMER.MSG.NEXT.ORDERS.d1", account: JS } }
					{ stream:  { subject: "p.d", account: JS } }
					{ stream:  { subject: "p.d3", account: JS } }
					{ stream:  { subject: "p.d4", account: JS } }
					{ service: { subject: "$JS.ACK.ORDERS.*.>", account: JS } }
				]
			},
		}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	// Create a stream using JSM.
	ncm, jsm := jsClient(t, s, nats.UserInfo("dlc", "foo"))
	defer ncm.Close()

	var err error

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

	_, err = jsm.AddConsumer("ORDERS", &nats.ConsumerConfig{
		Durable:        "d4",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: "p.d4",
	})
	if err != nil {
		t.Fatalf("push consumer create failed: %v", err)
	}

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Now make sure we can send to the stream from another account.
	toSend := 100
	for i := 0; i < toSend; i++ {
		if _, err := js.Publish("orders", []byte(fmt.Sprintf("ORDER-%d", i+1))); err != nil {
			t.Fatalf("Unexpected error publishing message %d: %v", i+1, err)
		}
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

	// Can attach to the consumer from another JS account if there is a durable name.
	sub, err = js.SubscribeSync("ORDERS", nats.Durable("d4"), nats.BindStream("ORDERS"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	waitForPending(t, toSend)

	// Even if there are no permissions or import to check that a consumer exists,
	// it is still possible to bind subscription to it.
	sub, err = js.PullSubscribe("ORDERS", "d1", nats.Bind("ORDERS", "d1"))
	if err != nil {
		t.Fatal(err)
	}
	expected := 10
	msgs, err := sub.Fetch(expected)
	if err != nil {
		t.Fatal(err)
	}
	got := len(msgs)
	if got != expected {
		t.Fatalf("Expected %d, got %d", expected, got)
	}

	// Account without permissions to lookup should be able to bind as well.
	eh := func(_ *nats.Conn, _ *nats.Subscription, err error) {}
	nc, err = nats.Connect(s.ClientURL(), nats.UserInfo("v", "quux"), nats.ErrorHandler(eh))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// Since we know that the lookup will fail, we use a smaller timeout than the 5s default.
	js, err = nc.JetStream(nats.MaxWait(500 * time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	sub, err = js.PullSubscribe("ORDERS", "d1", nats.Bind("ORDERS", "d1"))
	if err != nil {
		t.Fatal(err)
	}
	expected = 10
	msgs, err = sub.Fetch(expected)
	if err != nil {
		t.Fatal(err)
	}
	got = len(msgs)
	if got != expected {
		t.Fatalf("Expected %d, got %d", expected, got)
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
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc1, js1 := jsClient(t, s, nats.UserInfo("rip", "pass"))
	defer nc1.Close()

	var err error

	_, err = js1.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Replicas: 1,
	})
	if err != nil {
		t.Fatal(err)
	}

	const (
		toSend      = 100
		publishSubj = "TEST"
		sourceName  = "MY_SOURCE_TEST"
		mirrorName  = "MY_MIRROR_TEST"
	)
	for i := 0; i < toSend; i++ {
		data := []byte(fmt.Sprintf("OK %d", i))
		if _, err := js1.Publish(publishSubj, data); err != nil {
			t.Fatalf("Unexpected publish error: %v", err)
		}
	}

	nc2, js2 := jsClient(t, s, nats.UserInfo("dlc", "pass"))
	defer nc2.Close()

	checkMsgCount := func(t *testing.T, stream string) {
		t.Helper()
		checkFor(t, 20*time.Second, 100*time.Millisecond, func() error {
			si, err := js2.StreamInfo(stream)
			if err != nil {
				return err
			}
			if si.State.Msgs != uint64(toSend) {
				return fmt.Errorf("Expected %d msgs, got state: %+v", toSend, si.State)
			}
			return nil
		})
	}
	checkConsume := func(t *testing.T, js nats.JetStream, subject, stream string, want int) {
		t.Helper()
		sub, err := js.SubscribeSync(subject, nats.BindStream(stream))
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		checkSubsPending(t, sub, want)

		for i := 0; i < want; i++ {
			msg, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatal(err)
			}
			meta, err := msg.Metadata()
			if err != nil {
				t.Fatal(err)
			}
			if got, want := meta.Stream, stream; got != want {
				t.Fatalf("unexpected stream name, got=%q, want=%q", got, want)
			}
		}
	}

	_, err = js2.AddStream(&nats.StreamConfig{
		Name:    mirrorName,
		Storage: nats.FileStorage,
		Mirror: &nats.StreamSource{
			Name: publishSubj,
			External: &nats.ExternalStream{
				APIPrefix:     "RI.JS.API",
				DeliverPrefix: "RI.DELIVER.SYNC.MIRRORS",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	checkMsgCount(t, mirrorName)
	checkConsume(t, js2, publishSubj, mirrorName, toSend)

	_, err = js2.AddStream(&nats.StreamConfig{
		Name:    sourceName,
		Storage: nats.FileStorage,
		Sources: []*nats.StreamSource{
			&nats.StreamSource{
				Name: publishSubj,
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
	checkMsgCount(t, sourceName)
	checkConsume(t, js2, publishSubj, sourceName, toSend)
}

func TestJetStreamAutoMaxAckPending(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s, nats.SyncQueueLen(500))
	defer nc.Close()

	var err error

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
	sub, err := js.SubscribeSync("foo")
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
	defer shutdownJSServerAndRemoveStorage(t, s)

	var jsm nats.JetStreamManager
	var jsctx nats.JetStreamContext

	// JetStream that can publish/subscribe but cannot manage streams.
	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

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

func TestJetStreamSubscribe_DeliverPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
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
		time.Sleep(15 * time.Millisecond)
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
			timeout := 2 * time.Second
			if test.expected == 0 {
				timeout = 250 * time.Millisecond
			}
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
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

	js.Publish("bar", []byte("bar msg 1"))
	js.Publish("bar", []byte("bar msg 2"))

	sub, err := js.SubscribeSync("bar", nats.BindStream("TEST"), nats.DeliverLastPerSubject())
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error on next msg: %v", err)
	}
	if string(msg.Data) != "bar msg 2" {
		t.Fatalf("Unexepcted last message: %q", msg.Data)
	}
}

func TestJetStreamSubscribe_AckPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar"},
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

		meta, err := msg.Metadata()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		if meta.Sequence.Consumer != 1 || meta.Sequence.Stream != 1 || meta.NumDelivered != 1 {
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

		// Prevent double context and ack wait options.
		err = msg.AckSync(nats.Context(ctx), nats.AckWait(1*time.Second))
		if err != nats.ErrContextAndTimeout {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.AckSync(nats.Context(ctx))
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.AckSync(nats.AckWait(2 * time.Second))
		if err != nats.ErrMsgAlreadyAckd {
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

		// Prevent double context and ack wait options.
		err = msg.Nak(nats.Context(ctx), nats.AckWait(1*time.Second))
		if err != nats.ErrContextAndTimeout {
			t.Errorf("Unexpected error: %v", err)
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
		err = msg.Nak(nats.AckWait(2 * time.Second))
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

		// Prevent double context and ack wait options.
		err = msg.Term(nats.Context(ctx), nats.AckWait(1*time.Second))
		if err != nats.ErrContextAndTimeout {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.Term()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
		defer done()

		// Convert context into nats option.
		nctx := nats.Context(ctx)
		msg, err = sub.NextMsgWithContext(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:6"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		err = msg.Term(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		msg, err = sub.NextMsgWithContext(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		got = string(msg.Data)
		expected = "i:7"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}

		// Prevent double context and ack wait options.
		err = msg.InProgress(nats.Context(ctx), nats.AckWait(1*time.Second))
		if err != nats.ErrContextAndTimeout {
			t.Errorf("Unexpected error: %v", err)
		}

		err = msg.InProgress(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		err = msg.InProgress(nctx)
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		err = msg.Ack(nctx)
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

	t.Run("Nak with delay", func(t *testing.T) {
		js.Publish("bar", []byte("msg"))
		sub, err := js.SubscribeSync("bar", nats.Durable("nak_dur"))
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on NextMsg: %v", err)
		}
		if err := msg.NakWithDelay(500 * time.Millisecond); err != nil {
			t.Fatalf("Error on Nak: %v", err)
		}
		// We should not get redelivery before 500ms+
		if _, err = sub.NextMsg(250 * time.Millisecond); err != nats.ErrTimeout {
			t.Fatalf("Expected timeout, got %v", err)
		}
		msg, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on NextMsg: %v", err)
		}
		if err := msg.NakWithDelay(0); err != nil {
			t.Fatalf("Error on Nak: %v", err)
		}
		msg, err = sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			t.Fatalf("Expected timeout, got %v", err)
		}
		msg.Ack()
	})

	t.Run("BackOff redeliveries", func(t *testing.T) {
		inbox := nats.NewInbox()
		sub, err := nc.SubscribeSync(inbox)
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		defer sub.Unsubscribe()
		cc := nats.ConsumerConfig{
			Durable:        "backoff",
			AckPolicy:      nats.AckExplicitPolicy,
			DeliverPolicy:  nats.DeliverAllPolicy,
			FilterSubject:  "bar",
			DeliverSubject: inbox,
			BackOff:        []time.Duration{50 * time.Millisecond, 250 * time.Millisecond},
		}
		// First, try with a MaxDeliver that is < len(BackOff), which the
		// server should reject.
		cc.MaxDeliver = 1
		_, err = js.AddConsumer("TEST", &cc)
		if err == nil || !strings.Contains(err.Error(), "max deliver is required to be > length of backoff values") {
			t.Fatalf("Expected backoff/max deliver error, got %v", err)
		}
		// Now put a valid value
		cc.MaxDeliver = 4
		ci, err := js.AddConsumer("TEST", &cc)
		if err != nil {
			t.Fatalf("Error on add consumer: %v", err)
		}
		if !reflect.DeepEqual(ci.Config.BackOff, cc.BackOff) {
			t.Fatalf("Expected backoff to be %v, got %v", cc.BackOff, ci.Config.BackOff)
		}
		// Consume the first delivery
		_, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on nextMsg: %v", err)
		}
		// We should get a redelivery at around 50ms
		start := time.Now()
		_, err = sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error on nextMsg: %v", err)
		}
		if dur := time.Since(start); dur < 25*time.Millisecond || dur > 100*time.Millisecond {
			t.Fatalf("Expected to be redelivered at around 50ms, took %v", dur)
		}
		// Now it should be every 250ms or so
		for i := 0; i < 2; i++ {
			start = time.Now()
			_, err = sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Error on nextMsg for iter=%v: %v", i+1, err)
			}
			if dur := time.Since(start); dur < 200*time.Millisecond || dur > 300*time.Millisecond {
				t.Fatalf("Expected to be redelivered at around 250ms, took %v", dur)
			}
		}
		// At this point, we should have go reach MaxDeliver
		_, err = sub.NextMsg(300 * time.Millisecond)
		if err != nats.ErrTimeout {
			t.Fatalf("Expected timeout, got %v", err)
		}
	})
}

func TestJetStreamPullSubscribe_AckPending(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	const totalMsgs = 10
	for i := 0; i < totalMsgs; i++ {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish("foo", []byte(payload))
	}

	sub, err := js.PullSubscribe("foo", "wq",
		nats.AckWait(200*time.Millisecond),
		nats.MaxAckPending(5),
	)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	nextMsg := func() *nats.Msg {
		msgs, err := sub.Fetch(1)
		if err != nil {
			t.Fatal(err)
		}
		return msgs[0]
	}

	getPending := func() (int, int) {
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		return info.NumAckPending, int(info.NumPending)
	}

	getMetadata := func(msg *nats.Msg) *nats.MsgMetadata {
		meta, err := msg.Metadata()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		return meta
	}

	expectedPending := func(inflight int, pending int) {
		i, p := getPending()
		if i != inflight || p != pending {
			t.Errorf("Unexpected inflight/pending msgs: %v/%v", i, p)
		}
	}

	inflight, pending := getPending()
	if inflight != 0 || pending != totalMsgs {
		t.Errorf("Unexpected inflight/pending msgs: %v/%v", inflight, pending)
	}

	// Normal Ack should decrease pending
	msg := nextMsg()
	err = msg.Ack()
	if err != nil {
		t.Fatal(err)
	}

	expectedPending(0, 9)
	meta := getMetadata(msg)

	if meta.Sequence.Consumer != 1 || meta.Sequence.Stream != 1 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// AckSync
	msg = nextMsg()
	err = msg.AckSync()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(0, 8)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 2 || meta.Sequence.Stream != 2 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// Nak the message so that it is redelivered.
	msg = nextMsg()
	err = msg.Nak()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(1, 7)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 3 || meta.Sequence.Stream != 3 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}
	prevSeq := meta.Sequence.Stream
	prevPayload := string(msg.Data)

	// Nak same sequence again, sequence number should not change.
	msg = nextMsg()
	err = msg.Nak()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(1, 7)
	meta = getMetadata(msg)
	if meta.Sequence.Stream != prevSeq {
		t.Errorf("Expected to get message at seq=%v, got seq=%v", prevSeq, meta.Sequence.Stream)
	}
	if string(msg.Data) != prevPayload {
		t.Errorf("Expected: %q, got: %q", string(prevPayload), string(msg.Data))
	}
	if meta.Sequence.Consumer != 4 || meta.NumDelivered != 2 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// Terminate message so it is no longer pending.
	msg = nextMsg()
	err = msg.Term()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(0, 7)
	meta = getMetadata(msg)
	if meta.Sequence.Stream != prevSeq {
		t.Errorf("Expected to get message at seq=%v, got seq=%v", prevSeq, meta.Sequence.Stream)
	}
	if string(msg.Data) != prevPayload {
		t.Errorf("Expected: %q, got: %q", string(prevPayload), string(msg.Data))
	}
	if meta.Sequence.Consumer != 5 || meta.Sequence.Stream != 3 || meta.NumDelivered != 3 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// Get next message and ack in progress a few times
	msg = nextMsg()
	expected := "i:3"
	if string(msg.Data) != expected {
		t.Errorf("Expected: %q, got: %q", string(msg.Data), expected)
	}
	err = msg.InProgress()
	if err != nil {
		t.Fatal(err)
	}
	err = msg.InProgress()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(1, 6)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 6 || meta.Sequence.Stream != 4 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}

	// Now ack the message to mark it as done.
	err = msg.AckSync()
	if err != nil {
		t.Fatal(err)
	}
	expectedPending(0, 6)

	// Fetch next message, but do not ack and wait for redelivery.
	msg = nextMsg()
	expectedPending(1, 5)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 7 || meta.Sequence.Stream != 5 || meta.NumDelivered != 1 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}
	prevSeq = meta.Sequence.Stream
	time.Sleep(500 * time.Millisecond)
	expectedPending(1, 5)

	// Next message should be a redelivery.
	msg = nextMsg()
	expectedPending(1, 5)
	meta = getMetadata(msg)
	if meta.Sequence.Consumer != 8 || meta.Sequence.Stream != prevSeq || meta.NumDelivered != 2 {
		t.Errorf("Unexpected metadata: %+v", meta)
	}
	err = msg.AckSync()
	if err != nil {
		t.Fatal(err)
	}

	// Get rest of messages.
	msgs, err := sub.Fetch(5)
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs {
		getMetadata(msg)
		msg.Ack()
	}
	expectedPending(0, 0)
}

func TestJetStreamSubscribe_AckDup(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

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
		if e != nats.ErrMsgAlreadyAckd {
			t.Errorf("Expected error: %v", e)
		}
	}
	if len(pings) != 1 {
		t.Logf("Expected to receive a single ack, got: %v", len(pings))
	}
}

func TestJetStreamSubscribe_AckDupInProgress(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

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
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchConsumers := func(t *testing.T, expected int) {
		t.Helper()
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			var infos []*nats.ConsumerInfo
			for info := range js.ConsumersInfo("foo") {
				infos = append(infos, info)
			}
			if len(infos) != expected {
				return fmt.Errorf("Expected %d consumers, got: %d", expected, len(infos))
			}
			return nil
		})
	}

	deleteAllConsumers := func(t *testing.T) {
		t.Helper()
		for cn := range js.ConsumerNames("foo") {
			js.DeleteConsumer("foo", cn)
		}
	}

	js.Publish("foo.A", []byte("A"))
	js.Publish("foo.B", []byte("B"))
	js.Publish("foo.C", []byte("C"))

	t.Run("consumers deleted on unsubscribe", func(t *testing.T) {
		sub, err := js.SubscribeSync("foo.A")
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		sub, err = js.SubscribeSync("foo.B", nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		sub, err = js.Subscribe("foo.C", func(_ *nats.Msg) {})
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}

		sub, err = js.Subscribe("foo.C", func(_ *nats.Msg) {}, nats.Durable("C"))
		if err != nil {
			t.Fatal(err)
		}
		if err := sub.Unsubscribe(); err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 0)
	})

	t.Run("not deleted on unsubscribe if consumer created externally", func(t *testing.T) {
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
		subC, err := js.PullSubscribe("foo.C", "wq")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 1)

		msgs, err := subC.Fetch(1, nats.MaxWait(2*time.Second))
		if err != nil {
			t.Errorf("Unexpected error getting message: %v", err)
		}
		msg := msgs[0]
		got := string(msg.Data)
		expected := "C"
		if got != expected {
			t.Errorf("Expected %v, got %v", expected, got)
		}
		subC.Unsubscribe()
		fetchConsumers(t, 1)
		deleteAllConsumers(t)
	})

	t.Run("consumers deleted on drain", func(t *testing.T) {
		subA, err := js.Subscribe("foo.A", func(_ *nats.Msg) {})
		if err != nil {
			t.Fatal(err)
		}
		fetchConsumers(t, 1)
		err = subA.Drain()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 0)
		deleteAllConsumers(t)
	})

	t.Run("durable consumers deleted on drain", func(t *testing.T) {
		subB, err := js.Subscribe("foo.B", func(_ *nats.Msg) {}, nats.Durable("B"))
		if err != nil {
			t.Fatal(err)
		}
		fetchConsumers(t, 1)
		err = subB.Drain()
		if err != nil {
			t.Errorf("Unexpected error: %v", err)
		}
		fetchConsumers(t, 0)
	})
}

func TestJetStream_UnsubscribeCloseDrain(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	serverURL := s.ClientURL()
	mc, jsm := jsClient(t, s)
	defer mc.Close()

	var err error

	_, err = jsm.AddStream(&nats.StreamConfig{
		Name:     "foo",
		Subjects: []string{"foo.A", "foo.B", "foo.C"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	fetchConsumers := func(t *testing.T, expected int) []*nats.ConsumerInfo {
		t.Helper()
		var infos []*nats.ConsumerInfo
		for info := range jsm.ConsumersInfo("foo") {
			infos = append(infos, info)
		}
		if len(infos) != expected {
			t.Fatalf("Expected %d consumers, got: %d", expected, len(infos))
		}

		return infos
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

		// sub.Drain() or nc.Drain() delete JS consumer, same than Unsubscribe()
		nc.Drain()
		<-ctx.Done()
		fetchConsumers(t, 0)
	})

	jsm.Publish("foo.A", []byte("A.1"))
	jsm.Publish("foo.B", []byte("B.1"))
	jsm.Publish("foo.C", []byte("C.1"))

	t.Run("conn close does not delete any consumer", func(t *testing.T) {
		nc, js := jsClient(t, s)
		defer nc.Close()

		if _, err := js.SubscribeSync("foo.A"); err != nil {
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

	t.Run("reattached durables consumers cannot be deleted with unsubscribe", func(t *testing.T) {
		nc, js := jsClient(t, s)
		defer nc.Close()

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

		// Sub can still receive the same message.
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

		// Since library did not create, the JS consumers remain.
		fetchConsumers(t, 2)
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
	defer shutdownJSServerAndRemoveStorage(t, s)

	errCh := make(chan error, 2)
	nc, err := nats.Connect(s.ClientURL(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errCh <- err
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	js, err := nc.JetStream(nats.MaxWait(time.Second))
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

func TestJetStreamSubscribe_ReplayPolicy(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	i := 0
	totalMsgs := 10
	for range time.NewTicker(100 * time.Millisecond).C {
		payload := fmt.Sprintf("i:%d", i)
		js.Publish("foo", []byte(payload))
		i++

		if i == totalMsgs {
			break
		}
	}

	// By default it is ReplayInstant playback policy.
	isub, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err := isub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.ReplayPolicy != nats.ReplayInstantPolicy {
		t.Fatalf("Expected original replay policy, got: %v", ci.Config.ReplayPolicy)
	}

	// Change into original playback.
	sub, err := js.SubscribeSync("foo", nats.ReplayOriginal())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	ci, err = sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if ci.Config.ReplayPolicy != nats.ReplayOriginalPolicy {
		t.Fatalf("Expected original replay policy, got: %v", ci.Config.ReplayPolicy)
	}

	// There should already be a message delivered.
	_, err = sub.NextMsg(10 * time.Millisecond)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// We should timeout faster since too soon for the original playback.
	_, err = sub.NextMsg(10 * time.Millisecond)
	if err != nats.ErrTimeout {
		t.Fatalf("Expected timeout error replaying the stream, got: %v", err)
	}

	// Enough time to get the next message according to the original playback.
	_, err = sub.NextMsg(110 * time.Millisecond)
	if err != nil {

		t.Fatalf("Unexpected error: %v", err)
	}
}

func TestJetStreamSubscribe_RateLimit(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	totalMsgs := 2048
	for i := 0; i < totalMsgs; i++ {
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

	// Change rate limit.
	// Make the receive channel able to possibly hold ALL messages, but
	// we expect it to hold less due to rate limiting.
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
}

func TestJetStreamSubscribe_ConfigCantChange(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
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

	// Check that binding to a durable (without specifying durable option) works
	if _, err := js.AddConsumer("TEST", &nats.ConsumerConfig{
		Durable:        "BindDurable",
		DeliverSubject: "bar",
	}); err != nil {
		t.Fatalf("Failed to create consumer: %v", err)
	}
	if _, err := js.SubscribeSync("foo", nats.Bind("TEST", "BindDurable")); err != nil {
		t.Fatalf("Error on subscribe: %v", err)
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

		if size > 1 {
			o.Cluster.Name = clusterName
			_, host1, port1 := getAddr()
			o.Host = host1
			o.Port = port1

			addr2, host2, port2 := getAddr()
			o.Cluster.Host = host2
			o.Cluster.Port = port2
			o.Tags = []string{o.ServerName}
			routes = append(routes, fmt.Sprintf("nats://%s", addr2))
		}
		opts = append(opts, &o)
	}

	if size > 1 {
		routesStr := server.RoutesFromStr(strings.Join(routes, ","))

		for i, o := range opts {
			o.Routes = routesStr
			nodes[i] = &jsServer{Server: natsserver.RunServer(o), myopts: o}
		}
	} else {
		o := opts[0]
		nodes[0] = &jsServer{Server: natsserver.RunServer(o), myopts: o}
	}

	// Wait until JS is ready.
	srvA := nodes[0]
	nc, err := nats.Connect(srvA.ClientURL())
	if err != nil {
		t.Error(err)
	}
	waitForJSReady(t, nc)
	nc.Close()

	return nodes
}

func withJSServer(t *testing.T, tfn func(t *testing.T, srvs ...*jsServer)) {
	t.Helper()

	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	opts.LameDuckDuration = 3 * time.Second
	opts.LameDuckGracePeriod = 2 * time.Second
	s := &jsServer{Server: RunServerWithOptions(opts), myopts: &opts}
	defer shutdownJSServerAndRemoveStorage(t, s.Server)
	tfn(t, s)
}

func withJSCluster(t *testing.T, clusterName string, size int, tfn func(t *testing.T, srvs ...*jsServer)) {
	t.Helper()

	nodes := setupJSClusterWithSize(t, clusterName, size)
	defer func() {
		// Ensure that they get shutdown and remove their state.
		for _, node := range nodes {
			node.restart.Lock()
			shutdownJSServerAndRemoveStorage(t, node.Server)
			node.restart.Unlock()
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
		defer nc.Close()

		timeout := time.Now().Add(10 * time.Second)
		for time.Now().Before(timeout) {
			jsm, err := nc.JetStream()
			if err != nil {
				t.Fatal(err)
			}
			_, err = jsm.AccountInfo()
			if err != nil {
				// Backoff for a bit until cluster and resources are ready.
				time.Sleep(500 * time.Millisecond)
			}

			_, err = jsm.AddStream(stream)
			if err != nil {
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
		// Use a smaller MaxWait here since if it fails, we don't want
		// to wait for too long since we are going to try again.
		js, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		_, err = js.AccountInfo()
		if err != nil {
			continue
		}
		return
	}
	t.Fatalf("Timeout waiting for JS to be ready: %v", err)
}

func checkFor(t *testing.T, totalWait, sleepDur time.Duration, f func() error) {
	t.Helper()
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		t.Fatal(err.Error())
	}
}

func checkSubsPending(t *testing.T, sub *nats.Subscription, numExpected int) {
	t.Helper()
	checkFor(t, 4*time.Second, 20*time.Millisecond, func() error {
		if nmsgs, _, err := sub.Pending(); err != nil || nmsgs != numExpected {
			return fmt.Errorf("Did not receive correct number of messages: %d vs %d", nmsgs, numExpected)
		}
		return nil
	})
}

func TestJetStream_ClusterPlacement(t *testing.T) {
	size := 3

	t.Run("default cluster", func(t *testing.T) {
		cluster := "PLC1"
		withJSCluster(t, cluster, size, func(t *testing.T, nodes ...*jsServer) {
			srvA := nodes[0]
			nc, js := jsClient(t, srvA.Server)
			defer nc.Close()

			var err error

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
			nc, js := jsClient(t, srvA.Server)
			defer nc.Close()

			var err error

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
			nc, js := jsClient(t, srvA.Server)
			defer nc.Close()

			var err error

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
	withJSServer(t, testJetStreamMirror_Source)
}

func testJetStreamMirror_Source(t *testing.T, nodes ...*jsServer) {
	srvA := nodes[0]
	nc, js := jsClient(t, srvA.Server)
	defer nc.Close()

	var err error

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

		t.Run("consume from mirror", func(t *testing.T) {
			sub, err := js.SubscribeSync("origin", nats.BindStream("m1"))
			if err != nil {
				t.Fatal(err)
			}

			mmsgs := make([]*nats.Msg, 0)
			for i := 0; i < totalMsgs; i++ {
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
			t.Fatal("Expected stream not found error", err.Error())
		}
	})

	t.Run("bind to stream with wrong subject fails", func(t *testing.T) {
		_, err := js.SubscribeSync("secret", nats.BindStream("origin"))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err.Error() != `nats: consumer filter subject is not a valid subset of the interest subjects` {
			t.Fatal("Expected stream not found error")
		}
	})

	t.Run("bind to origin stream", func(t *testing.T) {
		// This would only avoid the stream names lookup.
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

		got = int(updated.Config.MaxMsgs)
		expected = int(config.MaxMsgs)
		if got != expected {
			t.Errorf("Expected %v, got: %v", expected, got)
		}
	})

	t.Run("create sourced stream from origin", func(t *testing.T) {
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
				msg, err = js.GetMsg(streamName, seq)
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

		si, err := js.StreamInfo(streamName)
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
			sub, err := js.SubscribeSync("origin", nats.BindStream(streamName))
			if err != nil {
				t.Fatal(err)
			}

			mmsgs := make([]*nats.Msg, 0)
			for i := 0; i < totalMsgs; i++ {
				msg, err := sub.NextMsg(2 * time.Second)
				if err != nil {
					t.Error(err)
				}
				meta, err := msg.Metadata()
				if err != nil {
					t.Error(err)
				}
				if meta.Stream != streamName {
					t.Errorf("Expected m1, got: %v", meta.Stream)
				}
				mmsgs = append(mmsgs, msg)
			}
			if len(mmsgs) != totalMsgs {
				t.Errorf("Expected to consume %v msgs, got: %v", totalMsgs, len(mmsgs))
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
				withJSClusterAndStream(t, name, n, stream, testJetStream_ClusterMultipleQueueSubscribe)
			})

			t.Run(fmt.Sprintf("psub n=%d r=%d", n, r), func(t *testing.T) {
				name := fmt.Sprintf("PSUBN%d%d", n, r)
				stream := &nats.StreamConfig{
					Name:     name,
					Replicas: n,
				}
				withJSClusterAndStream(t, name, n, stream, testJetStream_ClusterMultiplePullSubscribe)
			})
		}
	}
}

func testJetStream_ClusterMultipleQueueSubscribe(t *testing.T, subject string, srvs ...*jsServer) {
	srv := srvs[0]
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	defer nc.Close()

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

	// We are testing auto-bind here so create one first and expect others to bind to it.
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
			for attempt := 0; attempt < 5; attempt++ {
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
		// Unblock the main context when done.
		wg.Wait()
		done()
	}()

	wg.Wait()
	for i := 0; i < size*2; i++ {
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

func testJetStream_ClusterMultiplePullSubscribe(t *testing.T, subject string, srvs ...*jsServer) {
	srv := srvs[0]
	nc, js := jsClient(t, srv.Server)
	defer nc.Close()

	var wg sync.WaitGroup
	ctx, done := context.WithTimeout(context.Background(), 2*time.Second)
	defer done()

	size := 5
	subs := make([]*nats.Subscription, size)
	errCh := make(chan error, size)
	for i := 0; i < size; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			var sub *nats.Subscription
			var err error
			for attempt := 0; attempt < 5; attempt++ {
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
		// Unblock the main context when done.
		wg.Wait()
		done()
	}()

	wg.Wait()
	for i := 0; i < size*2; i++ {
		js.Publish(subject, []byte("test"))
	}

	delivered := 0
	for i, sub := range subs {
		if sub == nil {
			continue
		}
		for attempt := 0; attempt < 4; attempt++ {
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

func TestJetStream_ClusterReconnect(t *testing.T) {
	t.Skip("This test need to be revisited")
	n := 3
	replicas := []int{1, 3}

	t.Run("pull sub", func(t *testing.T) {
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
		for _, r := range replicas {
			t.Run(fmt.Sprintf("n=%d r=%d", n, r), func(t *testing.T) {
				stream := &nats.StreamConfig{
					Name:     fmt.Sprintf("bar-r%d", r),
					Replicas: r,
				}
				withJSClusterAndStream(t, fmt.Sprintf("QSUBR%d", r), n, stream, testJetStream_ClusterReconnectDurableQueueSubscriber)
			})
		}
	})
}

func testJetStream_ClusterReconnectDurableQueueSubscriber(t *testing.T, subject string, srvs ...*jsServer) {
	var (
		srvA          = srvs[0]
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
		nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
			t.Logf("WARN: Got error %v", err)
			if info, ok := err.(*nats.ErrConsumerSequenceMismatch); ok {
				t.Logf("WARN: %+v", info)
			}
			// Take out this QueueSubscriber from the group.
			sub.Drain()
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
				srvA.WaitForShutdown()
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
		}, nats.Durable(dname), nats.AckWait(5*time.Second), nats.ManualAck(), nats.IdleHeartbeat(100*time.Millisecond))

		if err != nil && (err != nats.ErrTimeout && err != context.DeadlineExceeded) {
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
			if err != nil && (err == nats.ErrNoStreamResponse || err == nats.ErrTimeout || err.Error() == `raft: not leader`) {
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

	// Wait a bit to get heartbeats.
	time.Sleep(2 * time.Second)

	// Drain to allow AckSync response to be received.
	nc.Drain()

	got := len(msgs)
	if got != totalMsgs {
		t.Logf("WARN: Expected %v, got: %v (failed publishes: %v)", totalMsgs, got, failedPubs)
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
		sub, err := js.PullSubscribe(subject, "d1", nats.PullMaxWaiting(5))
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
			// Server will shutdown after a couple of messages which will result
			// in empty messages with an status unavailable error.
			msgs, err := sub.Fetch(1, nats.MaxWait(2*time.Second))
			if err == nats.ErrNoResponders || err == nats.ErrTimeout {
				// Backoff before asking for more messages.
				time.Sleep(100 * time.Millisecond)
				continue NextMsg
			} else if err != nil {
				t.Errorf("Unexpected error: %v", err)
				continue NextMsg
			}
			msg := msgs[0]
			if len(msg.Data) == 0 && msg.Header.Get("Status") == "503" {
				t.Fatal("Got 503 JetStream API message!")
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

func TestJetStreamPullSubscribeOptions(t *testing.T) {
	withJSCluster(t, "FOPTS", 3, testJetStreamFetchOptions)
}

func testJetStreamFetchOptions(t *testing.T, srvs ...*jsServer) {
	srv := srvs[0]
	nc, js := jsClient(t, srv.Server)
	defer nc.Close()

	var err error

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
		for i := 0; i < totalMsgs; i++ {
			payload := fmt.Sprintf("i:%d", i)
			_, err := js.Publish(subject, []byte(payload))
			if err != nil {
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

		// Next fetch will timeout since no more messages.
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
		err = sub.Drain()
		if err != nil {
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

		err = sub.Unsubscribe()
		if err != nil {
			t.Fatal(err)
		}

		_, err = sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != nats.ErrBadSubscription {
			t.Fatalf("Unexpected error: %v", err)
		}
	})

	t.Run("max waiting timeout", func(t *testing.T) {
		defer js.PurgeStream(subject)

		expected := 10
		sendMsgs(t, expected)

		sub, err := js.PullSubscribe(subject, "max-waiting")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		// Poll more than the default max of waiting/inflight pull requests,
		// so that We will get only 408 timeout errors.
		errCh := make(chan error, 1024)
		defer close(errCh)
		var wg sync.WaitGroup
		for i := 0; i < 1024; i++ {
			wg.Add(1)

			go func() {
				_, err := sub.Fetch(1, nats.MaxWait(500*time.Millisecond))
				defer wg.Done()
				if err != nil {
					errCh <- err
				}
			}()
		}
		wg.Wait()

		select {
		case <-time.After(1 * time.Second):
			t.Fatal("Expected RequestTimeout (408) error due to many inflight pulls")
		case err := <-errCh:
			if err != nil && (err.Error() != `nats: Request Timeout` && err != nats.ErrTimeout) {
				t.Errorf("Expected request timeout fetching next message, got: %+v", err)
			}
		}
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
				err = msg.AckSync()
				if err != nil {
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

		// There should only be timeout errors since no more messages.
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
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	// Make sure we get a proper failure when no stream is present.
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
		// Should be able to get the message back to resend, etc.
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

	// Now create a stream and expect a PubAck from <-OK().
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

	// Make sure we can register an async err handler for these.
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

	// Now test that we can set our window for the JetStream context to limit number of outstanding messages.
	js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 100; i++ {
		if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if np := js.PublishAsyncPending(); np > 10 {
			t.Fatalf("Expected num pending to not exceed 10, got %d", np)
		}
	}

	// Now test that we can wait on all prior outstanding if we want.
	js, err = nc.JetStream(nats.PublishAsyncMaxPending(10))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for i := 0; i < 500; i++ {
		if _, err = js.PublishAsync("bar", []byte("Hello JS ASYNC PUB")); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	select {
	case <-js.PublishAsyncComplete():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	// Check invalid options
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
}

func TestJetStreamPublishAsyncPerf(t *testing.T) {
	// Comment out below to run this benchmark.
	t.SkipNow()

	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// 64 byte payload.
	msg := make([]byte, 64)
	rand.Read(msg)

	// Setup error handler.
	var errors uint32
	errHandler := func(js nats.JetStream, originalMsg *nats.Msg, err error) {
		t.Logf("Got an async err: %v", err)
		atomic.AddUint32(&errors, 1)
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
	for i := 0; i < toSend; i++ {
		if _, err = js.PublishAsync("B", msg); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	select {
	case <-js.PublishAsyncComplete():
		if ne := atomic.LoadUint32(&errors); ne > 0 {
			t.Fatalf("Got unexpected errors publishing")
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not receive completion signal")
	}

	tt := time.Since(start)
	fmt.Printf("Took %v to send %d msgs\n", tt, toSend)
	fmt.Printf("%.0f msgs/sec\n\n", float64(toSend)/tt.Seconds())
}

func TestJetStreamBindConsumer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

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

	for i := 0; i < 25; i++ {
		js.Publish("foo", []byte("hi"))
	}

	// Both stream and consumer names are required for bind only.
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

	// Pull consumer
	_, err = js.PullSubscribe("foo", "pull", nats.Bind("foo", "pull"))
	if err == nil || !errors.Is(err, nats.ErrConsumerNotFound) {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Push consumer
	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
		Durable:        "push",
		AckPolicy:      nats.AckExplicitPolicy,
		DeliverSubject: nats.NewInbox(),
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Push Consumer Bind Only
	sub, err := js.SubscribeSync("foo", nats.Bind("foo", "push"))
	if err != nil {
		t.Fatal(err)
	}
	// Ambiguous declaration should not be allowed.
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

	// Duplicate stream name is fine.
	sub, err = js.SubscribeSync("foo", nats.BindStream("foo"), nats.Bind("foo", "push"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Cannot have 2 instances for same durable
	_, err = js.SubscribeSync("foo", nats.Durable("push"))
	if err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Cannot start a queue sub since plain durable is active
	_, err = js.QueueSubscribeSync("foo", "wq", nats.Durable("push"))
	if err == nil || !strings.Contains(err.Error(), "without a deliver group") {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub.Unsubscribe()
	checkConsInactive()

	// Create a queue sub
	_, err = js.QueueSubscribeSync("foo", "wq1", nats.Durable("qpush"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Can't create a plain sub on that durable
	_, err = js.SubscribeSync("foo", nats.Durable("qpush"))
	if err == nil || !strings.Contains(err.Error(), "cannot create a subscription for a consumer with a deliver group") {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Try to attach different queue group
	_, err = js.QueueSubscribeSync("foo", "wq2", nats.Durable("qpush"))
	if err == nil || !strings.Contains(err.Error(), "cannot create a queue subscription") {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Pull consumer
	_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
		Durable:   "pull",
		AckPolicy: nats.AckExplicitPolicy,
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Pull consumer can bind without create using only the stream name (since durable is required argument).
	_, err = js.PullSubscribe("foo", "pull", nats.Bind("foo", "pull"))
	if err != nil {
		t.Fatal(err)
	}

	// Prevent binding to durable that is from a wrong type.
	_, err = js.PullSubscribe("foo", "push", nats.Bind("foo", "push"))
	if err != nats.ErrPullSubscribeToPushConsumer {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = js.SubscribeSync("foo", nats.Bind("foo", "pull"))
	if err != nats.ErrPullSubscribeRequired {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Create ephemeral consumer
	sub1, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	cinfo, err := sub1.ConsumerInfo()
	if err != nil {
		t.Fatal(err)
	}

	// Cannot bind to ephemeral consumer because it is active.
	_, err = js.SubscribeSync("foo", nats.Bind("foo", cinfo.Name))
	if err == nil || !strings.Contains(err.Error(), "already bound") {
		t.Fatalf("Unexpected error: %v", err)
	}

	// However, one can create an ephemeral Queue subscription and bind several members to it.
	sub2, err := js.QueueSubscribeSync("foo", "wq3")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	// Consumer all
	for i := 0; i < 25; i++ {
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
	for i := 0; i < 100; i++ {
		js.Publish("foo", []byte("new"))
	}
	// We expect sub3 to at least get a message
	if _, err := sub3.NextMsg(time.Second); err != nil {
		t.Fatalf("Second member failed to get a message: %v", err)
	}
}

func TestJetStreamDomain(t *testing.T) {
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: { domain: ABC }
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	// JS with custom domain
	jsd, err := nc.JetStream(nats.Domain("ABC"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	info, err := jsd.AccountInfo()
	if err != nil {
		t.Error(err)
	}
	got := info.Domain
	expected := "ABC"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	if _, err = jsd.AddStream(&nats.StreamConfig{Name: "foo"}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	jsd.Publish("foo", []byte("first"))

	sub, err := jsd.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got = string(msg.Data)
	expected = "first"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	// JS without explicit bound domain should also work.
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	info, err = js.AccountInfo()
	if err != nil {
		t.Error(err)
	}
	got = info.Domain
	expected = "ABC"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	js.Publish("foo", []byte("second"))

	sub2, err := js.SubscribeSync("foo")
	if err != nil {
		t.Fatal(err)
	}
	msg, err = sub2.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got = string(msg.Data)
	expected = "first"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	msg, err = sub2.NextMsg(time.Second)
	if err != nil {
		t.Fatal(err)
	}
	got = string(msg.Data)
	expected = "second"
	if got != expected {
		t.Errorf("Got %v, expected: %v", got, expected)
	}

	// Using different domain not configured is an error.
	jsb, err := nc.JetStream(nats.Domain("XYZ"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = jsb.AccountInfo()
	if err != nats.ErrJetStreamNotEnabled {
		t.Errorf("Unexpected error: %v", err)
	}
}

// Test that we properly enfore per subject msg limits.
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
			s := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, s)

			// Client for API requests.
			nc, js := jsClient(t, s)
			defer nc.Close()

			var err error

			_, err = js.AddStream(c.mconfig)
			if err != nil {
				t.Fatalf("Unexpected error adding stream: %v", err)
			}
			defer js.DeleteStream(c.mconfig.Name)

			pubAndCheck := func(subj string, num int, expectedNumMsgs uint64) {
				t.Helper()
				for i := 0; i < num; i++ {
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
			// Now make sure our per subject limits kick in..
			pubAndCheck("foo", 2, 5)
			pubAndCheck("baz.22", 5, 10)
			pubAndCheck("baz.33", 5, 15)
			// We are maxed so totals should be same no matter what we add here.
			pubAndCheck("baz.22", 5, 15)
			pubAndCheck("baz.33", 5, 15)

			// Now purge and make sure all is still good.
			if err := js.PurgeStream(c.mconfig.Name); err != nil {
				t.Fatalf("Unexpected purge error: %v", err)
			}
			pubAndCheck("foo", 1, 1)
			pubAndCheck("foo", 4, 5)
			pubAndCheck("baz.22", 5, 10)
			pubAndCheck("baz.33", 5, 15)
		})
	}
}

func TestJetStreamDrainFailsToDeleteConsumer(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	errCh := make(chan error, 1)
	nc, err := nats.Connect(s.ClientURL(), nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
		select {
		case errCh <- err:
		default:
		}
	}))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

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

	// Initiate the drain... it won't complete because we have blocked the
	// message callback.
	sub.Drain()

	// Now delete the JS consumer
	if err := js.DeleteConsumer("TEST", "dur"); err != nil {
		t.Fatalf("Error deleting consumer: %v", err)
	}

	// Now unblock and make sure we get the async error
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
	conf := createConfFile(t, []byte(`
		listen: 127.0.0.1:-1
		jetstream: {domain: "HUB"}
	`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Storage:  nats.MemoryStorage,
		Subjects: []string{"foo"},
	}
	if _, err := js.AddStream(cfg); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	pa, err := js.Publish("foo", []byte("msg"))
	if err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if pa.Domain != "HUB" {
		t.Fatalf("Expected PubAck to have domain of %q, got %q", "HUB", pa.Domain)
	}
}

func TestJetStreamStreamAndConsumerDescription(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

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
}

func TestJetStreamMsgSubjectRewrite(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

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
		t.Fatalf("Unexepcted data: %q", msg.Data)
	}
}

func TestJetStreamPullSubscribeFetchContext(t *testing.T) {
	withJSCluster(t, "PULLCTX", 3, testJetStreamFetchContext)
}

func testJetStreamFetchContext(t *testing.T, srvs ...*jsServer) {
	srv := srvs[0]
	nc, js := jsClient(t, srv.Server)
	defer nc.Close()

	var err error

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
		for i := 0; i < totalMsgs; i++ {
			payload := fmt.Sprintf("i:%d", i)
			_, err := js.Publish(subject, []byte(payload))
			if err != nil {
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

		// No messages at this point.
		ctx, cancel = context.WithTimeout(ctx, 250*time.Millisecond)
		defer cancel()

		_, err = sub.Fetch(1, nats.Context(ctx))
		if err != context.DeadlineExceeded {
			t.Errorf("Expected deadline exceeded fetching next message, got: %v", err)
		}

		// Send more messages then pull them with a new context
		expected = 5
		sendMsgs(t, expected)

		ctx, cancel = context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()

		// Single message fetch.
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

		// Fetch multiple messages.
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
		// New JS context with slightly shorter timeout than default.
		js, err = nc.JetStream(nats.MaxWait(2 * time.Second))
		if err != nil {
			t.Fatal(err)
		}

		sub, err := js.PullSubscribe(subject, "batch-cancel-ctx")
		if err != nil {
			t.Fatal(err)
		}
		defer sub.Unsubscribe()

		// Parent context
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Fetch all the messages as needed.
		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		total := info.NumPending

		// Child context with timeout with the same duration as JS context timeout
		// will be created to fetch next message.
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

		// Fetch the rest using same cancellation context.
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

		// Fetch more messages and wait for timeout since there are none.
		_, err = sub.Fetch(expected, nats.Context(ctx))
		if err == nil {
			t.Fatal("Unexpected success")
		}
		if err != context.DeadlineExceeded {
			t.Fatalf("Expected deadline exceeded fetching next message, got: %v", err)
		}

		// Original cancellation context is not yet canceled, it should still work.
		if ctx.Err() != nil {
			t.Fatalf("Expected no errors in original cancellation context, got: %v", ctx.Err())
		}

		// Should be possible to use the same context again.
		sendMsgs(t, 5)

		// Get the next message to leave 4 pending.
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

		// Cancel finally.
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
}

func TestJetStreamSubscribeContextCancel(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	var err error

	// Create the stream using our client API.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo", "bar", "baz", "foo.*"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	toSend := 100
	for i := 0; i < toSend; i++ {
		js.Publish("bar", []byte("foo"))
	}

	t.Run("cancel unsubscribes and deletes ephemeral", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		ch := make(chan *nats.Msg, 100)
		sub, err := js.Subscribe("bar", func(msg *nats.Msg) {
			ch <- msg

			// Cancel will unsubscribe and remove the subscription
			// of the consumer.
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

		// Consumer should not be present since unsubscribe already called.
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
		err = sub.Unsubscribe()
		if err != nil {
			t.Fatal(err)
		}

		// Consumer should not be present since unsubscribe already called.
		checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
			info, err := sub.ConsumerInfo()
			if err != nil && err == nats.ErrConsumerNotFound {
				return nil
			}
			return fmt.Errorf("Consumer still active, got: %v (info=%+v)", err, info)
		})
	})
}

func TestJetStreamClusterStreamLeaderChangeClientErr(t *testing.T) {
	cfg := &nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
		Replicas: 3,
	}

	withJSClusterAndStream(t, "R3S", 3, cfg, func(t *testing.T, stream string, servers ...*jsServer) {
		// We want to make sure the worse thing seen by the lower levels during a leadership change is NoResponders.
		// We will have three concurrent contexts going on.
		// 1. Leadership Changes every 500ms.
		// 2. Publishing messages to the stream every 10ms.
		// 3. StreamInfo calls every 15ms.
		expires := time.Now().Add(5 * time.Second)
		var wg sync.WaitGroup
		wg.Add(3)

		randServer := func() *server.Server {
			return servers[mrand.Intn(len(servers))].Server
		}

		// Leadership changes.
		go func() {
			defer wg.Done()
			nc, js := jsClient(t, randServer())
			defer nc.Close()

			sds := fmt.Sprintf(server.JSApiStreamLeaderStepDownT, "TEST")
			for time.Now().Before(expires) {
				time.Sleep(500 * time.Millisecond)
				si, err := js.StreamInfo("TEST")
				expectOk(t, err)
				_, err = nc.Request(sds, nil, time.Second)
				expectOk(t, err)

				// Wait on new leader.
				checkFor(t, 5*time.Second, 50*time.Millisecond, func() error {
					si, err = js.StreamInfo("TEST")
					if err != nil {
						return err
					}
					if si.Cluster.Leader == "" {
						return fmt.Errorf("No leader yet")
					}
					return nil
				})
			}
		}()

		// Published every 10ms
		toc := 0
		go func() {
			defer wg.Done()
			nc, js := jsClient(t, randServer())
			defer nc.Close()

			for time.Now().Before(expires) {
				time.Sleep(10 * time.Millisecond)
				_, err := js.Publish("foo", []byte("OK"))
				if err == nats.ErrTimeout {
					toc++
					continue
				}
				expectOk(t, err)
			}
		}()

		// StreamInfo calls.
		go func() {
			defer wg.Done()
			nc, js := jsClient(t, randServer())
			defer nc.Close()

			for time.Now().Before(expires) {
				time.Sleep(15 * time.Millisecond)
				_, err := js.StreamInfo("TEST")
				expectOk(t, err)
			}
		}()

		wg.Wait()

		// An occasional timeout can occur, but should be 0 or maybe 1 with ~10 leadership changes per test run.
		if toc > 1 {
			t.Fatalf("Got too many timeout errors from publish: %d", toc)
		}
	})
}
