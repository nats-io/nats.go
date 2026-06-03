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
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

func TestJetStreamNotEnabled(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.AccountInfo(); err != nats.ErrJetStreamNotEnabled {
			t.Fatalf("Did not get the proper error, got %v", err)
		}
	})
}

func TestJetStreamErrors(t *testing.T) {
	t.Run("API error", func(t *testing.T) {
		c := newTester(t)
		accounts := `accounts: {
  JS: {
    jetstream: enabled
    users: [ {user: dlc, password: foo} ]
  },
  IU: {
    users: [ {user: rip, password: bar} ]
  },
}`
		inst := c.CreateServer(t, true,
			testservice.WithAccounts(accounts),
			testservice.WithAuthorization("no_auth_user: rip"),
			testservice.WithSystemAccount(noSystemAccountBody()),
		)
		t.Cleanup(func() { inst.Destroy(t) })

		// wait for JS to be ready using the JS-enabled user
		jsNC := dialInstance(t, inst, nats.UserInfo("dlc", "foo"))
		c.WaitForJetStream(t, jsNC)
		jsNC.Close()

		nc := dialInstance(t, inst)
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AccountInfo()
		// check directly to var (backwards compatible)
		if err != nats.ErrJetStreamNotEnabledForAccount {
			t.Fatalf("Did not get the proper error, got %v", err)
		}

		// matching via errors.Is
		if ok := errors.Is(err, nats.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected ErrJetStreamNotEnabledForAccount")
		}

		// matching wrapped via error.Is
		err2 := fmt.Errorf("custom error: %w", nats.ErrJetStreamNotEnabledForAccount)
		if ok := errors.Is(err2, nats.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected wrapped ErrJetStreamNotEnabled")
		}

		// via classic type assertion.
		jserr, ok := err.(nats.JetStreamError)
		if !ok {
			t.Fatal("Expected a JetStreamError")
		}
		expected := nats.JSErrCodeJetStreamNotEnabledForAccount
		if jserr.APIError().ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, jserr.APIError().ErrorCode)
		}
		if jserr.APIError() == nil {
			t.Fatal("Expected APIError")
		}

		// matching to interface via errors.As(...)
		var apierr nats.JetStreamError
		ok = errors.As(err, &apierr)
		if !ok {
			t.Fatal("Expected a JetStreamError")
		}
		if apierr.APIError() == nil {
			t.Fatal("Expected APIError")
		}
		if apierr.APIError().ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, apierr.APIError().ErrorCode)
		}
		expectedMessage := "nats: jetstream not enabled for account"
		if apierr.Error() != expectedMessage {
			t.Fatalf("Expected: %v, got: %v", expectedMessage, apierr.Error())
		}

		// an APIError also implements the JetStreamError interface.
		var _ nats.JetStreamError = &nats.APIError{}

		// matching arbitrary custom error via errors.Is(...)
		customErr := &nats.APIError{ErrorCode: expected}
		if ok := errors.Is(customErr, nats.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected wrapped ErrJetStreamNotEnabledForAccount")
		}
		customErr = &nats.APIError{ErrorCode: 1}
		if ok := errors.Is(customErr, nats.ErrJetStreamNotEnabledForAccount); ok {
			t.Fatal("Expected to not match ErrJetStreamNotEnabled")
		}
		var cerr nats.JetStreamError
		if ok := errors.As(customErr, &cerr); !ok {
			t.Fatal("Expected custom error to be a JetStreamError")
		}

		// matching to concrete type via errors.As(...)
		var aerr *nats.APIError
		ok = errors.As(err, &aerr)
		if !ok {
			t.Fatal("Expected an APIError")
		}
		if aerr.ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, aerr.ErrorCode)
		}
		expectedMessage = "nats: jetstream not enabled for account"
		if aerr.Error() != expectedMessage {
			t.Fatalf("Expected: %v, got: %v", expectedMessage, apierr.Error())
		}
	})

	t.Run("test non-api error", func(t *testing.T) {
		withJSServer(t, func(t *testing.T, nc *nats.Conn) {
			js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			// stream with empty name
			_, err = js.AddStream(&nats.StreamConfig{})
			if err == nil {
				t.Fatalf("Expected error, got nil")
			}

			// check directly to var (backwards compatible)
			if err != nats.ErrStreamNameRequired {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidStreamName, err)
			}

			// matching via errors.Is
			if ok := errors.Is(err, nats.ErrStreamNameRequired); !ok {
				t.Fatalf("Expected: %v; got: %v", nats.ErrStreamNameRequired, err)
			}

			// matching wrapped via error.Is
			err2 := fmt.Errorf("custom error: %w", nats.ErrStreamNameRequired)
			if ok := errors.Is(err2, nats.ErrStreamNameRequired); !ok {
				t.Fatal("Expected wrapped ErrStreamNameRequired")
			}

			// via classic type assertion.
			jserr, ok := err.(nats.JetStreamError)
			if !ok {
				t.Fatal("Expected a JetStreamError")
			}
			if jserr.APIError() != nil {
				t.Fatalf("Expected: empty APIError; got: %v", jserr.APIError())
			}

			// matching to interface via errors.As(...)
			var jserr2 nats.JetStreamError
			ok = errors.As(err, &jserr2)
			if !ok {
				t.Fatal("Expected a JetStreamError")
			}
			if jserr2.APIError() != nil {
				t.Fatalf("Expected: empty APIError; got: %v", jserr2.APIError())
			}
			expectedMessage := "nats: stream name is required"
			if jserr2.Error() != expectedMessage {
				t.Fatalf("Expected: %v, got: %v", expectedMessage, jserr2.Error())
			}

			// matching to concrete type via errors.As(...)
			var aerr *nats.APIError
			ok = errors.As(err, &aerr)
			if ok {
				t.Fatal("Expected ErrStreamNameRequired not to map to APIError")
			}
		})
	})
}

func TestJetStreamPublish(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Make sure we get a proper failure when no stream is present.
		_, err = js.Publish("foo", []byte("Hello JS"))
		if err != nats.ErrNoStreamResponse {
			t.Fatalf("Expected a no stream error but got %v", err)
		}

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
	})
}

func TestPublishWithTTL(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name: "foo", Subjects: []string{"FOO.*"}, MaxMsgSize: 64, AllowMsgTTL: true})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ack, err := js.Publish("FOO.1", []byte("msg"), nats.MsgTTL(1*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		gotMsg, err := js.GetMsg("foo", ack.Sequence)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ttl := gotMsg.Header.Get("Nats-TTL"); ttl != "1s" {
			t.Fatalf("Expected message to have TTL header set to 1s; got: %s", ttl)
		}
		time.Sleep(1500 * time.Millisecond)
		_, err = js.GetMsg("foo", ack.Sequence)
		if !errors.Is(err, nats.ErrMsgNotFound) {
			t.Fatalf("Expected not found error; got: %v", err)
		}
	})
}

func TestMsgDeleteMarkerMaxAge(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name: "foo", Subjects: []string{"FOO.*"}, AllowMsgTTL: true, SubjectDeleteMarkerTTL: 50 * time.Second, MaxAge: 1 * time.Second})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.Publish("FOO.1", []byte("msg1"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		time.Sleep(1500 * time.Millisecond)
		gotMsg, err := js.GetLastMsg("foo", "FOO.1")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ttlMarker := gotMsg.Header.Get("Nats-Marker-Reason"); ttlMarker != "MaxAge" {
			t.Fatalf("Expected message to have Marker-Reason header set to MaxAge; got: %s", ttlMarker)
		}
		if ttl := gotMsg.Header.Get("Nats-TTL"); ttl != "50s" {
			t.Fatalf("Expected message to have Nats-TTL header set to 50s; got: %s", ttl)
		}
	})
}

func TestPublishAsyncWithTTL(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name: "foo", Subjects: []string{"FOO.*"}, MaxMsgSize: 64, AllowMsgTTL: true})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		paf, err := js.PublishAsync("FOO.1", []byte("msg"), nats.MsgTTL(1*time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		var ack *nats.PubAck
		select {
		case ack = <-paf.Ok():
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive ack")
		}

		gotMsg, err := js.GetMsg("foo", ack.Sequence)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if ttl := gotMsg.Header.Get("Nats-TTL"); ttl != "1s" {
			t.Fatalf("Expected message to have TTL header set to 1s; got: %s", ttl)
		}
		time.Sleep(1500 * time.Millisecond)
		_, err = js.GetMsg("foo", ack.Sequence)
		if !errors.Is(err, nats.ErrMsgNotFound) {
			t.Fatalf("Expected not found error; got: %v", err)
		}
	})
}

func TestJetStreamSubscribe(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		expectConsumers := func(t *testing.T, expected int) {
			t.Helper()
			checkFor(t, 2*time.Second, 15*time.Millisecond, func() error {
				var infos []*nats.ConsumerInfo
				for info := range js.Consumers("TEST") {
					infos = append(infos, info)
				}
				if len(infos) != expected {
					return fmt.Errorf("Expected %d consumers, got: %d", expected, len(infos))
				}
				return nil
			})
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"foo", "bar", "baz", "foo.*"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

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
		initialPending, err := sub.InitialConsumerPending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if initialPending != 0 {
			t.Fatalf("Expected no initial pending, got %d", initialPending)
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
		if err != nats.ErrInvalidConsumerName {
			t.Fatalf("Unexpected error: %v", err)
		}

		msg := []byte("Hello JS")

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
		for range toSend {
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

		initialPending, err = sub3.InitialConsumerPending()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if initialPending != 10 {
			t.Fatalf("Expected initial pending of 10, got %d", initialPending)
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatalf("Did not receive all of the messages in time")
		}

		// If we are here we have received all of the messages.
		// We hang the ConsumerInfo option off of the subscription, so we use that to check status.
		// We may need to retry this check since the acks sent by the client have to be processed
		// on the server.
		checkFor(t, 10*time.Second, 100*time.Millisecond, func() error {
			info, _ := sub3.ConsumerInfo()
			if info.Config.AckPolicy != nats.AckExplicitPolicy {
				t.Fatalf("Expected ack explicit policy, got %q", info.Config.AckPolicy)
			}
			if info.Delivered.Consumer != uint64(toSend) {
				return fmt.Errorf("Expected to have received all %d messages, got %d", toSend, info.Delivered.Consumer)
			}
			if info.AckFloor.Consumer != uint64(toSend) {
				return fmt.Errorf("Expected to have ack'd all %d messages, got ack floor of %d", toSend, info.AckFloor.Consumer)
			}
			return nil
		})
		sub3.Unsubscribe()
		sub2.Unsubscribe()
		sub1.Unsubscribe()
		expectConsumers(t, 0)

		// durable
		dname := "derek"
		sub, err = js.SubscribeSync("foo", nats.Durable(dname))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()
		expectConsumers(t, 1)

		info, _ := sub.ConsumerInfo()
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

		sub.Unsubscribe()
		expectConsumers(t, 0)

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

		// queue group on bar (durable=queue name)
		sub1, err = js.QueueSubscribeSync("bar", "v0")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub1.Unsubscribe()
		waitForPending(t, sub1, 10)
		expectConsumers(t, 1)

		if _, err = js.QueueSubscribeSync("baz", "v0"); err == nil {
			t.Fatal("Unexpected success")
		}

		if _, err = js.QueueSubscribeSync("bar", "v1", nats.Durable("v0")); err == nil {
			t.Fatal("Unexpected success")
		}
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
		if _, err := js.Subscribe("bar", nil); err != nats.ErrBadSubscription {
			t.Fatalf("Expected an error trying to create subscriber with nil callback, got %v", err)
		}

		// Since v2.7.0, we can create pull consumers with ephemerals.
		sub, err = js.PullSubscribe("bar", "")
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		sub.Unsubscribe()

		// Pull consumer with AckNone policy
		sub, err = js.PullSubscribe("bar", "", nats.AckNone())
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

		bmsgs, err := sub.Fetch(batch)
		if err != nil {
			t.Fatal(err)
		}

		if info, _ := sub.ConsumerInfo(); info.NumAckPending != batch || info.NumPending != uint64(batch) {
			t.Fatalf("Expected %d pending ack, and %d still waiting to be delivered, got %d and %d", batch, batch, info.NumAckPending, info.NumPending)
		}

		// Now go ahead and consume these and ack, but not ack+next.
		for i := range batch {
			m := bmsgs[i]
			err = m.AckSync()
			if err != nil {
				t.Fatal(err)
			}
		}
		checkFor(t, time.Second, 15*time.Millisecond, func() error {
			if info, _ := sub.ConsumerInfo(); info.AckFloor.Consumer != uint64(batch) {
				return fmt.Errorf("Expected ack floor to be %d, got %d", batch, info.AckFloor.Consumer)
			}
			return nil
		})
		waitForPending(t, sub, 0)

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
			msg.AckSync()
		}

		_, err = js.PullSubscribe("baz", "rip")
		if err != nats.ErrSubjectMismatch {
			t.Fatalf("Expected a %q error but got %q", nats.ErrSubjectMismatch, err)
		}

		for range toSend {
			js.Publish("bar", msg)
		}

		sub, err = js.PullSubscribe("bar", "rip")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()
		expectConsumers(t, 1)

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

		if _, err := js.SubscribeSync("baz", nats.Durable("test.durable")); err != nats.ErrInvalidConsumerName {
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
		expectConsumers(t, 4)

		for range toSend {
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

		expectConsumers(t, 4)

		sub, err = js.SubscribeSync("foo", nats.InactiveThreshold(-100*time.Millisecond))
		if err == nil || !strings.Contains(err.Error(), "invalid InactiveThreshold") {
			t.Fatalf("Expected error about invalid option, got %v", err)
		}

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

		nc2 := dialInstance(t, inst)
		defer nc2.Close()
		js2, err := nc2.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if ci, err := js2.ConsumerInfo("TEST", name); err == nil {
			t.Fatalf("Expected no consumer to exist, got %+v", ci)
		}
	})
}

func TestJetStreamSubscribe_SkipConsumerLookup(t *testing.T) {
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

		_, err = js.AddConsumer("TEST", &nats.ConsumerConfig{
			Name:           "cons",
			DeliverSubject: "_INBOX.foo",
			AckPolicy:      nats.AckExplicitPolicy,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// for checking whether subscribe looks up the consumer
		infoSub, err := nc.SubscribeSync("$JS.API.CONSUMER.INFO.TEST.*")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer infoSub.Unsubscribe()

		// for checking whether subscribe creates the consumer
		createConsSub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.>")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer createConsSub.Unsubscribe()
		t.Run("use Bind to skip consumer lookup and create", func(t *testing.T) {
			sub, err := js.SubscribeSync("", nats.Bind("TEST", "cons"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
				t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
			}

			if msg, err := createConsSub.NextMsg(50 * time.Millisecond); err == nil {
				t.Fatalf("Expected to skip consumer create; got message on %q", msg.Subject)
			}
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
				t.Fatalf("Expected to receive msg; got: %s", err)
			}
		})
		t.Run("use Durable, skip consumer lookup but overwrite the consumer", func(t *testing.T) {
			sub, err := js.SubscribeSync("foo", nats.Durable("cons"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
				t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
			}

			if _, err := createConsSub.NextMsg(50 * time.Millisecond); err != nil {
				t.Fatalf("Expected consumer create; got: %s", err)
			}
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
				t.Fatalf("Expected to receive msg; got: %s", err)
			}
		})
		t.Run("create new consumer with Durable, skip lookup", func(t *testing.T) {
			sub, err := js.SubscribeSync("foo", nats.Durable("pp"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo1"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
				t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
			}

			if _, err := createConsSub.NextMsg(50 * time.Millisecond); err != nil {
				t.Fatalf("Expected consumer create; got: %s", err)
			}
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
				t.Fatalf("Expected to receive msg; got: %s", err)
			}
		})
		t.Run("create new consumer with ConsumerName, skip lookup", func(t *testing.T) {
			sub, err := js.SubscribeSync("foo", nats.ConsumerName("pp"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo1"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
				t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
			}

			if _, err := createConsSub.NextMsg(50 * time.Millisecond); err != nil {
				t.Fatalf("Expected consumer create; got: %s", err)
			}
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
				t.Fatalf("Expected to receive msg; got: %s", err)
			}
		})

		t.Run("create ephemeral consumer, SkipConsumerLookup has no effect", func(t *testing.T) {
			sub, err := js.SubscribeSync("foo", nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo2"))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			if msg, err := infoSub.NextMsg(50 * time.Millisecond); err == nil {
				t.Fatalf("Expected to skip consumer lookup; got message on %q", msg.Subject)
			}

			if _, err := createConsSub.NextMsg(50 * time.Millisecond); err != nil {
				t.Fatalf("Expected consumer create; got: %s", err)
			}
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			if _, err := sub.NextMsg(100 * time.Millisecond); err != nil {
				t.Fatalf("Expected to receive msg; got: %s", err)
			}
		})
		t.Run("attempt to update ack policy of existing consumer", func(t *testing.T) {
			_, err := js.SubscribeSync("foo", nats.Durable("cons"), nats.SkipConsumerLookup(), nats.DeliverSubject("_INBOX.foo"), nats.AckAll())
			if err == nil || !strings.Contains(err.Error(), "ack policy can not be updated") {
				t.Fatalf("Expected update consumer error, got: %v", err)
			}
		})
	})
}

func TestPullSubscribeFetchWithHeartbeat(t *testing.T) {
	t.Skip("Since v2.10.26 server sends no responders if the consumer is deleted, we need to figure out how else to test missing heartbeats")
}

func TestPullSubscribeConsumerDoesNotExist(t *testing.T) {
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

		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		defer sub.Unsubscribe()

		info, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if err := js.DeleteConsumer("TEST", info.Name); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = sub.Fetch(5)
		if !errors.Is(err, nats.ErrNoResponders) {
			t.Fatalf("Expected no responders error; got: %v", err)
		}

		msgs, err := sub.FetchBatch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		select {
		case _, ok := <-msgs.Messages():
			if ok {
				t.Fatalf("Expected no messages")
			}
		case <-time.After(time.Second):
			t.Fatalf("Timeout waiting for messages")
		}

		if !errors.Is(msgs.Error(), nats.ErrNoResponders) {
			t.Fatalf("Expected no responders error; got: %v", msgs.Error())
		}
	})
}

func TestPullSubscribeFetchDrain(t *testing.T) {
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

		defer js.PurgeStream("TEST")
		sub, err := js.PullSubscribe("foo", "")
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for range 100 {
			if _, err := js.Publish("foo", []byte("msg")); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		}
		// fill buffer with messages
		cinfo, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		nextSubject := fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.TEST.%s", cinfo.Name)
		replySubject := strings.Replace(sub.Subject, "*", "abc", 1)
		payload := `{"batch":10,"no_wait":true}`
		if err := nc.PublishRequest(nextSubject, replySubject, []byte(payload)); err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		time.Sleep(100 * time.Millisecond)

		sub.Drain()
		msgs, err := sub.Fetch(100)
		if err != nil {
			t.Fatalf("Unexpected error: %s", err)
		}
		for _, msg := range msgs {
			msg.Ack()
		}
		if len(msgs) != 10 {
			t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
		}

		_, err = sub.Fetch(10, nats.MaxWait(100*time.Millisecond))
		if !errors.Is(err, nats.ErrSubscriptionClosed) {
			t.Fatalf("Expected error: %s; got: %s", nats.ErrSubscriptionClosed, err)
		}
	})
}

func TestPullSubscribeFetchBatchWithHeartbeat(t *testing.T) {
	t.Skip("Since v2.10.26 server sends no responders if the consumer is deleted, we need to figure out how else to test missing heartbeats")
}

func TestPullSubscribeFetchBatch(t *testing.T) {
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

		t.Run("basic fetch", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 5 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			res, err := sub.FetchBatch(10)
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			go func() {
				time.Sleep(10 * time.Millisecond)
				for range 5 {
					js.Publish("foo", []byte("msg"))
				}
			}()
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
			}
			if res.Error() != nil {
				t.Fatalf("Unexpected error: %s", res.Error())
			}
			if len(msgs) != 10 {
				t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
			}
		})

		t.Run("multiple concurrent fetches", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 50 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			var r1, r2, r3 nats.MessageBatch
			started := &sync.WaitGroup{}
			started.Add(3)
			errs := make(chan error, 3)
			go func() {
				var err error
				r1, err = sub.FetchBatch(10)
				if err != nil {
					errs <- err
				}
				started.Done()
			}()
			go func() {
				var err error
				r2, err = sub.FetchBatch(10)
				if err != nil {
					errs <- err
				}
				started.Done()
			}()
			go func() {
				var err error
				r3, err = sub.FetchBatch(10)
				if err != nil {
					errs <- err
				}
				started.Done()
			}()
			started.Wait()

			select {
			case err := <-errs:
				t.Fatalf("Error initializing fetch: %s", err)
			default:
			}

			var msgsReceived int
			for msgsReceived < 30 {
				select {
				case <-r1.Messages():
					msgsReceived++
				case <-r2.Messages():
					msgsReceived++
				case <-r3.Messages():
					msgsReceived++
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout waiting for incoming messages")
				}
			}
			select {
			case <-r1.Done():
			case <-time.After(1 * time.Second):
				t.Fatalf("FetchBatch result channel should be closed after receiving all messages on r1")
			}
			select {
			case <-r2.Done():
			case <-time.After(1 * time.Second):
				t.Fatalf("FetchBatch result channel should be closed after receiving all messages on r2")
			}
			select {
			case <-r3.Done():
			case <-time.After(1 * time.Second):
				t.Fatalf("FetchBatch result channel should be closed after receiving all messages on r3")
			}
			if r1.Error() != nil {
				t.Fatalf("Unexpected error: %s", r1.Error())
			}
			if r2.Error() != nil {
				t.Fatalf("Unexpected error: %s", r2.Error())
			}
			if r3.Error() != nil {
				t.Fatalf("Unexpected error: %s", r3.Error())
			}
			if msgsReceived != 30 {
				t.Fatalf("Expected %d messages; got: %d", 30, msgsReceived)
			}
		})

		t.Run("deliver all, then consume", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 5 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			res, err := sub.FetchBatch(5)
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			time.Sleep(10 * time.Millisecond)
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
			}
			if res.Error() != nil {
				t.Fatalf("Unexpected error: %s", res.Error())
			}
			if len(msgs) != 5 {
				t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
			}
		})

		t.Run("fetch with context", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 5 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			res, err := sub.FetchBatch(10, nats.Context(ctx))
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			go func() {
				time.Sleep(10 * time.Millisecond)
				for range 5 {
					js.Publish("foo", []byte("msg"))
				}
			}()
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
			}
			if res.Error() != nil {
				t.Fatalf("Unexpected error: %s", res.Error())
			}
			if len(msgs) != 10 {
				t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
			}
		})

		t.Run("fetch subset of messages", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 10 {
				js.Publish("foo", []byte("msg"))
			}
			res, err := sub.FetchBatch(5)
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
			}
			if res.Error() != nil {
				t.Fatalf("Unexpected error: %s", res.Error())
			}
			if len(msgs) != 5 {
				t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
			}
		})

		t.Run("context timeout, no error", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 5 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
			defer cancel()
			res, err := sub.FetchBatch(10, nats.Context(ctx))
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
			}
			if res.Error() != nil {
				t.Fatalf("Unexpected error: %s", res.Error())
			}
			if len(msgs) != 5 {
				t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
			}
		})

		t.Run("request expired", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 5 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			res, err := sub.FetchBatch(10, nats.MaxWait(50*time.Millisecond))
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
			}
			if res.Error() != nil {
				t.Fatalf("Unexpected error: %s", res.Error())
			}
			if len(msgs) != 5 {
				t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
			}
		})

		t.Run("cancel context during fetch", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 5 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			res, err := sub.FetchBatch(10, nats.Context(ctx))
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			go func() {
				time.Sleep(200 * time.Millisecond)
				cancel()
			}()
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
			}
			if res.Error() == nil || !errors.Is(res.Error(), context.Canceled) {
				t.Fatalf("Expected error: %s; got: %s", nats.ErrConsumerDeleted, res.Error())
			}
			if len(msgs) != 5 {
				t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
			}
		})

		t.Run("remove durable consumer during fetch", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "cons")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 5 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			res, err := sub.FetchBatch(10)
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			go func() {
				time.Sleep(10 * time.Millisecond)
				js.DeleteConsumer("TEST", "cons")
			}()
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
			}
			if res.Error() == nil || !errors.Is(res.Error(), nats.ErrConsumerDeleted) {
				t.Fatalf("Expected error: %s; got: %s", nats.ErrConsumerDeleted, err)
			}
			if len(msgs) != 5 {
				t.Fatalf("Expected %d messages; got: %d", 5, len(msgs))
			}
		})

		t.Run("validation errors", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			_, err = sub.FetchBatch(-1)
			if !errors.Is(err, nats.ErrInvalidArg) {
				t.Errorf("Expected error: %s; got: %s", nats.ErrInvalidArg, err)
			}

			syncSub, err := js.SubscribeSync("foo")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			_, err = syncSub.FetchBatch(10)
			if !errors.Is(err, nats.ErrTypeSubscription) {
				t.Errorf("Expected error: %s; got: %s", nats.ErrTypeSubscription, err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
			defer cancel()
			_, err = sub.FetchBatch(10, nats.Context(ctx), nats.MaxWait(2*time.Second))
			if !errors.Is(err, nats.ErrContextAndTimeout) {
				t.Errorf("Expected error: %s; got: %s", nats.ErrContextAndTimeout, err)
			}

			_, err = sub.FetchBatch(10, nats.Context(context.Background()))
			if !errors.Is(err, nats.ErrNoDeadlineContext) {
				t.Errorf("Expected error: %s; got: %s", nats.ErrNoDeadlineContext, err)
			}
		})

		t.Run("close subscription", func(t *testing.T) {
			defer js.PurgeStream("TEST")
			sub, err := js.PullSubscribe("foo", "")
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			for range 100 {
				if _, err := js.Publish("foo", []byte("msg")); err != nil {
					t.Fatalf("Unexpected error: %s", err)
				}
			}
			cinfo, err := sub.ConsumerInfo()
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			nextSubject := fmt.Sprintf("$JS.API.CONSUMER.MSG.NEXT.TEST.%s", cinfo.Name)
			replySubject := strings.Replace(sub.Subject, "*", "abc", 1)
			payload := `{"batch":10,"no_wait":true}`
			if err := nc.PublishRequest(nextSubject, replySubject, []byte(payload)); err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			time.Sleep(100 * time.Millisecond)

			sub.Drain()
			res, err := sub.FetchBatch(100)
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
			msgs := make([]*nats.Msg, 0)
			for msg := range res.Messages() {
				msgs = append(msgs, msg)
				msg.Ack()
			}
			if res.Error() != nil {
				t.Fatalf("Unexpected error: %s", res.Error())
			}
			if len(msgs) != 10 {
				t.Fatalf("Expected %d messages; got: %d", 10, len(msgs))
			}

			_, err = sub.FetchBatch(10, nats.MaxWait(100*time.Millisecond))
			if !errors.Is(err, nats.ErrSubscriptionClosed) {
				t.Fatalf("Expected error: %s; got: %s", nats.ErrSubscriptionClosed, err)
			}
		})
	})
}

func TestPullSubscribeConsumerDeleted(t *testing.T) {
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
		if _, err := js.Publish("foo", []byte("msg")); err != nil {
			t.Fatal(err)
		}
		t.Run("delete consumer", func(t *testing.T) {
			sub, err := js.PullSubscribe("foo", "cons")
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()

			if _, err = sub.Fetch(1, nats.MaxWait(10*time.Millisecond)); err != nil {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrTimeout, err)
			}
			time.AfterFunc(50*time.Millisecond, func() { js.DeleteConsumer("TEST", "cons") })
			if _, err = sub.Fetch(1, nats.MaxWait(100*time.Millisecond)); !errors.Is(err, nats.ErrConsumerDeleted) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerDeleted, err)
			}
		})

		t.Run("delete stream", func(t *testing.T) {
			sub, err := js.PullSubscribe("foo", "cons")
			if err != nil {
				t.Fatal(err)
			}
			defer sub.Unsubscribe()

			if _, err = sub.Fetch(1, nats.MaxWait(10*time.Millisecond)); err != nil {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrTimeout, err)
			}
			time.AfterFunc(50*time.Millisecond, func() { js.DeleteStream("TEST") })
			if _, err = sub.Fetch(1, nats.MaxWait(100*time.Millisecond)); !errors.Is(err, nats.ErrConsumerDeleted) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerDeleted, err)
			}
		})
	})
}

func TestJetStreamAckPending_Pull(t *testing.T) {
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

		const totalMsgs = 4
		for i := range totalMsgs {
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
		for range ackPendingLimit {
			ms, err := sub.Fetch(1)
			if err != nil {
				t.Fatalf("Error on fetch: %v", err)
			}
			msgs = append(msgs, ms...)
		}

		if _, err := sub.Fetch(1, nats.MaxWait(250*time.Millisecond)); err != nats.ErrTimeout {
			t.Fatalf("Expected timeout, got: %v", err)
		}
		msgs[0].Ack()
		if _, err := sub.Fetch(1); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	})
}

func TestJetStreamAckPending_Push(t *testing.T) {
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

		const totalMsgs = 3
		for i := range totalMsgs {
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
	})
}

func TestJetStream_Drain(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, true)
	t.Cleanup(func() { inst.Destroy(t) })

	ctx, done := context.WithTimeout(context.Background(), 10*time.Second)

	nc, err := nats.Connect(inst.Servers[0].URL, nats.MaxReconnects(-1), nats.ClosedHandler(func(_ *nats.Conn) {
		done()
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
		Subjects: []string{"drain"},
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	total := 500
	for i := range total {
		_, err := js.Publish("drain", []byte(fmt.Sprintf("i:%d", i)))
		if err != nil {
			t.Error(err)
		}
	}

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
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		sub, _ := nc.SubscribeSync("foo")
		nc.PublishRequest("foo", "_INBOX_", []byte("OK"))
		m, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if err := m.Ack(); err != nil {
			t.Fatalf("Expected no errors, got '%v'", err)
		}
	})
}

func TestJetStreamManagement(t *testing.T) {
	c := newTester(t)
	accounts := `accounts: {
  A {
    users: [{ user: "foo" }]
    jetstream: { max_mem: 64MB, max_file: 64MB }
  }
}`
	inst := c.CreateServer(t, true,
		testservice.WithAccounts(accounts),
		testservice.WithAuthorization("# auth required, no no_auth_user"),
		testservice.WithSystemAccount(noSystemAccountBody()),
	)
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst, nats.UserInfo("foo", ""))
	c.WaitForJetStream(t, nc)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var si *nats.StreamInfo

	t.Run("create stream", func(t *testing.T) {
		consLimits := nats.StreamConsumerLimits{
			MaxAckPending:     100,
			InactiveThreshold: 10 * time.Second,
		}
		cfg := &nats.StreamConfig{
			Name:        "foo",
			Subjects:    []string{"foo", "bar", "baz"},
			Compression: nats.S2Compression,
			ConsumerLimits: nats.StreamConsumerLimits{
				MaxAckPending:     100,
				InactiveThreshold: 10 * time.Second,
			},
			FirstSeq: 22,
			Metadata: map[string]string{
				"foo": "bar",
				"baz": "quux",
			},
		}

		si, err := js.AddStream(cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si == nil || si.Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", si)
		}
		if v1, v2 := si.Config.Metadata["foo"], si.Config.Metadata["baz"]; v1 != "bar" || v2 != "quux" {
			t.Fatalf("Metadata is not correct %+v", si.Config.Metadata)
		}
		if si.Config.Compression != nats.S2Compression {
			t.Fatalf("Compression is not correct %+v", si.Config.Compression)
		}
		if si.Config.FirstSeq != 22 {
			t.Fatalf("FirstSeq is not correct %+v", si.Config.FirstSeq)
		}
		if si.Config.ConsumerLimits != consLimits {
			t.Fatalf("ConsumerLimits is not correct %+v", si.Config.ConsumerLimits)
		}
	})

	t.Run("stream with given name already exists", func(t *testing.T) {
		if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Description: "desc"}); !errors.Is(err, nats.ErrStreamNameAlreadyInUse) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrStreamNameAlreadyInUse, err)
		}
	})

	for range 25 {
		js.Publish("foo", []byte("hi"))
	}

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
		if _, err := js.AddStream(nil); err != nats.ErrStreamConfigRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamConfigRequired, err)
		}
		if _, err := js.AddStream(&nats.StreamConfig{Name: ""}); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.AddStream(&nats.StreamConfig{Name: "bad.stream.name"}); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if _, err := js.AddStream(&nats.StreamConfig{Name: "bad stream name"}); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
	})

	t.Run("bad stream info", func(t *testing.T) {
		if _, err := js.StreamInfo(""); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.StreamInfo("bad.stream.name"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
	})

	t.Run("stream update", func(t *testing.T) {
		if _, err := js.UpdateStream(nil); err != nats.ErrStreamConfigRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamConfigRequired, err)
		}
		if _, err := js.UpdateStream(&nats.StreamConfig{Name: ""}); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.UpdateStream(&nats.StreamConfig{Name: "bad.stream.name"}); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
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
		t.Run("with durable set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				Durable:   "dlc",
				AckPolicy: nats.AckExplicitPolicy,
				Metadata: map[string]string{
					"foo": "bar",
					"baz": "quux",
				},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
			if v1, v2 := ci.Config.Metadata["foo"], ci.Config.Metadata["baz"]; v1 != "bar" || v2 != "quux" {
				t.Fatalf("Metadata is not correct %+v", ci.Config.Metadata)
			}
		})
		t.Run("with name set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-1")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc-1", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-1"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-1" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("with same Durable and Name set", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-2")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc-2", Name: "dlc-2", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-2"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-2" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("with name and filter subject", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-3.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				Durable:       "dlc-3",
				AckPolicy:     nats.AckExplicitPolicy,
				FilterSubject: "foo",
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-3"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Name != "dlc-3" || ci.Stream != "foo" || ci.Config.FilterSubject != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("legacy ephemeral consumer without name", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"stream_name":"foo"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Config.Durable != "" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("legacy durable with jetstream context option", func(t *testing.T) {
			jsLegacy, err := nc.JetStream(nats.UseLegacyDurableConsumers())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.DURABLE.CREATE.foo.dlc-4")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := jsLegacy.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc-4", AckPolicy: nats.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-4"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Config.Durable != "dlc-4" || ci.Stream != "foo" {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("durable consumer with multiple filter subjects", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.dlc-5")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				Durable:        "dlc-5",
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo", "bar"},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			msg, err := sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(msg.Data), `"durable_name":"dlc-5"`) {
				t.Fatalf("create consumer message is not correct: %q", string(msg.Data))
			}
			if ci == nil || ci.Config.Durable != "dlc-5" || !reflect.DeepEqual(ci.Config.FilterSubjects, []string{"foo", "bar"}) {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("ephemeral consumer with multiple filter subjects", func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()
			ci, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo", "bar"},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			_, err = sub.NextMsg(1 * time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci == nil || !reflect.DeepEqual(ci.Config.FilterSubjects, []string{"foo", "bar"}) {
				t.Fatalf("ConsumerInfo is not correct %+v", ci)
			}
		})

		t.Run("multiple filter subjects errors", func(t *testing.T) {
			_, err := js.AddConsumer("foo", &nats.ConsumerConfig{
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo", "bar"},
				FilterSubject:  "baz",
			})
			if !errors.Is(err, nats.ErrDuplicateFilterSubjects) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrDuplicateFilterSubjects, err)
			}
			_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo.*", "foo.A"},
			})
			if !errors.Is(err, nats.ErrOverlappingFilterSubjects) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrOverlappingFilterSubjects, err)
			}
			_, err = js.AddConsumer("foo", &nats.ConsumerConfig{
				AckPolicy:      nats.AckExplicitPolicy,
				FilterSubjects: []string{"foo", ""},
			})
			if !errors.Is(err, nats.ErrEmptyFilter) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrEmptyFilter, err)
			}
		})

		t.Run("with invalid filter subject", func(t *testing.T) {
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Name: "tc", FilterSubject: ".foo"}); !errors.Is(err, nats.ErrInvalidFilterSubject) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidFilterSubject, err)
			}
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Name: "tc", FilterSubject: "foo."}); !errors.Is(err, nats.ErrInvalidFilterSubject) {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidFilterSubject, err)
			}
		})

		t.Run("with invalid consumer name", func(t *testing.T) {
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "test.durable"}); err != nats.ErrInvalidConsumerName {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidConsumerName, err)
			}
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "test durable"}); err != nats.ErrInvalidConsumerName {
				t.Fatalf("Expected: %v; got: %v", nats.ErrInvalidConsumerName, err)
			}
		})

		t.Run("consumer with given name already exists, configs do not match", func(t *testing.T) {
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckAllPolicy}); !errors.Is(err, nats.ErrConsumerNameAlreadyInUse) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerNameAlreadyInUse, err)
			}
		})

		t.Run("consumer with given name already exists, configs are the same", func(t *testing.T) {
			if _, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy}); err != nil {
				t.Fatalf("Expected no error; got: %v", err)
			}
		})

		t.Run("stream does not exist", func(t *testing.T) {
			_, err = js.AddConsumer("missing", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrStreamNotFound {
				t.Fatalf("Expected stream not found error, got: %v", err)
			}
		})

		t.Run("params validation error", func(t *testing.T) {
			_, err = js.AddConsumer("", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrStreamNameRequired {
				t.Fatalf("Expected %v, got: %v", nats.ErrStreamNameRequired, err)
			}
			_, err = js.AddConsumer("bad.stream.name", &nats.ConsumerConfig{Durable: "dlc", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrInvalidStreamName {
				t.Fatalf("Expected %v, got: %v", nats.ErrInvalidStreamName, err)
			}
			_, err = js.AddConsumer("foo", &nats.ConsumerConfig{Durable: "bad.consumer.name", AckPolicy: nats.AckExplicitPolicy})
			if err != nats.ErrInvalidConsumerName {
				t.Fatalf("Expected %v, got: %v", nats.ErrInvalidConsumerName, err)
			}
		})
	})

	t.Run("consumer info", func(t *testing.T) {
		if _, err := js.ConsumerInfo("", "dlc"); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if _, err := js.ConsumerInfo("bad.stream.name", "dlc"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if _, err := js.ConsumerInfo("foo", ""); err != nats.ErrConsumerNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrConsumerNameRequired, err)
		}
		if _, err := js.ConsumerInfo("foo", "bad.consumer.name"); err != nats.ErrInvalidConsumerName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidConsumerName, err)
		}
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
		for info := range js.Streams() {
			infos = append(infos, info)
		}
		if len(infos) != 1 || infos[0].Config.Name != "foo" {
			t.Fatalf("StreamInfo is not correct %+v", infos)
		}
	})

	t.Run("list consumers", func(t *testing.T) {
		var infos []*nats.ConsumerInfo
		for info := range js.Consumers("") {
			infos = append(infos, info)
		}
		if len(infos) != 0 {
			t.Fatalf("ConsumerInfo is not correct %+v", infos)
		}
		for info := range js.Consumers("bad.stream.name") {
			infos = append(infos, info)
		}
		if len(infos) != 0 {
			t.Fatalf("ConsumerInfo is not correct %+v", infos)
		}
		infos = infos[:0]
		for info := range js.Consumers("foo") {
			infos = append(infos, info)
		}
		if len(infos) != 8 || infos[0].Stream != "foo" {
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
		if got, want := len(names), 8; got != want {
			t.Fatalf("Unexpected names, got=%d, want=%d", got, want)
		}
	})

	t.Run("delete consumers", func(t *testing.T) {
		if err := js.DeleteConsumer("", "dlc"); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if err := js.DeleteConsumer("bad.stream.name", "dlc"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
		if err := js.DeleteConsumer("foo", ""); err != nats.ErrConsumerNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrConsumerNameRequired, err)
		}
		if err := js.DeleteConsumer("foo", "bad.consumer.name"); err != nats.ErrInvalidConsumerName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidConsumerName, err)
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

		_, err = js.UpdateConsumer("", &expected)
		if err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected stream name required error, got %v", err)
		}
		_, err = js.UpdateConsumer("bad.stream.name", &expected)
		if err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected stream name required error, got %v", err)
		}
		expected.Durable = ""
		expected.Name = ""
		_, err = js.UpdateConsumer("foo", &expected)
		if err != nats.ErrConsumerNameRequired {
			t.Fatalf("Expected consumer name required error, got %v", err)
		}
		expected.Durable = "bad.consumer.name"
		_, err = js.UpdateConsumer("foo", &expected)
		if err != nats.ErrInvalidConsumerName {
			t.Fatalf("Expected invalid consumer name error, got %v", err)
		}
		expected.Durable = "update_push_consumer"

		_, err = js.UpdateConsumer("foo", nil)
		if err != nats.ErrConsumerConfigRequired {
			t.Fatalf("Expected consumer configuration required error, got %v", err)
		}

		ci, err = js.UpdateConsumer("foo", &expected)
		if err != nil {
			t.Fatalf("Error on update: %v", err)
		}
		expected.Name = "update_push_consumer"
		if !reflect.DeepEqual(ci.Config, expected) {
			t.Fatalf("Expected config to be %+v, got %+v", expected, ci.Config)
		}

		ci, err = js.AddConsumer("foo", &nats.ConsumerConfig{
			Durable:    "update_pull_consumer",
			AckPolicy:  nats.AckExplicitPolicy,
			MaxWaiting: 1,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expected = ci.Config
		expected.Description = "my description"
		expected.AckWait = 2 * time.Second
		expected.MaxDeliver = 1
		expected.SampleFrequency = "30"
		expected.MaxAckPending = 10
		expected.HeadersOnly = true
		expected.MaxRequestBatch = 10
		expected.MaxRequestExpires = 2 * time.Second
		expected.MaxRequestMaxBytes = 1024

		ci, err = js.UpdateConsumer("foo", &expected)
		if err != nil {
			t.Fatalf("Error on update: %v", err)
		}
		if !reflect.DeepEqual(ci.Config, expected) {
			t.Fatalf("Expected config to be %+v, got %+v", expected, ci.Config)
		}
	})

	t.Run("purge stream", func(t *testing.T) {
		if err := js.PurgeStream(""); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if err := js.PurgeStream("bad.stream.name"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
		}
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
		if err := js.DeleteStream(""); err != nats.ErrStreamNameRequired {
			t.Fatalf("Expected %v, got %v", nats.ErrStreamNameRequired, err)
		}
		if err := js.DeleteStream("bad.stream.name"); err != nats.ErrInvalidStreamName {
			t.Fatalf("Expected %v, got %v", nats.ErrInvalidStreamName, err)
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

func TestStreamConfigMatches(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		cfg := nats.StreamConfig{
			Name:                 "stream",
			Description:          "desc",
			Subjects:             []string{"foo.*"},
			Retention:            nats.WorkQueuePolicy,
			MaxConsumers:         10,
			MaxMsgs:              100,
			MaxBytes:             1000,
			Discard:              nats.DiscardNew,
			DiscardNewPerSubject: true,
			MaxAge:               100 * time.Second,
			MaxMsgsPerSubject:    1000,
			MaxMsgSize:           10000,
			Storage:              nats.MemoryStorage,
			Replicas:             1,
			NoAck:                true,
			Duplicates:           10 * time.Second,
			Sealed:               false,
			DenyDelete:           true,
			DenyPurge:            false,
			AllowRollup:          true,
			Compression:          nats.S2Compression,
			FirstSeq:             5,
			SubjectTransform:     &nats.SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
			RePublish: &nats.RePublish{
				Source:      ">",
				Destination: "RP.>",
				HeadersOnly: true,
			},
			AllowDirect: true,
			ConsumerLimits: nats.StreamConsumerLimits{
				InactiveThreshold: 10 * time.Second,
				MaxAckPending:     500,
			},
		}

		s, err := js.AddStream(&cfg)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		// server will set metadata values, so we need to clear them
		s.Config.Metadata = nil
		if !reflect.DeepEqual(s.Config, cfg) {
			t.Fatalf("StreamConfig doesn't match: %#v", s.Config)
		}

		cfgMirror := nats.StreamConfig{
			Name:              "mirror",
			MaxConsumers:      10,
			MaxMsgs:           100,
			MaxBytes:          1000,
			MaxAge:            100 * time.Second,
			MaxMsgsPerSubject: 1000,
			MaxMsgSize:        10000,
			Replicas:          1,
			Duplicates:        10 * time.Second,
			Mirror: &nats.StreamSource{
				Name:        "stream",
				OptStartSeq: 10,
				SubjectTransforms: []nats.SubjectTransformConfig{
					{Source: ">", Destination: "transformed.>"},
				},
			},
			MirrorDirect:     true,
			SubjectTransform: &nats.SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
		}

		s, err = js.AddStream(&cfgMirror)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		s.Config.Metadata = nil
		if !reflect.DeepEqual(s.Config, cfgMirror) {
			t.Fatalf("StreamConfig doesn't match: %#v", s.Config)
		}

		cfgSourcing := nats.StreamConfig{
			Name:              "sourcing",
			Subjects:          []string{"BAR"},
			MaxConsumers:      10,
			MaxMsgs:           100,
			MaxBytes:          1000,
			MaxAge:            100 * time.Second,
			MaxMsgsPerSubject: 1000,
			MaxMsgSize:        10000,
			Replicas:          1,
			Duplicates:        10 * time.Second,
			Sources: []*nats.StreamSource{
				{
					Name:        "stream",
					OptStartSeq: 10,
					SubjectTransforms: []nats.SubjectTransformConfig{
						{Source: ">", Destination: "transformed.>"},
					},
				},
			},
			SubjectTransform: &nats.SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
		}

		s, err = js.AddStream(&cfgSourcing)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		s.Config.Metadata = nil
		if !reflect.DeepEqual(s.Config, cfgSourcing) {
			t.Fatalf("StreamConfig doesn't match: %#v", s.Config)
		}
	})
}
