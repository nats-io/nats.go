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
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

func TestJetStreamClusterStreamLeaderChangeClientErr(t *testing.T) {
	t.Skip("The 2.9 server changed behavior making this test fail now")
}

func TestJetStreamConsumerConfigReplicasAndMemStorage(t *testing.T) {
	c := newTester(t)
	inst := c.CreateCluster(t, 3, true, clientAdvertiseOpt(t))
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	c.WaitForJetStream(t, nc)

	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "CR",
		Subjects: []string{"foo"},
		Replicas: 3,
	}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// We can't really check if the consumer ends-up with memory storage or not.
	// We are simply going to create a NATS subscription on the request subject
	// and make sure that the request contains "mem_storage:true".
	sub, err := nc.SubscribeSync("$JS.API.CONSUMER.CREATE.CR.dur")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	ci, err := js.AddConsumer("CR", &nats.ConsumerConfig{
		Durable:        "dur",
		DeliverSubject: "bar",
		Replicas:       1,
		MemoryStorage:  true,
	})
	if err != nil {
		t.Fatalf("Error adding consumer: %v", err)
	}
	if n := len(ci.Cluster.Replicas); n > 0 {
		t.Fatalf("Expected replicas to be 1, got %+v", ci.Cluster)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error on next msg: %v", err)
	}
	if str := string(msg.Data); !strings.Contains(str, "mem_storage\":true") {
		t.Fatalf("Does not look like the request asked for memory storage: %s", str)
	}
}

func TestJetStreamRePublish(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     "RP",
			Storage:  nats.MemoryStorage,
			Subjects: []string{"foo", "bar", "baz"},
			RePublish: &nats.RePublish{
				Source:      ">",
				Destination: "RP.>",
			},
		}); err != nil {
			t.Fatalf("Error adding stream: %v", err)
		}

		sub, err := nc.SubscribeSync("RP.>")
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		msg, toSend := []byte("OK TO REPUBLISH?"), 100
		for range toSend {
			js.Publish("foo", msg)
			js.Publish("bar", msg)
			js.Publish("baz", msg)
		}

		lseq := map[string]int{
			"foo": 0,
			"bar": 0,
			"baz": 0,
		}

		for i := 1; i <= toSend; i++ {
			m, err := sub.NextMsg(time.Second)
			if err != nil {
				t.Fatalf("Error on next msg: %v", err)
			}
			stream := m.Header.Get(nats.JSStream)
			if stream != "RP" {
				t.Fatalf("Unexpected header: %+v", m.Header)
			}
			seq, err := strconv.Atoi(m.Header.Get(nats.JSSequence))
			if err != nil {
				t.Fatalf("Error decoding sequence for %s", m.Header.Get(nats.JSSequence))
			}
			if seq != i {
				t.Fatalf("Expected sequence to be %v, got %v", i, seq)
			}
			last, err := strconv.Atoi(m.Header.Get(nats.JSLastSequence))
			if err != nil {
				t.Fatalf("Error decoding last sequence for %s", m.Header.Get(nats.JSLastSequence))
			}
			if last != lseq[m.Subject] {
				t.Fatalf("Expected last sequence to be %v, got %v", lseq[m.Subject], last)
			}
			lseq[m.Subject] = seq
		}
	})
}

func TestJetStreamDirectGetMsg(t *testing.T) {
	// Using standalone server here, we are testing the client side API, not
	// the server feature, which has tests checking it works in cluster mode.
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		si, err := js.AddStream(&nats.StreamConfig{
			Name:     "DGM",
			Storage:  nats.MemoryStorage,
			Subjects: []string{"foo", "bar"},
		})
		if err != nil {
			t.Fatalf("Error adding stream: %v", err)
		}

		send := func(subj, body string) {
			t.Helper()
			if _, err := js.Publish(subj, []byte(body)); err != nil {
				t.Fatalf("Error on publish: %v", err)
			}
		}

		send("foo", "a")
		send("foo", "b")
		send("foo", "c")
		send("bar", "d")
		send("foo", "e")

		// Without AllowDirect, we should get no responders
		if _, err := js.GetMsg("DGM", 1, nats.DirectGet()); err != nats.ErrNoResponders {
			t.Fatalf("Unexpected error: %v", err)
		}

		si.Config.AllowDirect = true
		si, err = js.UpdateStream(&si.Config)
		if err != nil {
			t.Fatalf("Error updating stream: %v", err)
		}
		if !si.Config.AllowDirect {
			t.Fatalf("AllowDirect should be true: %+v", si)
		}

		check := func(seq uint64, opt nats.JSOpt, useGetLast bool, expectedSubj string, expectedSeq uint64, expectedBody string) {
			t.Helper()

			var msg *nats.RawStreamMsg
			var err error
			if useGetLast {
				msg, err = js.GetLastMsg("DGM", expectedSubj, []nats.JSOpt{opt}...)
			} else {
				msg, err = js.GetMsg("DGM", seq, []nats.JSOpt{opt}...)
			}
			if err != nil {
				t.Fatalf("Unable to get message: %v", err)
			}
			if msg.Subject != expectedSubj {
				t.Fatalf("Expected subject %q, got %q", expectedSubj, msg.Subject)
			}
			if msg.Sequence != expectedSeq {
				t.Fatalf("Expected sequence %v, got %v", expectedSeq, msg.Sequence)
			}
			if msg.Time.IsZero() {
				t.Fatal("Expected timestamp, did not get one")
			}
			if b := string(msg.Data); b != expectedBody {
				t.Fatalf("Expected body %q, got %q", expectedBody, b)
			}
		}

		check(0, nats.DirectGetNext("bar"), false, "bar", 4, "d")
		check(0, nats.DirectGet(), true, "foo", 5, "e")
		check(0, nats.DirectGetNext("foo"), false, "foo", 1, "a")
		check(4, nats.DirectGetNext("foo"), false, "foo", 5, "e")
		check(2, nats.DirectGetNext("foo"), false, "foo", 2, "b")

		msg := nats.NewMsg("foo")
		msg.Header.Set("MyHeader", "MyValue")
		if _, err := js.PublishMsg(msg); err != nil {
			t.Fatalf("Error publishing message: %v", err)
		}
		r, err := js.GetMsg("DGM", 6, nats.DirectGet())
		if err != nil {
			t.Fatalf("Error getting message: %v", err)
		}
		if v := r.Header.Get("MyHeader"); v != "MyValue" {
			t.Fatalf("Expected header to be present, was not: %v", r.Header)
		}

		if _, err := js.GetMsg("DGM", 100, nats.DirectGet()); err != nats.ErrMsgNotFound {
			t.Fatalf("Expected not found error, got %v", err)
		}
		if _, err := js.GetMsg("DGM", 0, nats.DirectGet()); err == nil || !strings.Contains(err.Error(), "Empty Request") {
			t.Fatalf("Unexpected error: %v", err)
		}

		r, err = js.GetLastMsg("DGM", "bar", nats.DirectGet())
		if err != nil {
			t.Fatalf("Error getting message: %v", err)
		}
		if r.Subject != "bar" {
			t.Fatalf("expected subject to be 'bar', got: %v", r.Subject)
		}
		if string(r.Data) != "d" {
			t.Fatalf("expected data to be 'd', got: %v", string(r.Data))
		}
	})
}

func TestJetStreamConsumerReplicasOption(t *testing.T) {
	c := newTester(t)
	inst := c.CreateCluster(t, 3, true, clientAdvertiseOpt(t))
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	c.WaitForJetStream(t, nc)

	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{
		Name:     "ConsumerReplicasTest",
		Subjects: []string{"foo"},
		Replicas: 3,
	}); err != nil {
		t.Fatalf("Error adding stream: %v", err)
	}

	// Subscribe to the stream with a durable consumer "bar" and replica set to 1.
	cb := func(msg *nats.Msg) {}
	_, err = js.Subscribe("foo", cb, nats.Durable("bar"), nats.ConsumerReplicas(1))
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	consInfo, err := js.ConsumerInfo("ConsumerReplicasTest", "bar")
	if err != nil {
		t.Fatalf("Error getting consumer info: %v", err)
	}

	if consInfo.Config.Replicas != 1 {
		t.Fatalf("Expected consumer replica to be %v, got %+v", 1, consInfo.Config.Replicas)
	}
}

func TestJetStreamMsgAckShouldErrForConsumerAckNone(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.AddStream(&nats.StreamConfig{
			Name:     "ACKNONE",
			Storage:  nats.MemoryStorage,
			Subjects: []string{"foo"},
		}); err != nil {
			t.Fatalf("Error adding stream: %v", err)
		}
		if _, err := js.Publish("foo", []byte("hello")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}

		sub, err := js.SubscribeSync("foo", nats.OrderedConsumer())
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error getting message: %v", err)
		}
		if err := msg.Ack(); err != nats.ErrCantAckIfConsumerAckNone {
			t.Fatalf("Expected error indicating that sub is AckNone, got %v", err)
		}
	})
}

func TestJetStreamOrderedConsumerRecreateAfterReconnect(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, true, clientAdvertiseOpt(t))
	t.Cleanup(func() { inst.Destroy(t) })

	// monitor for ErrConsumerNotActive error and suppress logging
	hbMissed := make(chan struct{}, 10)
	errHandler := func(c *nats.Conn, s *nats.Subscription, err error) {
		if !errors.Is(err, nats.ErrConsumerNotActive) {
			t.Errorf("Unexpected error: %v", err)
			return
		}
		select {
		case hbMissed <- struct{}{}:
		default:
		}
	}
	nc := dialInstance(t, inst, nats.ErrorHandler(errHandler))
	c.WaitForJetStream(t, nc)

	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if _, err := js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := js.SubscribeSync("FOO.A", nats.OrderedConsumer(), nats.IdleHeartbeat(100*time.Millisecond))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	consInfo, err := sub.ConsumerInfo()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	consName := consInfo.Name
	// validate that the generated name of the consumer is 8
	// characters long (shorter than standard nuid)
	if len(consName) != 8 {
		t.Fatalf("Unexpected consumer name: %q", consName)
	}
	if _, err := js.Publish("FOO.A", []byte("msg 1")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, err := sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if string(msg.Data) != "msg 1" {
		t.Fatalf("Invalid msg value; want: 'msg 1'; got: %q", string(msg.Data))
	}

	apiSub, err := nc.SubscribeSync("$JS.API.CONSUMER.*.>")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// restart the server (storage preserved by testservice across stop/start)
	inst.StopServer(t, inst.Servers[0])
	inst.StartServer(t, inst.Servers[0])
	c.WaitForJetStream(t, nc)

	// wait until we miss heartbeat
	select {
	case <-hbMissed:
	case <-time.After(10 * time.Second):
		t.Fatalf("Did not receive consumer not active error")
	}
	consDeleteMsg, err := apiSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !strings.HasPrefix(consDeleteMsg.Subject, "$JS.API.CONSUMER.") {
		t.Fatalf("Unexpected message subject: %q", consDeleteMsg.Subject)
	}
	consCreateMsg, err := apiSub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if !strings.HasPrefix(consCreateMsg.Subject, "$JS.API.CONSUMER.") {
		t.Fatalf("Unexpected message subject: %q", consCreateMsg.Subject)
	}
	if _, err := js.Publish("FOO.A", []byte("msg 2")); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	msg, err = sub.NextMsg(2 * time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	var infoErr error
	for range 5 {
		consInfo, infoErr = sub.ConsumerInfo()
		if infoErr != nil {
			if errors.Is(infoErr, nats.ErrConsumerInfoOnOrderedReset) {
				time.Sleep(100 * time.Millisecond)
				continue
			}
			t.Fatalf("Unexpected error: %v", err)
		}
		infoErr = nil
		break
	}
	if infoErr != nil {
		t.Fatalf("Unexpected error: %v", infoErr)
	}
	if consInfo.Name == consName || len(consInfo.Name) != 8 {
		t.Fatalf("Unexpected consumer name: %q", consInfo.Name)
	}

	if string(msg.Data) != "msg 2" {
		t.Fatalf("Invalid msg value; want: 'msg 2'; got: %q", string(msg.Data))
	}
}

func TestJetStreamCreateStreamDiscardPolicy(t *testing.T) {
	tests := []struct {
		name                 string
		discardPolicy        nats.DiscardPolicy
		discardNewPerSubject bool
		maxMsgsPerSubject    int64
		withAPIError         bool
	}{
		{
			name:                 "with discard policy 'new' and discard new per subject set",
			discardPolicy:        nats.DiscardNew,
			discardNewPerSubject: true,
			maxMsgsPerSubject:    100,
		},
		{
			name:                 "with discard policy 'new' and discard new per subject not set",
			discardPolicy:        nats.DiscardNew,
			discardNewPerSubject: false,
			maxMsgsPerSubject:    100,
		},
		{
			name:                 "with discard policy 'old' and discard new per subject set",
			discardPolicy:        nats.DiscardOld,
			discardNewPerSubject: true,
			maxMsgsPerSubject:    100,
			withAPIError:         true,
		},
		{
			name:                 "with discard policy 'old' and discard new per subject not set",
			discardPolicy:        nats.DiscardOld,
			discardNewPerSubject: true,
			maxMsgsPerSubject:    100,
			withAPIError:         true,
		},
		{
			name:                 "with discard policy 'new' and discard new per subject set and max msgs per subject not set",
			discardPolicy:        nats.DiscardNew,
			discardNewPerSubject: true,
			withAPIError:         true,
		},
	}

	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i, test := range tests {
			t.Run(test.name, func(t *testing.T) {
				streamName := fmt.Sprintf("FOO%d", i)

				_, err := js.AddStream(&nats.StreamConfig{
					Name:                 streamName,
					Discard:              test.discardPolicy,
					DiscardNewPerSubject: test.discardNewPerSubject,
					MaxMsgsPerSubject:    test.maxMsgsPerSubject,
				})

				if test.withAPIError {
					var apiErr *nats.APIError
					if err == nil {
						t.Fatalf("Expected error, got nil")
					}
					if ok := errors.As(err, &apiErr); !ok {
						t.Fatalf("Expected nats.APIError, got %v", err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				info, err := js.StreamInfo(streamName)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if info.Config.Discard != test.discardPolicy {
					t.Fatalf("Invalid value of discard policy; want: %s; got: %s", test.discardPolicy.String(), info.Config.Discard.String())
				}
				if info.Config.DiscardNewPerSubject != test.discardNewPerSubject {
					t.Fatalf("Invalid value of discard_new_per_subject; want: %t; got: %t", test.discardNewPerSubject, info.Config.DiscardNewPerSubject)
				}
			})
		}
	})
}

func TestJetStreamStreamInfoAlternates(t *testing.T) {
	c := newTester(t)
	inst := c.CreateCluster(t, 3, true, clientAdvertiseOpt(t))
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)
	c.WaitForJetStream(t, nc)

	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "TEST",
		Subjects: []string{"foo"},
	})
	expectOk(t, err)

	_, err = js.AddStream(&nats.StreamConfig{
		Name: "MIRROR",
		Mirror: &nats.StreamSource{
			Name: "TEST",
		},
	})
	expectOk(t, err)

	si, err := js.StreamInfo("TEST")
	expectOk(t, err)

	if len(si.Alternates) != 2 {
		t.Fatalf("Expected 2 alternates, got %d", len(si.Alternates))
	}
}

func TestJetStreamSubscribeConsumerName(t *testing.T) {
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
		_, err = js.Publish("foo", []byte("first"))
		if err != nil {
			t.Fatal(err)
		}

		_, err = js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("stream lookup failed: %v", err)
		}

		sub, err := js.SubscribeSync("foo", nats.ConsumerName("my-ephemeral"))
		if err != nil {
			t.Fatal(err)
		}
		cinfo, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		got := cinfo.Config.Name
		expected := "my-ephemeral"
		if got != expected {
			t.Fatalf("Expected: %v, got: %v", expected, got)
		}
		got = cinfo.Config.Durable
		expected = ""
		if got != expected {
			t.Fatalf("Expected: %v, got: %v", expected, got)
		}
		_, err = sub.NextMsg(1 * time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// ConsumerName will be ignored in case a durable name has been set.
		sub, err = js.SubscribeSync("foo", nats.Durable("durable"), nats.ConsumerName("custom-name"))
		if err != nil {
			t.Fatal(err)
		}
		cinfo, err = sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		got = cinfo.Config.Name
		expected = "durable"
		if got != expected {
			t.Fatalf("Expected: %v, got: %v", expected, got)
		}
		got = cinfo.Config.Durable
		expected = "durable"
		if got != expected {
			t.Fatalf("Expected: %v, got: %v", expected, got)
		}
		_, err = sub.NextMsg(1 * time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// Default Ephemeral name should be short like in the server.
		sub, err = js.SubscribeSync("foo", nats.ConsumerName(""))
		if err != nil {
			t.Fatal(err)
		}
		cinfo, err = sub.ConsumerInfo()
		if err != nil {
			t.Fatal(err)
		}
		expectedSize := 8
		result := len(cinfo.Config.Name)
		if result != expectedSize {
			t.Fatalf("Expected: %v, got: %v", expectedSize, result)
		}
	})
}

func TestJetStreamOrderedConsumerDeleteAssets(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// For capturing errors.
		errCh := make(chan error, 1)
		nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			errCh <- err
		})

		// Create a sample asset.
		mlen := 128 * 1024
		msg := make([]byte, mlen)

		createStream := func() {
			t.Helper()
			_, err = js.AddStream(&nats.StreamConfig{
				Name:     "OBJECT",
				Subjects: []string{"a"},
				Storage:  nats.MemoryStorage,
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

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

			sub, err := js.SubscribeSync("a", nats.OrderedConsumer(), nats.IdleHeartbeat(200*time.Millisecond))
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer sub.Unsubscribe()

			// Since we are sync we will be paused here due to flow control.
			time.Sleep(100 * time.Millisecond)
			if err := js.DeleteStream("OBJECT"); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			select {
			case err := <-errCh:
				if !errors.Is(err, nats.ErrStreamNotFound) {
					t.Fatalf("Got wrong error, wanted %v, got %v", nats.ErrStreamNotFound, err)
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
			sub, err := js.SubscribeSync("a", nats.OrderedConsumer(), nats.IdleHeartbeat(200*time.Millisecond))
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
	})
}

// We want to make sure we do the right thing with lots of concurrent queue durable consumer requests.
// One should win and the others should share the delivery subject with the first one who wins.
func TestJetStreamConcurrentQueueDurablePushConsumers(t *testing.T) {
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

		// Now create 10 durables concurrently.
		subs := make([]*nats.Subscription, 0, 10)
		var wg sync.WaitGroup
		mx := &sync.Mutex{}

		for range 10 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				sub, _ := js.QueueSubscribeSync("foo", "bar")
				mx.Lock()
				subs = append(subs, sub)
				mx.Unlock()
			}()
		}
		wg.Wait()

		si, err := js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if si.State.Consumers != 1 {
			t.Fatalf("Expected exactly one consumer, got %d", si.State.Consumers)
		}

		total := 1000
		for range total {
			js.Publish("foo", []byte("Hello"))
		}

		timeout := time.Now().Add(2 * time.Second)
		got := 0
		for time.Now().Before(timeout) {
			got = 0
			for _, sub := range subs {
				pending, _, _ := sub.Pending()
				if pending == total {
					t.Fatalf("A single member should not have gotten all messages")
				}
				got += pending
			}
			if got == total {
				return
			}
		}
		t.Fatalf("Expected %v messages, got only %v", total, got)
	})
}

func TestJetStreamAckTokens(t *testing.T) {
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

		sub, err := js.SubscribeSync("foo")
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}

		now := time.Now()
		for _, test := range []struct {
			name     string
			expected *nats.MsgMetadata
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
				&nats.MsgMetadata{
					Stream:       "TEST",
					Consumer:     "cons",
					NumDelivered: 1,
					Sequence: nats.SequencePair{
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
				&nats.MsgMetadata{
					Stream:       "TEST",
					Consumer:     "cons",
					NumDelivered: 1,
					Sequence: nats.SequencePair{
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
				&nats.MsgMetadata{
					Domain:       "HUB",
					Stream:       "TEST",
					Consumer:     "cons",
					NumDelivered: 1,
					Sequence: nats.SequencePair{
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
				&nats.MsgMetadata{
					Domain:       "HUB",
					Stream:       "TEST",
					Consumer:     "cons",
					NumDelivered: 1,
					Sequence: nats.SequencePair{
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
				msg := nats.NewMsg("foo")
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
	})
}

func TestJetStreamTracing(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		// Open a dedicated connection without the default JetStream context
		// from withJSServer to wire in the ClientTrace.
		nc := dialInstance(t, inst)
		defer nc.Close()

		ctr := 0
		js, err := nc.JetStream(&nats.ClientTrace{
			RequestSent: func(subj string, payload []byte) {
				ctr++
				if subj != "$JS.API.STREAM.CREATE.X" {
					t.Fatalf("Expected sent trace to %s: got: %s", "$JS.API.STREAM.CREATE.X", subj)
				}
			},
			ResponseReceived: func(subj string, payload []byte, hdr nats.Header) {
				ctr++
				if subj != "$JS.API.STREAM.CREATE.X" {
					t.Fatalf("Expected received trace to %s: got: %s", "$JS.API.STREAM.CREATE.X", subj)
				}
			},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{Name: "X"})
		if err != nil {
			t.Fatalf("add stream failed: %s", err)
		}
		if ctr != 2 {
			t.Fatalf("did not receive all trace events: %d", ctr)
		}
	})
}

func TestJetStreamExpiredPullRequests(t *testing.T) {
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

		sub, err := js.PullSubscribe("foo", "bar", nats.PullMaxWaiting(2))
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

		for range 2 {
			if _, err = sub.Fetch(1, nats.MaxWait(15*time.Millisecond)); err == nil {
				t.Fatalf("Expected error, got none")
			}
		}
		// Wait before the above expire
		time.Sleep(50 * time.Millisecond)
		batches := []int{1, 10}
		for _, bsz := range batches {
			start := time.Now()
			_, err = sub.Fetch(bsz, nats.MaxWait(250*time.Millisecond))
			dur := time.Since(start)
			if err == nil || dur < 50*time.Millisecond {
				t.Fatalf("Expected error and wait for 250ms, got err=%v and dur=%v", err, dur)
			}
		}
	})
}

func TestJetStreamSyncSubscribeWithMaxAckPending(t *testing.T) {
	t.Skip("DIVERGENCE: original sets natsserver opts.JetStreamLimits.MaxAckPending = 123, which renders inside the server's `jetstream { max_ack_pending: 123 }` block. testservice's default template already renders a `jetstream:` block; adding another via WithTopLevel produces two coexisting blocks and the limit is lost. Same blocker as TestJetStreamDomain / TestJetStreamDomainInPubAck. Recommended resolution: upstream a `WithJetStreamLimits(...)` option (or general `WithJetStream(body string)` snippet) in testservice that injects limits into the rendered jetstream block.")
}

func TestJetStreamClusterPlacement(t *testing.T) {
	// There used to be a test here that would not work because it would require
	// all servers in the cluster to know about each other tags. So we will simply
	// verify that if a stream is configured with placement and tags, the proper
	// "stream create" request is sent.
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub, err := nc.SubscribeSync("$JS.API.STREAM.CREATE.TEST")
		if err != nil {
			t.Fatalf("Error on sub: %v", err)
		}
		js.AddStream(&nats.StreamConfig{
			Name: "TEST",
			Placement: &nats.Placement{
				Tags: []string{"my_tag"},
			},
		})
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error getting stream create request: %v", err)
		}
		var req nats.StreamConfig
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
	})
}

func TestJetStreamConsumerMemoryStorage(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if _, err := js.AddStream(&nats.StreamConfig{Name: "STR", Subjects: []string{"foo"}}); err != nil {
			t.Fatalf("Error adding stream: %v", err)
		}

		// Pull ephemeral consumer with memory storage.
		sub, err := js.PullSubscribe("foo", "", nats.ConsumerMemoryStorage())
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

		sub, err = js.SubscribeSync("foo", nats.ConsumerMemoryStorage())
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

		cb := func(msg *nats.Msg) {}
		sub, err = js.Subscribe("foo", cb, nats.ConsumerMemoryStorage())
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
	})
}

func TestJetStreamStreamInfoWithSubjectDetails(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:     "TEST",
			Subjects: []string{"test.*"},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// Publish on enough subjects to exercise the pagination
		payload := make([]byte, 10)
		for i := range 100001 {
			_, err := js.Publish(fmt.Sprintf("test.%d", i), payload)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		}

		result, err := js.StreamInfo("TEST", &nats.StreamInfoRequest{SubjectsFilter: ">"})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(result.State.Subjects) != 100001 {
			t.Fatalf("expected 100001 subjects in the stream, but got %d instead", len(result.State.Subjects))
		}

		result, err = js.StreamInfo("TEST")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		if len(result.State.Subjects) != 0 {
			t.Fatalf("expected 0 subjects details from StreamInfo, but got %d instead", len(result.State.Subjects))
		}
	})
}

func TestStreamNameBySubject(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
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
			{name: "lookup on not existing stream", streamName: "not.existing", err: nats.ErrNoMatchingStream},
		} {

			stream, err := js.StreamNameBySubject(test.streamName)
			if err != test.err {
				t.Fatalf("expected %v, got %v", test.err, err)
			}

			if stream != "TEST" && err == nil {
				t.Fatalf("returned stream name should be 'TEST'")
			}
		}
	})
}

func TestJetStreamTransform(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Name:             "ORIGIN",
			Subjects:         []string{"test"},
			SubjectTransform: &nats.SubjectTransformConfig{Source: ">", Destination: "transformed.>"},
			Storage:          nats.MemoryStorage,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		err = nc.Publish("test", []byte("1"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{
			Subjects: []string{},
			Name:     "SOURCING",
			Sources:  []*nats.StreamSource{{Name: "ORIGIN", SubjectTransforms: []nats.SubjectTransformConfig{{Source: ">", Destination: "fromtest.>"}}}},
			Storage:  nats.MemoryStorage,
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		sub, err := js.SubscribeSync("fromtest.>", nats.ConsumerMemoryStorage(), nats.BindStream("SOURCING"))
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
	})
}

func TestPullConsumerFetchRace(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		for i := range 3 {
			if _, err := js.Publish("FOO.123", []byte(fmt.Sprintf("msg-%d", i))); err != nil {
				t.Fatalf("Unexpected error during publish: %s", err)
			}
		}
		sub, err := js.PullSubscribe("FOO.123", "")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		cons, err := sub.ConsumerInfo()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		msgs, err := sub.FetchBatch(5)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		errCh := make(chan error)
		go func() {
			for {
				err := msgs.Error()
				if err != nil {
					errCh <- err
					return
				}
			}
		}()
		deleteErrCh := make(chan error, 1)
		go func() {
			time.Sleep(100 * time.Millisecond)
			if err := js.DeleteConsumer("foo", cons.Name); err != nil {
				deleteErrCh <- err
			}
			close(deleteErrCh)
		}()

		var i int
		for msg := range msgs.Messages() {
			if string(msg.Data) != fmt.Sprintf("msg-%d", i) {
				t.Fatalf("Invalid msg on index %d; expected: %s; got: %s", i, fmt.Sprintf("msg-%d", i), string(msg.Data))
			}
			i++
		}
		if i != 3 {
			t.Fatalf("Invalid number of messages received; want: %d; got: %d", 5, i)
		}
		select {
		case err := <-errCh:
			if !errors.Is(err, nats.ErrConsumerDeleted) {
				t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerDeleted, err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Expected error: %v; got: %v", nats.ErrConsumerDeleted, nil)
		}

		// wait until the consumer is deleted, otherwise we may close the connection
		// before the consumer delete response is received
		select {
		case ert, ok := <-deleteErrCh:
			if !ok {
				break
			}
			t.Fatalf("Error deleting consumer: %s", ert)
		case <-time.After(1 * time.Second):
			t.Fatalf("Expected done to be closed")
		}
	})
}

func TestJetStreamSubscribeConsumerCreateTimeout(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Nanosecond)
		defer cancel()
		_, err = js.SubscribeSync("", nats.BindStream("foo"), nats.Context(ctx))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected error")
		}
	})
}

func TestJetStreamPullSubscribeFetchErrOnReconnect(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		sub, err := js.PullSubscribe("FOO.123", "bar")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()
		errs := make(chan error, 1)
		go func() {
			time.Sleep(100 * time.Millisecond)
			errs <- nc.ForceReconnect()
		}()
		_, err = sub.Fetch(1, nats.MaxWait(time.Second))
		if !errors.Is(err, nats.ErrFetchDisconnected) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrFetchDisconnected, err)
		}
		if err := <-errs; err != nil {
			t.Fatalf("Error on reconnect: %v", err)
		}
	})
}

func TestJetStreamPullSubscribeFetchBatchErrOnReconnect(t *testing.T) {
	withJSServer(t, func(t *testing.T, nc *nats.Conn) {
		js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		sub, err := js.PullSubscribe("FOO.123", "bar")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer sub.Unsubscribe()
		errs := make(chan error, 1)
		go func() {
			time.Sleep(100 * time.Millisecond)
			errs <- nc.ForceReconnect()
		}()
		msgs, err := sub.FetchBatch(1, nats.MaxWait(time.Second), nats.PullHeartbeat(100*time.Millisecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		for range msgs.Messages() {
			t.Fatalf("Expected no messages, got one")
		}
		if !errors.Is(msgs.Error(), nats.ErrFetchDisconnected) {
			t.Fatalf("Expected error: %v; got: %v", nats.ErrFetchDisconnected, msgs.Error())
		}
		if err := <-errs; err != nil {
			t.Fatalf("Error on reconnect: %v", err)
		}
	})
}

func TestJetStreamSubscribeShortTimeoutWithContext(t *testing.T) {
	withJSServerInstance(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		// create separate JetStream context to create a stream
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		_, err = js.AddStream(&nats.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// subscribe with MaxWait set to nanosecond
		// this should fail with context.DeadlineExceeded
		jsShort, err := nc.JetStream(nats.MaxWait(time.Nanosecond))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		// no context - creating consumer should fail with js.MaxWait set to nanosecond
		_, err = jsShort.SubscribeSync("FOO.123", nats.BindStream("foo"))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected error: %v; got: %v", context.DeadlineExceeded, err)
		}

		// use context.Background() - creating consumer should fail with js.MaxWait set to nanosecond
		_, err = jsShort.SubscribeSync("FOO.123", nats.BindStream("foo"), nats.Context(context.Background()))
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Expected error: %v; got: %v", context.DeadlineExceeded, err)
		}
	})
}
