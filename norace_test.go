// Copyright 2019-2020 The NATS Authors
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

// +build !race

package nats

import (
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
)

func TestNoRaceParseStateReconnectFunctionality(t *testing.T) {
	ts := RunServerOnPort(TEST_PORT)
	ch := make(chan bool)

	opts := reconnectOpts
	dch := make(chan bool)
	dErrCh := make(chan bool)
	opts.DisconnectedErrCB = func(_ *Conn, _ error) {
		dErrCh <- true
	}
	opts.DisconnectedCB = func(_ *Conn) {
		dch <- true
	}
	opts.NoCallbacksAfterClientClose = true

	nc, errc := opts.Connect()
	if errc != nil {
		t.Fatalf("Failed to create a connection: %v\n", errc)
	}
	ec, errec := NewEncodedConn(nc, DEFAULT_ENCODER)
	if errec != nil {
		nc.Close()
		t.Fatalf("Failed to create an encoded connection: %v\n", errec)
	}
	defer ec.Close()

	testString := "bar"
	ec.Subscribe("foo", func(s string) {
		if s != testString {
			t.Fatal("String doesn't match")
		}
		ch <- true
	})
	ec.Flush()

	// Simulate partialState, this needs to be cleared.
	nc.mu.Lock()
	nc.ps.state = OP_PON
	nc.mu.Unlock()

	ts.Shutdown()
	// server is stopped here...

	if err := Wait(dErrCh); err != nil {
		t.Fatal("Did not get the DisconnectedErrCB")
	}

	select {
	case <-dch:
		t.Fatal("Get the DEPRECATED DisconnectedCB while DisconnectedErrCB was set")
	default:
	}

	if err := ec.Publish("foo", testString); err != nil {
		t.Fatalf("Failed to publish message: %v\n", err)
	}

	ts = RunServerOnPort(TEST_PORT)
	defer ts.Shutdown()

	if err := ec.FlushTimeout(5 * time.Second); err != nil {
		t.Fatalf("Error on Flush: %v", err)
	}

	if err := Wait(ch); err != nil {
		t.Fatal("Did not receive our message")
	}

	expectedReconnectCount := uint64(1)
	reconnectedCount := ec.Conn.Stats().Reconnects

	if reconnectedCount != expectedReconnectCount {
		t.Fatalf("Reconnect count incorrect: %d vs %d\n",
			reconnectedCount, expectedReconnectCount)
	}
	nc.Close()
}

func TestNoRaceJetStreamConsumerSlowConsumer(t *testing.T) {
	// This test fails many times, need to look harder at the imbalance.
	t.SkipNow()

	s := RunServerOnPort(-1)
	defer s.Shutdown()

	if err := s.EnableJetStream(nil); err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}
	defer os.RemoveAll(s.JetStreamConfig().StoreDir)

	str, err := s.GlobalAccount().AddStream(&server.StreamConfig{
		Name:     "PENDING_TEST",
		Subjects: []string{"js.p"},
		Storage:  server.MemoryStorage,
	})
	if err != nil {
		t.Fatalf("stream create failed: %v", err)
	}

	nc, _ := Connect(s.ClientURL())
	defer nc.Close()

	// Override default handler for test.
	nc.SetErrorHandler(func(_ *Conn, _ *Subscription, _ error) {})

	// Queue up 1M small messages.
	toSend := uint64(1_000_000)
	for i := uint64(0); i < toSend; i++ {
		nc.Publish("js.p", []byte("ok"))
	}
	nc.Flush()

	if nm := str.State().Msgs; nm != toSend {
		t.Fatalf("Expected to have stored all %d msgs, got only %d", toSend, nm)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var received uint64
	done := make(chan bool, 1)

	js.Subscribe("js.p", func(m *Msg) {
		received++
		if received >= toSend {
			done <- true
		}
		meta, err := m.MetaData()
		if err != nil {
			t.Fatalf("could not get message metadata: %s", err)
		}
		if meta.Stream != received {
			t.Errorf("Missed a sequence, was expecting %d but got %d, last error: '%v'", received, meta.Stream, nc.LastError())
			nc.Close()
		}
		m.Ack()
	})

	select {
	case <-time.After(5 * time.Second):
		t.Fatalf("Failed to get all %d messages, only got %d", toSend, received)
	case <-done:
	}
}
