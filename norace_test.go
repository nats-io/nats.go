// Copyright 2019 The NATS Authors
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
	"testing"
	"time"
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
}
