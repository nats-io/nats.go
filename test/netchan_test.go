// Copyright 2013-2026 The NATS Authors
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
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

//lint:file-ignore SA1019 Ignore deprecation warnings for EncodedConn

// newEncodedConn wraps an existing *nats.Conn as a DEFAULT_ENCODER conn.
func newEncodedConn(t *testing.T, nc *nats.Conn) *nats.EncodedConn {
	t.Helper()
	ec, err := nats.NewEncodedConn(nc, nats.DEFAULT_ENCODER)
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

func TestBadChan(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)

		if err := ec.BindSendChan("foo", "not a chan"); err == nil {
			t.Fatalf("Expected an Error when sending a non-channel")
		}

		if _, err := ec.BindRecvChan("foo", "not a chan"); err == nil {
			t.Fatalf("Expected an Error when sending a non-channel")
		}

		if err := ec.BindSendChan("foo", "not a chan"); err != nats.ErrChanArg {
			t.Fatalf("Expected an ErrChanArg when sending a non-channel")
		}

		if _, err := ec.BindRecvChan("foo", "not a chan"); err != nats.ErrChanArg {
			t.Fatalf("Expected an ErrChanArg when sending a non-channel")
		}
	})
}

func TestSimpleSendChan(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)

		recv := make(chan bool)

		numSent := int32(22)
		ch := make(chan int32)

		if err := ec.BindSendChan("foo", ch); err != nil {
			t.Fatalf("Failed to bind to a send channel: %v", err)
		}

		ec.Subscribe("foo", func(num int32) {
			if num != numSent {
				t.Fatalf("Failed to receive correct value: %d vs %d", num, numSent)
			}
			recv <- true
		})

		ch <- numSent

		if e := Wait(recv); e != nil {
			if ec.LastError() != nil {
				e = ec.LastError()
			}
			t.Fatalf("Did not receive the message: %s", e)
		}
		close(ch)
	})
}

func TestFailedChannelSend(t *testing.T) {
	withServerInstance(t, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		// First half of the test uses the helper's connection.
		ec := newEncodedConn(t, nc)
		ch := make(chan bool)
		wch := make(chan bool)

		nc.Opts.AsyncErrorCB = func(_ *nats.Conn, _ *nats.Subscription, _ error) {
			wch <- true
		}

		if err := ec.BindSendChan("foo", ch); err != nil {
			t.Fatalf("Failed to bind to a send channel: %v", err)
		}

		nc.Flush()

		go func() {
			time.Sleep(100 * time.Millisecond)
			nc.Close()
		}()

		func() {
			for {
				select {
				case ch <- true:
				case <-wch:
					return
				case <-time.After(time.Second):
					t.Fatal("Failed to get async error cb")
				}
			}
		}()

		// Second half: open a fresh connection and try a too-big payload.
		nc2 := dialInstance(t, inst)
		ec2 := newEncodedConn(t, nc2)
		bch := make(chan []byte)

		nc2.Opts.AsyncErrorCB = func(_ *nats.Conn, _ *nats.Subscription, _ error) {
			wch <- true
		}

		if err := ec2.BindSendChan("foo", bch); err != nil {
			t.Fatalf("Failed to bind to a send channel: %v", err)
		}

		buf := make([]byte, 2*1024*1024)
		bch <- buf

		if e := Wait(wch); e != nil {
			t.Fatal("Failed to call async err handler")
		}
	})
}

func TestSimpleRecvChan(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)

		numSent := int32(22)
		ch := make(chan int32)

		if _, err := ec.BindRecvChan("foo", ch); err != nil {
			t.Fatalf("Failed to bind to a receive channel: %v", err)
		}

		ec.Publish("foo", numSent)

		select {
		case num := <-ch:
			if num != numSent {
				t.Fatalf("Failed to receive correct value: %d vs %d", num, numSent)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Failed to receive a value, timed-out")
		}
		close(ch)
	})
}

func TestQueueRecvChan(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)

		numSent := int32(22)
		ch := make(chan int32)

		if _, err := ec.BindRecvQueueChan("foo", "bar", ch); err != nil {
			t.Fatalf("Failed to bind to a queue receive channel: %v", err)
		}

		ec.Publish("foo", numSent)

		select {
		case num := <-ch:
			if num != numSent {
				t.Fatalf("Failed to receive correct value: %d vs %d", num, numSent)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Failed to receive a value, timed-out")
		}
		close(ch)
	})
}

func TestDecoderErrRecvChan(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)
		wch := make(chan bool)

		nc.Opts.AsyncErrorCB = func(_ *nats.Conn, _ *nats.Subscription, _ error) {
			wch <- true
		}

		ch := make(chan *int32)

		if _, err := ec.BindRecvChan("foo", ch); err != nil {
			t.Fatalf("Failed to bind to a recv channel: %v", err)
		}

		ec.Publish("foo", "Hello World")

		if e := Wait(wch); e != nil {
			t.Fatal("Failed to call async err handler")
		}
	})
}

func TestRecvChanPanicOnClosedChan(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)

		ch := make(chan int)

		if _, err := ec.BindRecvChan("foo", ch); err != nil {
			t.Fatalf("Failed to bind to a recv channel: %v", err)
		}

		close(ch)
		ec.Publish("foo", 22)
		ec.Flush()
	})
}

func TestRecvChanAsyncLeakGoRoutines(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)

		ec.Flush()

		base := getStableNumGoroutine(t)

		ch := make(chan int)

		if _, err := ec.BindRecvChan("foo", ch); err != nil {
			t.Fatalf("Failed to bind to a recv channel: %v", err)
		}

		close(ch)

		ec.Publish("foo", 22)
		ec.Flush()

		checkNoGoroutineLeak(t, base, "closing channel")
	})
}

func TestRecvChanLeakGoRoutines(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)

		ec.Flush()

		base := getStableNumGoroutine(t)

		ch := make(chan int)

		sub, err := ec.BindRecvChan("foo", ch)
		if err != nil {
			t.Fatalf("Failed to bind to a recv channel: %v", err)
		}
		sub.Unsubscribe()

		checkNoGoroutineLeak(t, base, "Unsubscribe()")
	})
}

func TestRecvChanMultipleMessages(t *testing.T) {
	// Make sure we can receive more than one message (#25 fix regression of #22).
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		ec := newEncodedConn(t, nc)

		size := 10
		ch := make(chan int, size)

		if _, err := ec.BindRecvChan("foo", ch); err != nil {
			t.Fatalf("Failed to bind to a recv channel: %v", err)
		}

		for i := 0; i < size; i++ {
			ec.Publish("foo", 22)
		}
		ec.Flush()
		time.Sleep(10 * time.Millisecond)

		if lch := len(ch); lch != size {
			t.Fatalf("Expected %d messages queued, got %d.", size, lch)
		}
	})
}

// BenchmarkPublishSpeedViaChan (original netchan_test.go) deferred to task 4.13 —
// testservice helpers take *testing.T, not *testing.B.
