// Copyright 2018-2026 The NATS Authors
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
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

// Drain can be very useful for graceful shutdown of subscribers.
// Especially queue subscribers.
func TestDrain(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		done := make(chan bool)
		received := int32(0)
		expected := int32(100)

		cb := func(_ *nats.Msg) {
			time.Sleep(time.Millisecond)
			rcvd := atomic.AddInt32(&received, 1)
			if rcvd >= expected {
				done <- true
			}
		}

		sub, err := nc.Subscribe("foo", cb)
		if err != nil {
			t.Fatalf("Error creating subscription; %v", err)
		}

		for i := int32(0); i < expected; i++ {
			nc.Publish("foo", []byte("Don't forget about me"))
		}

		sub.Drain()
		if !sub.IsDraining() {
			t.Fatalf("Expected to be draining")
		}
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			r := atomic.LoadInt32(&received)
			if r != expected {
				t.Fatalf("Did not receive all messages: %d of %d", r, expected)
			}
		}
		time.Sleep(100 * time.Millisecond)
		if sub.IsDraining() {
			t.Fatalf("Expected to be done draining")
		}
	})
}

func TestDrainQueueSub(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		done := make(chan bool)
		received := int32(0)
		expected := int32(4096)
		numSubs := int32(32)

		checkDone := func() int32 {
			rcvd := atomic.AddInt32(&received, 1)
			if rcvd >= expected {
				done <- true
			}
			return rcvd
		}

		callback := func(m *nats.Msg) {
			rcvd := checkDone()
			if rcvd%3 == 0 {
				m.Sub.Drain()
				nc.QueueSubscribe("foo", "bar", func(m *nats.Msg) { checkDone() })
			}
		}

		for i := int32(0); i < numSubs; i++ {
			if _, err := nc.QueueSubscribe("foo", "bar", callback); err != nil {
				t.Fatalf("Error creating subscription; %v", err)
			}
		}

		for i := int32(0); i < expected; i++ {
			nc.Publish("foo", []byte("Don't forget about me"))
		}

		select {
		case <-done:
		case <-time.After(5 * time.Second):
			r := atomic.LoadInt32(&received)
			if r != expected {
				t.Fatalf("Did not receive all messages: %d of %d", r, expected)
			}
		}
	})
}

func TestDrainUnSubs(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		num := 100
		subs := make([]*nats.Subscription, num)

		for i := 0; i < num; i++ {
			sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {})
			if err != nil {
				t.Fatalf("Error creating subscription; %v", err)
			}
			subs[i] = sub
		}

		if numSubs := nc.NumSubscriptions(); numSubs != num {
			t.Fatalf("Expected %d subscriptions, got %d", num, numSubs)
		}
		for i := 0; i < num; i++ {
			subs[i].Unsubscribe()
		}
		if numSubs := nc.NumSubscriptions(); numSubs != 0 {
			t.Fatalf("Expected no subscriptions, got %d", numSubs)
		}

		for i := 0; i < num; i++ {
			sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {})
			if err != nil {
				t.Fatalf("Error creating subscription; %v", err)
			}
			subs[i] = sub
		}

		if numSubs := nc.NumSubscriptions(); numSubs != num {
			t.Fatalf("Expected %d subscriptions, got %d", num, numSubs)
		}
		for i := 0; i < num; i++ {
			subs[i].Drain()
		}
		waitFor(t, 2*time.Second, 10*time.Millisecond, func() error {
			if numSubs := nc.NumSubscriptions(); numSubs != 0 {
				return fmt.Errorf("Expected no subscriptions, got %d", numSubs)
			}
			return nil
		})
	})
}

func TestDrainSlowSubscriber(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		received := int32(0)

		sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
			atomic.AddInt32(&received, 1)
			time.Sleep(100 * time.Millisecond)
		})
		if err != nil {
			t.Fatalf("Error creating subscription; %v", err)
		}

		total := 10
		for i := 0; i < total; i++ {
			nc.Publish("foo", []byte("Slow Slow"))
		}
		nc.Flush()

		pmsgs, _, _ := sub.Pending()
		if pmsgs != total && pmsgs != total-1 {
			t.Fatalf("Expected most messages to be pending, but got %d vs %d", pmsgs, total)
		}
		sub.Drain()

		waitFor(t, 2*time.Second, 100*time.Millisecond, func() error {
			_, _, err := sub.Pending()
			if err != nats.ErrBadSubscription {
				return errors.New("Still valid")
			}
			r := int(atomic.LoadInt32(&received))
			if r != total {
				t.Fatalf("Did not receive all messages, got %d vs %d", r, total)
			}
			return nil
		})
	})
}

func TestDrainConnection(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		done := make(chan bool)
		rdone := make(chan bool)

		closed := func(_ *nats.Conn) {
			done <- true
		}

		nc, err := nats.Connect(inst.Servers[0].URL, nats.ClosedHandler(closed))
		if err != nil {
			t.Fatalf("Failed to create default connection: %v", err)
		}
		defer nc.Close()

		nc2, err := nats.Connect(inst.Servers[0].URL)
		if err != nil {
			t.Fatalf("Failed to create default connection: %v", err)
		}
		defer nc2.Close()

		received := int32(0)
		responses := int32(0)
		expected := int32(50)
		sleep := 10 * time.Millisecond

		_, err = nc2.Subscribe("bar", func(_ *nats.Msg) {
			r := atomic.AddInt32(&responses, 1)
			if r == expected {
				rdone <- true
			}
		})
		if err != nil {
			t.Fatalf("Error creating subscription for responses: %v", err)
		}

		sub, err := nc.Subscribe("foo", func(m *nats.Msg) {
			time.Sleep(sleep)
			atomic.AddInt32(&received, 1)
			if err := nc.Publish(m.Reply, []byte("Stop bugging me")); err != nil {
				t.Errorf("Publisher received an error sending response: %v", err)
			}
		})
		if err != nil {
			t.Fatalf("Error creating subscription; %v", err)
		}

		for i := int32(0); i < expected; i++ {
			nc.PublishRequest("foo", "bar", []byte("Slow Slow"))
		}

		drainStart := time.Now()
		nc.Drain()

		if err := sub.Unsubscribe(); err == nil {
			t.Fatalf("Expected to receive an error on Unsubscribe after drain")
		}
		if _, err := nc.Subscribe("foo", func(_ *nats.Msg) {}); err == nil {
			t.Fatalf("Expected to receive an error on new Subscription after drain")
		}

		if err := nc.Publish("baz", []byte("Slow Slow")); err != nil {
			t.Fatalf("Expected to not receive an error on Publish after drain, got %v", err)
		}

		select {
		case <-done:
			if time.Since(drainStart) < (sleep * time.Duration(expected)) {
				t.Fatalf("Drain exited too soon")
			}
			r := atomic.LoadInt32(&received)
			if r != expected {
				t.Fatalf("Did not receive all messages from Drain, %d vs %d", r, expected)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for closed state for connection")
		}

		select {
		case <-rdone:
			r := atomic.LoadInt32(&responses)
			if r != expected {
				t.Fatalf("Did not receive all responses, %d vs %d", r, expected)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for all the responses")
		}
	})
}

func TestDrainConnectionAutoUnsub(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		errCount := int32(0)
		received := int32(0)
		expected := int32(10)

		done := make(chan bool)

		closed := func(_ *nats.Conn) {
			done <- true
		}
		errCb := func(_ *nats.Conn, _ *nats.Subscription, _ error) {
			atomic.AddInt32(&errCount, 1)
		}

		nc, err := nats.Connect(inst.Servers[0].URL,
			nats.ErrorHandler(errCb),
			nats.ClosedHandler(closed))
		if err != nil {
			t.Fatalf("Failed to create default connection: %v", err)
		}
		defer nc.Close()

		sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
			time.Sleep(10 * time.Millisecond)
			atomic.AddInt32(&received, 1)
		})
		if err != nil {
			t.Fatalf("Error creating subscription; %v", err)
		}

		sub.AutoUnsubscribe(int(expected))

		for i := 0; i < 50; i++ {
			nc.Publish("foo", []byte("Only 10 please!"))
		}
		nc.Flush()

		time.Sleep(10 * time.Millisecond)
		nc.Drain()

		select {
		case <-done:
			errs := atomic.LoadInt32(&errCount)
			if errs > 0 {
				t.Fatalf("Did not expect any errors, got %d", errs)
			}
			r := atomic.LoadInt32(&received)
			if r != expected {
				t.Fatalf("Did not receive all messages from Drain, %d vs %d", r, expected)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for closed state for connection")
		}
	})
}

func TestDrainConnLastError(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		done := make(chan bool, 1)
		closedCb := func(_ *nats.Conn) {
			done <- true
		}

		nc, err := nats.Connect(inst.Servers[0].URL,
			nats.ClosedHandler(closedCb),
			nats.DrainTimeout(time.Millisecond))
		if err != nil {
			t.Fatalf("Failed to create default connection: %v", err)
		}
		defer nc.Close()

		nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {})

		wg := sync.WaitGroup{}
		wg.Add(1)
		if _, err := nc.Subscribe("foo", func(_ *nats.Msg) {
			time.Sleep(100 * time.Millisecond)
			wg.Done()
		}); err != nil {
			t.Fatalf("Error creating subscription; %v", err)
		}

		if err := nc.Publish("foo", []byte("msg")); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		if err := nc.Drain(); err != nil {
			t.Fatalf("Error on drain: %v", err)
		}

		select {
		case <-done:
			if e := nc.LastError(); e == nil || e != nats.ErrDrainTimeout {
				t.Fatalf("Expected last error to be set to %v, got %v", nats.ErrDrainTimeout, e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for closed state for connection")
		}

		wg.Wait()
	})
}

func TestDrainConnDuringReconnect(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		done := make(chan bool, 1)
		closedCb := func(_ *nats.Conn) {
			done <- true
		}

		nc, err := nats.Connect(inst.Servers[0].URL,
			nats.ClosedHandler(closedCb),
			nats.DrainTimeout(20*time.Millisecond))
		if err != nil {
			t.Fatalf("Failed to create default connection: %v", err)
		}
		defer nc.Close()

		// Shutdown the server.
		inst.StopServer(t, inst.Servers[0])

		waitFor(t, time.Second, 10*time.Millisecond, func() error {
			if nc.IsReconnecting() {
				return nil
			}
			return errors.New("Not reconnecting yet")
		})

		if err := nc.Drain(); err != nats.ErrConnectionReconnecting {
			t.Fatalf("Unexpected error on drain: %v", err)
		}

		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Fatalf("Timeout waiting for closed state for connection")
		}
	})
}

func TestDrainClosedHandlerRace(t *testing.T) {
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		nc, err := nats.Connect(inst.Servers[0].URL, nats.ClosedHandler(func(_ *nats.Conn) {}))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		if err := nc.Drain(); err != nil {
			t.Fatalf("Unexpected error on drain: %v", err)
		}
		go func() {
			nc.SetClosedHandler(nil)
		}()
		time.Sleep(500 * time.Millisecond)
	})
}
