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

package test

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
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
	for _, reconnect := range []bool{false, true} {
		t.Run(fmt.Sprintf("reconnect=%v", reconnect), func(t *testing.T) {
			testDrainSlowSubscriber(t, reconnect)
		})
	}
}

func testDrainSlowSubscriber(t *testing.T, reconnect bool) {
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
		// Reconnecting mid-drain must not resubscribe on the new connection.
		if reconnect {
			if err := nc.ForceReconnect(); err != nil {
				t.Fatalf("Error on force reconnect: %v", err)
			}
		}

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
		if _, err := nc.Request("foo", nil, time.Second); !errors.Is(err, nats.ErrNoResponders) {
			t.Fatalf("Expected %v, got %v", nats.ErrNoResponders, err)
		}
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

func TestDrainFlushTimeout(t *testing.T) {
	// The mock server completes the handshake but holds back PONGs, so
	// the final flush of a drain times out after the configured
	// DrainFlushTimeout, not the 5s default, and the error is reported
	// to the async error handler before the connection is closed.
	answerPongs := &atomic.Bool{}
	natsURL := startHoldPongServer(t, answerPongs)

	errCh := make(chan error, 1)
	closed := make(chan struct{})
	nc, err := nats.Connect(natsURL,
		nats.NoReconnect(),
		// DrainTimeout is much longer than the waits below so the test
		// fails if the final flush is ever wired to the wrong option:
		// the connection would stay open past the waits. There are no
		// subscriptions, so the subscription drain phase completes
		// immediately and the long value does not slow the test down.
		// The dial timeout is raised for the same reason: the 2s default
		// would fall inside the waits below.
		nats.DrainTimeout(10*time.Second),
		nats.Timeout(8*time.Second),
		nats.DrainFlushTimeout(100*time.Millisecond),
		nats.ErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, err error) {
			select {
			case errCh <- err:
			default:
			}
		}),
		nats.ClosedHandler(func(_ *nats.Conn) {
			close(closed)
		}),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	if nc.Opts.DrainFlushTimeout != 100*time.Millisecond {
		t.Fatalf("Expected DrainFlushTimeout to be 100ms, got %v", nc.Opts.DrainFlushTimeout)
	}

	drainStart := time.Now()
	if err := nc.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}

	// With no subscriptions, the drain phase completes immediately and
	// the final flush fails with ErrTimeout. The failure must arrive
	// after the configured DrainFlushTimeout (a shorter fixed wait would
	// mean the option is ignored) and well before the waits above expire
	// even against the long DrainTimeout.
	select {
	case err := <-errCh:
		if err != nats.ErrTimeout {
			t.Fatalf("Expected ErrTimeout from final drain flush, got %v", err)
		}
		if elapsed := time.Since(drainStart); elapsed < 100*time.Millisecond {
			t.Fatalf("Final drain flush failed after %v with DrainFlushTimeout=100ms, option ignored?", elapsed)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for final drain flush error")
	}

	// The connection must still be closed after the failed flush.
	select {
	case <-closed:
	case <-time.After(3 * time.Second):
		t.Fatal("Timeout waiting for closed state for connection")
	}
	if !nc.IsClosed() {
		t.Fatal("Expected connection to be closed")
	}
}

func TestSubDrainShortFlushWait(t *testing.T) {
	// Regression test for short FlushWait values: the flush that guards
	// the end of a subscription drain must keep the default wait. The
	// mock server queues the last message behind the drain flush's PONG
	// and releases both after a delay much longer than the configured
	// FlushWait. If the drain used the short FlushWait for its protocol
	// confirmation, it would complete (and remove the subscription)
	// before the delayed message is delivered, dropping it.
	serverInfo := "INFO {\"server_id\":\"foobar\",\"host\":\"%s\",\"port\":%d,\"auth_required\":false,\"tls_required\":false,\"max_payload\":1048576}\r\n"

	l, e := net.Listen("tcp", "127.0.0.1:0")
	if e != nil {
		t.Fatal("Could not listen on an ephemeral port")
	}
	t.Cleanup(func() { l.Close() })
	addr := l.Addr().(*net.TCPAddr)

	sawSub := make(chan bool)
	gotMsg := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		conn, err := l.Accept()
		if err != nil {
			errCh <- fmt.Errorf("error accepting client connection: %v", err)
			return
		}
		defer conn.Close()
		conn.Write([]byte(fmt.Sprintf(serverInfo, addr.IP, addr.Port)))

		// Read connect and ping commands sent from the client
		br := bufio.NewReaderSize(conn, 1024)
		if _, err := br.ReadString('\n'); err != nil {
			errCh <- fmt.Errorf("expected CONNECT from client, got: %s", err)
			return
		}
		if _, err := br.ReadString('\n'); err != nil {
			errCh <- fmt.Errorf("expected PING from client, got: %s", err)
			return
		}
		conn.Write([]byte("PONG\r\n"))

		var sid string
		draining := false
		held := false
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				// The client disconnected, which is expected on Close.
				return
			}
			fields := strings.Fields(line)
			switch {
			case len(fields) >= 3 && fields[0] == "SUB":
				sid = fields[2]
				close(sawSub)
			case len(fields) > 0 && fields[0] == "UNSUB":
				draining = true
			case len(fields) > 0 && fields[0] == "PING":
				if draining && !held {
					held = true
					// Hold back the message and the PONG answering the
					// drain flush, and release both together after a
					// delay longer than the configured FlushWait.
					go func() {
						time.Sleep(500 * time.Millisecond)
						msg := fmt.Sprintf("MSG foo %s 3\r\nbar\r\n", sid)
						conn.Write([]byte(msg))
						conn.Write([]byte("PONG\r\n"))
					}()
					continue
				}
				conn.Write([]byte("PONG\r\n"))
			}
		}
	}()

	// Wait for server mock to start
	time.Sleep(100 * time.Millisecond)
	t.Cleanup(func() { checkErrChannel(t, errCh) })

	nc, err := nats.Connect(fmt.Sprintf("nats://%s:%d", addr.IP, addr.Port),
		nats.NoReconnect(),
		nats.FlushWait(30*time.Millisecond),
	)
	if err != nil {
		t.Fatalf("Failed to connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.Subscribe("foo", func(_ *nats.Msg) {
		close(gotMsg)
	})
	if err != nil {
		t.Fatalf("Error creating subscription; %v", err)
	}
	if err := Wait(sawSub); err != nil {
		t.Fatal("Timeout waiting for subscription to reach the server")
	}

	if err := sub.Drain(); err != nil {
		t.Fatalf("Error on drain: %v", err)
	}

	// The message released with the drain flush PONG must be delivered.
	select {
	case <-gotMsg:
	case <-time.After(5 * time.Second):
		t.Fatal("Timeout waiting for message released with the drain flush PONG")
	}

	// The subscription is removed once the drain completes.
	waitFor(t, 2*time.Second, 10*time.Millisecond, func() error {
		if nc.NumSubscriptions() == 0 {
			return nil
		}
		return errors.New("Subscription not removed after drain")
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

// Closing the connection while drainConnection is still waiting for
// subscriptions to drain must abort the rest of the drain: the drain
// goroutine must not flip the (already closed) connection back to
// DRAINING_PUBS and start a new FlushTimeout on it, nor keep waiting
// for subscriptions that Close has already removed.
func TestDrainConnectionCloseDuringDrain(t *testing.T) {
	for _, tc := range []struct {
		name    string
		request bool
	}{
		{"no requests", false},
		{"after request", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			withServer(t, func(t *testing.T, nc *nats.Conn) {
				if tc.request {
					// Creates the shared response subscription, which
					// drainConnection waits on separately.
					if _, err := nc.Request("nobody", nil, time.Second); !errors.Is(err, nats.ErrNoResponders) {
						t.Fatalf("Expected %v, got %v", nats.ErrNoResponders, err)
					}
				}
				entered := make(chan struct{})
				release := make(chan struct{})
				sub, err := nc.Subscribe("pending", func(*nats.Msg) {
					close(entered)
					<-release
				})
				if err != nil {
					t.Fatalf("Error creating subscription: %v", err)
				}
				defer close(release)

				if err := nc.Publish("pending", nil); err != nil {
					t.Fatalf("Error on publish: %v", err)
				}
				if err := nc.Flush(); err != nil {
					t.Fatalf("Error on flush: %v", err)
				}
				select {
				case <-entered:
				case <-time.After(time.Second):
					t.Fatal("callback not entered")
				}

				if err := nc.Drain(); err != nil {
					t.Fatalf("Unexpected error on drain: %v", err)
				}

				// drainConnection marks the subscription as draining, so
				// Close lands mid-drain.
				waitFor(t, time.Second, 5*time.Millisecond, func() error {
					if !sub.IsDraining() {
						return errors.New("subscription not draining yet")
					}
					return nil
				})

				nc.Close()
				if !nc.IsClosed() {
					t.Fatalf("connection not closed after Close: %v", nc.Status())
				}

				time.Sleep(250 * time.Millisecond)
				if !nc.IsClosed() {
					t.Fatalf("connection resurrected after Close: %v", nc.Status())
				}
				for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); {
					b := make([]byte, 1<<20)
					n := runtime.Stack(b, true)
					if !strings.Contains(string(b[:n]), "(*Conn).drainConnection") {
						return
					}
					time.Sleep(10 * time.Millisecond)
				}
				t.Fatal("drain goroutine still running after Close")
			})
		})
	}
}
