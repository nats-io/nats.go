// Copyright 2026 The NATS Authors
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

package jetstream

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type fakePushServer struct {
	ln     net.Listener
	mu     sync.Mutex
	conn   net.Conn
	subCh  chan string
	closed chan struct{}
}

func startFakePushServer(t *testing.T) *fakePushServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &fakePushServer{
		ln:     ln,
		subCh:  make(chan string, 4),
		closed: make(chan struct{}),
	}
	go func() {
		defer close(s.closed)
		c, err := ln.Accept()
		if err != nil {
			return
		}
		s.mu.Lock()
		s.conn = c
		s.mu.Unlock()
		defer c.Close()

		_, _ = c.Write([]byte(`INFO {"server_id":"S1","version":"2.11.0","headers":true,"max_payload":1048576}` + "\r\n"))
		rd := bufio.NewReader(c)
		for {
			line, err := rd.ReadString('\n')
			if err != nil {
				return
			}
			line = strings.TrimSpace(line)
			switch {
			case strings.HasPrefix(line, "PING"):
				s.mu.Lock()
				_, _ = c.Write([]byte("PONG\r\n"))
				s.mu.Unlock()
			case strings.HasPrefix(line, "SUB "):
				fields := strings.Fields(line)
				if len(fields) >= 3 {
					sid := fields[len(fields)-1]
					select {
					case s.subCh <- sid:
					default:
					}
				}
			}
		}
	}()
	t.Cleanup(func() {
		ln.Close()
		s.mu.Lock()
		if s.conn != nil {
			s.conn.Close()
		}
		s.mu.Unlock()
		<-s.closed
	})
	return s
}

func (s *fakePushServer) sendStatus(t *testing.T, subject, sid, statusLine string) {
	t.Helper()
	hdr := fmt.Sprintf("NATS/1.0 %s\r\n\r\n", statusLine)
	frame := fmt.Sprintf("HMSG %s %s %d %d\r\n%s\r\n", subject, sid, len(hdr), len(hdr), hdr)
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.conn == nil {
		t.Fatal("server has no client connection")
	}
	if _, err := s.conn.Write([]byte(frame)); err != nil {
		t.Fatalf("write status frame: %v", err)
	}
}

func newTestPushConsumer(t *testing.T, nc *nats.Conn, deliverSubject string, idleHeartbeat time.Duration) *pushConsumer {
	t.Helper()
	js, err := New(nc)
	if err != nil {
		t.Fatalf("jetstream.New: %v", err)
	}
	pc := &pushConsumer{
		js:     js.(*jetStream),
		stream: "TEST",
		name:   "push",
	}
	pc.info.Store(&ConsumerInfo{
		Stream: "TEST",
		Name:   "push",
		Config: ConsumerConfig{
			DeliverSubject: deliverSubject,
			IdleHeartbeat:  idleHeartbeat,
		},
	})
	return pc
}

func TestPushConsumerStatusConsumerDeleted(t *testing.T) {
	for _, descr := range []string{"Consumer Deleted", "consumer deleted"} {
		t.Run(descr, func(t *testing.T) {
			srv := startFakePushServer(t)
			nc, err := nats.Connect("nats://" + srv.ln.Addr().String())
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer nc.Close()

			const subject = "deliver.deleted"
			pc := newTestPushConsumer(t, nc, subject, 0)
			errCh := make(chan error, 4)
			cc, err := pc.Consume(
				func(Msg) {},
				ConsumeErrHandler(func(_ ConsumeContext, err error) {
					errCh <- err
				}),
			)
			if err != nil {
				t.Fatalf("Consume: %v", err)
			}
			if err := nc.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}

			var sid string
			select {
			case sid = <-srv.subCh:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for SUB")
			}

			closedCh := cc.Closed()
			srv.sendStatus(t, subject, sid, "409 "+descr)

			select {
			case got := <-errCh:
				if !errors.Is(got, ErrConsumerDeleted) {
					t.Fatalf("expected ErrConsumerDeleted, got %v", got)
				}
			case <-time.After(time.Second):
				t.Fatalf("timed out waiting for ErrConsumerDeleted (%q)", descr)
			}

			select {
			case <-closedCh:
			case <-time.After(time.Second):
				t.Fatalf("push consumer deadlocked or failed to stop on %q", descr)
			}
		})
	}
}

func TestPushConsumerStatusLeadershipChangeCalledOnce(t *testing.T) {
	for _, descr := range []string{"Leadership Change", "leadership change"} {
		t.Run(descr, func(t *testing.T) {
			srv := startFakePushServer(t)
			nc, err := nats.Connect("nats://" + srv.ln.Addr().String())
			if err != nil {
				t.Fatalf("connect: %v", err)
			}
			defer nc.Close()

			const subject = "deliver.leader"
			pc := newTestPushConsumer(t, nc, subject, 0)
			var count atomic.Int32
			errCh := make(chan error, 4)
			cc, err := pc.Consume(
				func(Msg) {},
				ConsumeErrHandler(func(_ ConsumeContext, err error) {
					count.Add(1)
					errCh <- err
				}),
			)
			if err != nil {
				t.Fatalf("Consume: %v", err)
			}
			defer cc.Stop()
			if err := nc.Flush(); err != nil {
				t.Fatalf("Flush: %v", err)
			}

			var sid string
			select {
			case sid = <-srv.subCh:
			case <-time.After(2 * time.Second):
				t.Fatal("timed out waiting for SUB")
			}

			srv.sendStatus(t, subject, sid, "409 "+descr)

			select {
			case got := <-errCh:
				if !errors.Is(got, ErrConsumerLeadershipChanged) {
					t.Fatalf("expected ErrConsumerLeadershipChanged, got %v", got)
				}
			case <-time.After(time.Second):
				t.Fatalf("timed out waiting for ErrConsumerLeadershipChanged (%q)", descr)
			}

			time.Sleep(50 * time.Millisecond)
			if got := count.Load(); got != 1 {
				t.Fatalf("ErrHandler called %d times for %q; want 1", got, descr)
			}
		})
	}
}

func TestPushConsumerErrHandlerStopNoDeadlock(t *testing.T) {
	srv := startFakePushServer(t)
	nc, err := nats.Connect("nats://" + srv.ln.Addr().String())
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer nc.Close()

	const subject = "deliver.hb"
	pc := newTestPushConsumer(t, nc, subject, 30*time.Millisecond)
	stopped := make(chan error, 1)
	cc, err := pc.Consume(
		func(Msg) {},
		ConsumeErrHandler(func(ctx ConsumeContext, err error) {
			ctx.Stop()
			stopped <- err
		}),
	)
	if err != nil {
		t.Fatalf("Consume: %v", err)
	}
	defer cc.Stop()

	select {
	case got := <-stopped:
		if !errors.Is(got, ErrNoHeartbeat) {
			t.Fatalf("expected ErrNoHeartbeat, got %v", got)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("deadlock calling Stop() from ConsumeErrHandler on ErrNoHeartbeat")
	}
}

func TestPushSubscriptionHeartbeatTimerDoesNotBlockStop(t *testing.T) {
	nc := &nats.Conn{}
	sub := &pushSubscription{
		errs:              make(chan error, 1),
		conn:              nc,
		done:              make(chan struct{}),
		connStatusChanged: nc.StatusChanged(nats.CONNECTED, nats.RECONNECTING),
		consumeOpts:       &pushConsumeOpts{},
	}
	sub.errs <- ErrNoHeartbeat
	sub.hbMonitor = sub.scheduleHeartbeatCheck(10 * time.Millisecond)
	time.Sleep(40 * time.Millisecond)

	done := make(chan struct{})
	go func() {
		sub.Lock()
		close(sub.done)
		sub.release()
		sub.Unlock()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("heartbeat timer callback deadlocked waiting on full errs channel while holding lock")
	}
}
