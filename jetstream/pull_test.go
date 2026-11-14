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
	"encoding/json"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

// fakeServer is a minimal NATS server: it answers pull requests on
// "$JS.API.CONSUMER.MSG.NEXT." by delivering the requested batch to the reply
// subject, and reports every pull request on pulls.
type fakeServer struct {
	ln    net.Listener
	pulls chan pullRequest
}

func newFakeServer(t *testing.T) *fakeServer {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	fs := &fakeServer{ln: ln, pulls: make(chan pullRequest, 16)}
	t.Cleanup(func() { ln.Close() })
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go fs.serve(c)
		}
	}()
	return fs
}

func (fs *fakeServer) serve(c net.Conn) {
	defer c.Close()
	var mu sync.Mutex
	send := func(format string, args ...any) {
		mu.Lock()
		defer mu.Unlock()
		fmt.Fprintf(c, format, args...)
	}
	send("INFO {\"server_id\":\"fake\",\"version\":\"2.10.0\",\"proto\":1,\"max_payload\":1048576}\r\n")
	sids := map[string]string{} // subject -> sid
	r := bufio.NewReader(c)
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			return
		}
		f := strings.Fields(line)
		switch {
		case len(f) == 0:
		case f[0] == "PING":
			send("PONG\r\n")
		case f[0] == "SUB":
			sids[f[1]] = f[len(f)-1]
		case f[0] == "PUB": // PUB <subject> <reply> <size>
			size, _ := strconv.Atoi(f[len(f)-1])
			payload := make([]byte, size+2)
			if _, err := io.ReadFull(r, payload); err != nil {
				return
			}
			if !strings.HasPrefix(f[1], "$JS.API.CONSUMER.MSG.NEXT.") {
				continue
			}
			var req pullRequest
			if err := json.Unmarshal(payload[:size], &req); err != nil {
				return
			}
			fs.pulls <- req
			for i := 0; i < req.Batch; i++ {
				send("MSG %s %s 1\r\nx\r\n", f[2], sids[f[2]])
			}
		}
	}
}

// newFakeConsumer returns a pull consumer connected to a fake server.
func newFakeConsumer(t *testing.T) (*pullConsumer, *fakeServer) {
	t.Helper()
	fs := newFakeServer(t)
	nc, err := nats.Connect("nats://" + fs.ln.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := New(nc)
	if err != nil {
		t.Fatal(err)
	}
	c := &pullConsumer{js: js.(*jetStream), stream: "S", name: "C"}
	c.info.Store(&ConsumerInfo{})
	return c, fs
}

func (fs *fakeServer) expectPull(t *testing.T, batch int) {
	t.Helper()
	select {
	case req := <-fs.pulls:
		if req.Batch != batch {
			t.Fatalf("Expected pull request for %d messages, got %d", batch, req.Batch)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("No pull request for %d messages", batch)
	}
}

// A new pull request must be issued even if all messages of the previous one
// were handled before pullMessages cleared fetchInProgress. Without a wake-up,
// nothing is requested from the server until the missing heartbeat timeout.
func TestPullRequestAfterFetchInProgress(t *testing.T) {
	t.Run("Messages", func(t *testing.T) {
		c, fs := newFakeConsumer(t)
		it, err := c.Messages(PullMaxMessages(4))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(it.Stop)
		sub := it.(*pullSubscription)

		// Hold pullMessages after the pull request was published, with
		// fetchInProgress still set.
		release := make(chan struct{})
		sub.afterPull = func() { <-release }

		// The server delivers the whole batch while the hook is blocking, so
		// all of it is consumed with fetchInProgress set.
		for i := 0; i < 4; i++ {
			if _, err := it.Next(NextMaxWait(5 * time.Second)); err != nil {
				t.Fatalf("Next %d: %v", i, err)
			}
		}
		fs.expectPull(t, 4)

		// Park Next, then let pullMessages finish.
		errCh := make(chan error, 1)
		go func() {
			_, err := it.Next(NextMaxWait(10 * time.Second))
			errCh <- err
		}()
		for sub.TryLock() { // Next holds the lock while it is waiting
			sub.Unlock()
			time.Sleep(time.Millisecond)
		}
		time.Sleep(50 * time.Millisecond) // let it get past checkPending into select
		close(release)

		fs.expectPull(t, 4)
		if err := <-errCh; err != nil {
			t.Fatalf("Next: %v", err)
		}
	})

	t.Run("Consume", func(t *testing.T) {
		c, fs := newFakeConsumer(t)
		start := make(chan struct{})
		var handled atomic.Int32
		cc, err := c.Consume(func(Msg) {
			<-start
			handled.Add(1)
		}, PullMaxMessages(4))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(cc.Stop)
		sub := cc.(*pullSubscription)

		release := make(chan struct{})
		sub.afterPull = func() { <-release }
		close(start)

		// The initial pull is for 4 messages. Handling the third one drops
		// below the threshold and requests 3 more. The server delivers them
		// while the hook is blocking, so all 7 are handled with
		// fetchInProgress set and nothing more is requested.
		fs.expectPull(t, 4)
		fs.expectPull(t, 3)
		pending := func() int {
			sub.Lock()
			defer sub.Unlock()
			return sub.pending.msgCount
		}
		for deadline := time.Now().Add(5 * time.Second); handled.Load() != 7 || pending() != 0; {
			if time.Now().After(deadline) {
				t.Fatalf("Handled %d messages, %d pending", handled.Load(), pending())
			}
			time.Sleep(time.Millisecond)
		}
		close(release)

		fs.expectPull(t, 4)
	})
}
