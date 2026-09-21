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
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// Concurrent registerPAF used to insert before stalling, so pending could
// exceed WithPublishAsyncMaxPending (issue #1612). Cap check and insert now
// share one lock; this white-box burst does not need a server.
func TestPublishAsyncMaxPendingNotExceeded(t *testing.T) {
	const maxPending = 1
	js := &jetStream{
		publisher: &jetStreamClient{
			asyncPublisherOpts: asyncPublisherOpts{maxpa: maxPending},
			asyncPublishContext: asyncPublishContext{
				replyPrefix: "test.",
				rr:          rand.New(rand.NewSource(1)),
			},
		},
	}

	const n = 32
	var observed atomic.Int32
	track := func() {
		p := int32(js.PublishAsyncPending())
		for {
			cur := observed.Load()
			if p <= cur || observed.CompareAndSwap(cur, p) {
				return
			}
		}
	}

	stopPoll := make(chan struct{})
	var pollWg sync.WaitGroup
	pollWg.Add(1)
	go func() {
		defer pollWg.Done()
		for {
			select {
			case <-stopPoll:
				return
			default:
				track()
			}
		}
	}()

	start := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			<-start
			paf := &pubAckFuture{jsClient: js.publisher}
			_, _ = js.registerPAF(paf, time.Second)
			track()
		}()
	}

	close(start)
	// Concurrent callers insert before stalling on unmodified code;
	// sample while they are still in-flight / stalled.
	time.Sleep(50 * time.Millisecond)
	track()

	wg.Wait()
	close(stopPoll)
	pollWg.Wait()

	if got := observed.Load(); int(got) > maxPending {
		t.Fatalf("PublishAsyncPending exceeded max pending: got %d, want <= %d", got, maxPending)
	}
}

// clearPAF frees a pending slot; stalled registerPAF callers must be woken
// instead of sitting out the full stallWait (review on #2126).
func TestRegisterPAFUnstallOnClear(t *testing.T) {
	const maxPending = 1
	js := &jetStream{
		publisher: &jetStreamClient{
			asyncPublisherOpts: asyncPublisherOpts{maxpa: maxPending},
			asyncPublishContext: asyncPublishContext{
				replyPrefix: "test.",
				rr:          rand.New(rand.NewSource(1)),
			},
		},
	}

	paf0 := &pubAckFuture{jsClient: js.publisher}
	reply0, err := js.registerPAF(paf0, time.Second)
	if err != nil {
		t.Fatalf("registerPAF: %v", err)
	}
	id0 := reply0[len(js.publisher.replyPrefix):]

	done := make(chan error, 1)
	go func() {
		paf := &pubAckFuture{jsClient: js.publisher}
		_, err := js.registerPAF(paf, time.Second)
		done <- err
	}()

	// Let the goroutine reach the stall wait.
	time.Sleep(20 * time.Millisecond)
	js.clearPAF(id0)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("expected registerPAF to succeed after clearPAF, got %v", err)
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("registerPAF did not wake after clearPAF")
	}
}
