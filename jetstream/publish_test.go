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
	"testing"
	"time"
)

// clearPAF frees a pending slot; stalled registerPAF callers must be woken
// instead of sitting out the full stallWait.
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
