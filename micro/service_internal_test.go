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

package micro

import "testing"

func TestAsyncCallbacksHandlerClose(t *testing.T) {
	ac := &asyncCallbacksHandler{cbQueue: make(chan func(), 100), done: make(chan struct{})}
	var n int
	for range cap(ac.cbQueue) {
		ac.push(func() { n++ })
	}
	ac.close()
	ac.run()
	if n != cap(ac.cbQueue) {
		t.Fatalf("Expected %d callbacks to run; got %d", cap(ac.cbQueue), n)
	}
	for range 2 * cap(ac.cbQueue) {
		ac.push(func() { n++ })
	}
	ac.run()
	if n != cap(ac.cbQueue) {
		t.Fatalf("Expected callbacks pushed after close to be dropped; %d ran", n-cap(ac.cbQueue))
	}
	ac.close()
}
