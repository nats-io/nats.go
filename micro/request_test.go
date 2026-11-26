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

import (
	"slices"
	"sync"
	"testing"

	"github.com/nats-io/nats.go"
)

func TestWithHeadersDoesNotMutateCallerMapsOrSlices(t *testing.T) {
	base := Headers{
		"X-Base":  []string{"one", "two"},
		"X-Share": []string{"base"},
	}
	extra := Headers{
		"X-Extra": []string{"added"},
		"X-Share": []string{"override"},
	}

	msg := &nats.Msg{}
	WithHeaders(base)(msg)
	WithHeaders(extra)(msg)

	if _, leaked := base["X-Extra"]; leaked {
		t.Fatalf("caller base headers mutated with extra key: %v", base)
	}
	if !slices.Equal(base["X-Share"], []string{"base"}) {
		t.Fatalf("caller base headers overwritten: got %v; want [base]", base["X-Share"])
	}

	// Mutating caller slices after applying WithHeaders must not alter msg.Header.
	base["X-Base"][0] = "mutated"
	extra["X-Extra"][0] = "mutated"

	if !slices.Equal(msg.Header.Values("X-Base"), []string{"one", "two"}) {
		t.Fatalf("msg X-Base aliased caller slice: got %v; want [one two]", msg.Header.Values("X-Base"))
	}
	if !slices.Equal(msg.Header.Values("X-Extra"), []string{"added"}) {
		t.Fatalf("msg X-Extra aliased caller slice: got %v; want [added]", msg.Header.Values("X-Extra"))
	}
	if !slices.Equal(msg.Header.Values("X-Share"), []string{"override"}) {
		t.Fatalf("msg X-Share = %v; want [override]", msg.Header.Values("X-Share"))
	}
}

func TestWithHeadersConcurrentSharedBase(t *testing.T) {
	shared := Headers{
		"X-Service": []string{"demo"},
	}

	var wg sync.WaitGroup
	for i := range 16 {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := &nats.Msg{}
			WithHeaders(shared)(msg)
			WithHeaders(Headers{
				"X-Request": []string{"req"},
			})(msg)
			if got := msg.Header.Get("X-Service"); got != "demo" {
				t.Errorf("goroutine %d: X-Service = %q; want demo", id, got)
			}
			if got := msg.Header.Get("X-Request"); got != "req" {
				t.Errorf("goroutine %d: X-Request = %q; want req", id, got)
			}
		}(i)
	}
	wg.Wait()

	if _, leaked := shared["X-Request"]; leaked {
		t.Fatalf("shared headers map mutated: %v", shared)
	}
}

func TestWithHeadersEmptyAndExisting(t *testing.T) {
	msg := &nats.Msg{}
	WithHeaders(nil)(msg)
	WithHeaders(Headers{})(msg)
	if msg.Header != nil {
		t.Fatalf("expected nil Header for empty WithHeaders, got %v", msg.Header)
	}

	errMsg := &nats.Msg{
		Header: nats.Header{
			ErrorHeader:     []string{"boom"},
			ErrorCodeHeader: []string{"500"},
		},
	}
	custom := Headers{"X-Trace": []string{"abc"}}
	WithHeaders(custom)(errMsg)
	custom["X-Trace"][0] = "changed"

	if got := errMsg.Header.Get("X-Trace"); got != "abc" {
		t.Fatalf("X-Trace = %q; want abc", got)
	}
	if got := errMsg.Header.Get(ErrorCodeHeader); got != "500" {
		t.Fatalf("%s = %q; want 500", ErrorCodeHeader, got)
	}
}
