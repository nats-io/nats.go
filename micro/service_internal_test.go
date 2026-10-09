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
	"errors"
	"testing"

	"github.com/nats-io/nats.go"
)

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

func TestConnHooks(t *testing.T) {
	nc := &nats.Conn{}
	first, second := &service{nc: nc}, &service{nc: nc}
	registerService(first)
	if nc.ClosedHandler() == nil || nc.ErrorHandler() == nil {
		t.Fatal("Expected the first service to install the connection handlers")
	}
	errHandler := nc.ErrorHandler()
	nc.SetErrorHandler(nil)
	registerService(second)
	if nc.ErrorHandler() != nil {
		t.Fatal("Expected a later service not to reinstall the connection handlers")
	}
	nc.SetErrorHandler(errHandler)
	unregisterService(first)
	h, ok := hooks.conns[nc]
	if !ok {
		t.Fatal("Expected the connection to stay registered while it has services")
	}
	if got := len(h.services); got != 1 {
		t.Fatalf("Expected 1 registered service; got %d", got)
	}
	unregisterService(second)
	if _, ok := hooks.conns[nc]; ok {
		t.Fatal("Expected the connection to be removed with its last service")
	}
	if nc.ClosedHandler() != nil || nc.ErrorHandler() != nil {
		t.Fatal("Expected the connection handlers to be restored with its last service")
	}

	_, err := AddService(nc, Config{
		Name:    "test_service",
		Version: "0.1.0",
		Endpoint: &EndpointConfig{
			Subject: "endpoint subject",
			Handler: HandlerFunc(func(Request) {}),
		},
	})
	if !errors.Is(err, ErrConfigValidation) {
		t.Fatalf("Expected %v; got: %v", ErrConfigValidation, err)
	}
	if _, ok := hooks.conns[nc]; ok {
		t.Fatal("Expected a failed AddService to leave nothing registered")
	}
}
