// Copyright 2022 The NATS Authors
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

package nats

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

func TestServiceBasics(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	// Stub service.
	doAdd := func(svc Service, req *Msg) error {
		if rand.Intn(10) == 0 {
			return fmt.Errorf("Unexpected Error!")
		}
		// Happy Path.
		// Random delay between 5-10ms
		time.Sleep(5*time.Millisecond + time.Duration(rand.Intn(5))*time.Millisecond)
		req.Respond([]byte("42"))
		return nil
	}

	// Create 10 service responders.

	var svcs []Service

	// Create 5 service responders.
	for i := 0; i < 5; i++ {
		svc, err := nc.AddService(
			"CoolAddService",
			"Add things together",
			"v0.1",
			"svc.add",
			_EMPTY_, // TBD - request schema
			_EMPTY_, // TBD - response schema
			doAdd,
		)
		if err != nil {
			t.Fatalf("Expected to create service, got %v", err)
		}
		defer svc.Close()
		svcs = append(svcs, svc)
	}

	// Now send 50 requests.
	for i := 0; i < 50; i++ {
		_, err := nc.Request("svc.add", []byte(`{ "x": 22, "y": 11 }`), time.Second)
		if err != nil {
			t.Fatalf("Expected a response, got %v", err)
		}
	}

	for _, svc := range svcs {
		if svc.Name() != "CoolAddService" {
			t.Fatalf("Expected %q, got %q", "CoolAddService", svc.Name())
		}
		if len(svc.Description()) == 0 || len(svc.Version()) == 0 {
			t.Fatalf("Expected non emoty description and version")
		}
	}

	// Make sure we can request info, 1 response.
	// This could be exported as well as main service.
	info, err := nc.Request("svc.add.INFO", nil, time.Second)
	if err != nil {
		t.Fatalf("Expected a response, got %v", err)
	}
	fmt.Printf("\ninfo response:\n%s\n\n", info.Data)

	// Get stats for all the nodes. Multiple responses.
	// could do STATZ too?
	inbox := NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}
	if err := nc.PublishRequest("svc.add.PING", inbox, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for {
		resp, err := sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			break
		}
		fmt.Printf("Received ping response: %s\n", resp.Data)
	}
}
