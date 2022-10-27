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

package services

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestServiceBasics(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	// Stub service.
	doAdd := func(svc Service, req *nats.Msg) error {
		if rand.Intn(10) == 0 {
			return fmt.Errorf("Unexpected Error!")
		}
		// Happy Path.
		// Random delay between 5-10ms
		time.Sleep(5*time.Millisecond + time.Duration(rand.Intn(5))*time.Millisecond)
		if err := req.Respond([]byte("42")); err != nil {
			return err
		}
		return nil
	}

	var svcs []Service

	// Create 5 service responders.
	config := ServiceConfig{
		Name:        "CoolAddService",
		Version:     "v0.1",
		Description: "Add things together",
		Endpoint: Endpoint{
			Subject: "svc.add",
			Handler: doAdd,
		},
		Schema: ServiceSchema{Request: "", Response: ""},
	}

	for i := 0; i < 5; i++ {
		svc, err := Add(nc, config)
		if err != nil {
			t.Fatalf("Expected to create Service, got %v", err)
		}
		defer svc.Stop()
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
			t.Fatalf("Expected non empty description and version")
		}
	}

	// Make sure we can request info, 1 response.
	// This could be exported as well as main ServiceImpl.
	subj, err := SvcControlSubject(SrvInfo, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Failed to building info subject %v", err)
	}
	info, err := nc.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Expected a response, got %v", err)
	}
	var inf ServiceInfo
	if err := json.Unmarshal(info.Data, &inf); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if inf.Subject != "svc.add" {
		t.Fatalf("expected service subject to be srv.add: %s", inf.Subject)
	}

	// Ping all services. Multiple responses.
	// could do STATZ too?
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}
	pingSubject, err := SvcControlSubject(SrvPing, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := nc.PublishRequest(pingSubject, inbox, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	var pingCount int
	for {
		_, err := sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			break
		}
		pingCount++
	}
	if pingCount != 5 {
		t.Fatalf("Expected 5 ping responses, got: %d", pingCount)
	}

	// Get stats from all services
	statsInbox := nats.NewInbox()
	sub, err = nc.SubscribeSync(statsInbox)
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}
	statsSubject, err := SvcControlSubject(SrvStatus, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := nc.PublishRequest(statsSubject, statsInbox, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	stats := make([]ServiceStats, 0)
	var requestsNum int
	for {
		resp, err := sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			break
		}
		var srvStats ServiceStats
		if err := json.Unmarshal(resp.Data, &srvStats); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if len(srvStats.Endpoints) != 10 {
			t.Fatalf("Expected 10 endpoints on a serivce, got: %d", len(srvStats.Endpoints))
		}
		for _, e := range srvStats.Endpoints {
			if e.Name == "CoolAddService" {
				requestsNum += e.NumRequests
			}
		}
		stats = append(stats, srvStats)
	}
	if len(stats) != 5 {
		t.Fatalf("Expected stats for 5 services, got: %d", len(stats))
	}

	// Services should process 50 requests total
	if requestsNum != 50 {
		t.Fatalf("Expected a total fo 50 requests processed, got: %d", requestsNum)
	}
}

func TestServiceErrors(t *testing.T) {
	tests := []struct {
		name            string
		handlerResponse error
		expectedStatus  string
	}{
		{
			name:            "generic error",
			handlerResponse: fmt.Errorf("oops"),
			expectedStatus:  "500 oops",
		},
		{
			name:            "api error",
			handlerResponse: &ServiceAPIError{ErrorCode: 400, Description: "oops"},
			expectedStatus:  "400 oops",
		},
		{
			name:            "no error",
			handlerResponse: nil,
			expectedStatus:  "",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunServerOnPort(-1)
			defer s.Shutdown()

			nc, err := nats.Connect(s.ClientURL())
			if err != nil {
				t.Fatalf("Expected to connect to server, got %v", err)
			}
			defer nc.Close()

			// Stub service.
			handler := func(svc Service, req *nats.Msg) error {
				if test.handlerResponse == nil {
					if err := req.Respond([]byte("ok")); err != nil {
						return err
					}
				}
				return test.handlerResponse
			}

			svc, err := Add(nc, ServiceConfig{
				Name:        "CoolService",
				Description: "Erroring service",
				Endpoint: Endpoint{
					Subject: "svc.fail",
					Handler: handler,
				},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer svc.Stop()

			resp, err := nc.Request("svc.fail", nil, 1*time.Second)
			if err != nil {
				t.Fatalf("request error")
			}

			status := resp.Header.Get("Nats-Service-Error")
			if status != test.expectedStatus {
				t.Fatalf("Invalid response status; want: %q; got: %q", test.expectedStatus, status)
			}
		})
	}
}

func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	return RunServerWithOptions(&opts)
}

func RunServerWithOptions(opts *server.Options) *server.Server {
	return natsserver.RunServer(opts)
}
