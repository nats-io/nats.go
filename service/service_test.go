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

package service

import (
	"bytes"
	"encoding/json"
	"errors"
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
	doAdd := func(req *Request) {
		if rand.Intn(10) == 0 {
			if err := req.Error("500", "Unexpected error!"); err != nil {
				t.Fatalf("Unexpected error when sending error response: %v", err)
			}
			return
		}
		// Happy Path.
		// Random delay between 5-10ms
		time.Sleep(5*time.Millisecond + time.Duration(rand.Intn(5))*time.Millisecond)
		if err := req.Respond([]byte("42")); err != nil {
			if err := req.Error("500", "Unexpected error!"); err != nil {
				t.Fatalf("Unexpected error when sending error response: %v", err)
			}
			return
		}
	}

	var svcs []*Service

	// Create 5 service responders.
	config := Config{
		Name:        "CoolAddService",
		Version:     "v0.1",
		Description: "Add things together",
		Endpoint: Endpoint{
			Subject: "svc.add",
			Handler: doAdd,
		},
		Schema: Schema{Request: "", Response: ""},
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
		if svc.Name != "CoolAddService" {
			t.Fatalf("Expected %q, got %q", "CoolAddService", svc.Name)
		}
		if len(svc.Description) == 0 || len(svc.Version) == 0 {
			t.Fatalf("Expected non empty description and version")
		}
	}

	// Make sure we can request info, 1 response.
	// This could be exported as well as main ServiceImpl.
	subj, err := ControlSubject(InfoVerb, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Failed to building info subject %v", err)
	}
	info, err := nc.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Expected a response, got %v", err)
	}
	var inf Info
	if err := json.Unmarshal(info.Data, &inf); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if inf.Subject != "svc.add" {
		t.Fatalf("expected service subject to be srv.add: %s", inf.Subject)
	}

	// Ping all services. Multiple responses.
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}
	pingSubject, err := ControlSubject(PingVerb, "CoolAddService", "")
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
	statsSubject, err := ControlSubject(StatsVerb, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := nc.PublishRequest(statsSubject, statsInbox, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	stats := make([]Stats, 0)
	var requestsNum int
	for {
		resp, err := sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			break
		}
		var srvStats Stats
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
	// Reset stats for a service
	svcs[0].Reset()
	for _, e := range svcs[0].Stats().Endpoints {
		emptyStats := EndpointStats{Name: e.Name}
		if e != emptyStats {
			t.Fatalf("Expected empty stats after reset; got: %+v", e)
		}
	}
}

func TestAddService(t *testing.T) {
	testHandler := func(*Request) {}
	errNats := make(chan struct{})
	errService := make(chan struct{})
	closedNats := make(chan struct{})
	doneService := make(chan struct{})

	tests := []struct {
		name              string
		givenConfig       Config
		natsClosedHandler nats.ConnHandler
		natsErrorHandler  nats.ErrHandler
		expectedPing      Ping
		withError         error
	}{
		{
			name: "minimal config",
			givenConfig: Config{
				Name: "test_service",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
			},
			expectedPing: Ping{
				Name: "test_service",
			},
		},
		{
			name: "with done handler, no handlers on nats connection",
			givenConfig: Config{
				Name: "test_service",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				DoneHandler: func(*Service) {
					doneService <- struct{}{}
				},
			},
			expectedPing: Ping{
				Name: "test_service",
			},
		},
		{
			name: "with error handler, no handlers on nats connection",
			givenConfig: Config{
				Name: "test_service",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				ErrorHandler: func(*Service, *NATSError) {
					errService <- struct{}{}
				},
			},
			expectedPing: Ping{
				Name: "test_service",
			},
		},
		{
			name: "with done handler, append to nats handlers",
			givenConfig: Config{
				Name: "test_service",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				DoneHandler: func(*Service) {
					doneService <- struct{}{}
				},
			},
			natsClosedHandler: func(c *nats.Conn) {
				closedNats <- struct{}{}
			},
			natsErrorHandler: func(*nats.Conn, *nats.Subscription, error) {
				errNats <- struct{}{}
			},
			expectedPing: Ping{
				Name: "test_service",
			},
		},
		{
			name: "with error handler, append to nats handlers",
			givenConfig: Config{
				Name: "test_service",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				DoneHandler: func(*Service) {
					doneService <- struct{}{}
				},
			},
			natsClosedHandler: func(c *nats.Conn) {
				closedNats <- struct{}{}
			},
			natsErrorHandler: func(*nats.Conn, *nats.Subscription, error) {
				errNats <- struct{}{}
			},
			expectedPing: Ping{
				Name: "test_service",
			},
		},
		{
			name: "validation error, invalid service name",
			givenConfig: Config{
				Name: "test_service!",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
			},
			withError: ErrConfigValidation,
		},
		{
			name: "validation error, empty subject",
			givenConfig: Config{
				Name: "test_service",
				Endpoint: Endpoint{
					Subject: "",
					Handler: testHandler,
				},
			},
			withError: ErrConfigValidation,
		},
		{
			name: "validation error, no handler",
			givenConfig: Config{
				Name: "test_service",
				Endpoint: Endpoint{
					Subject: "test_subject",
					Handler: nil,
				},
			},
			withError: ErrConfigValidation,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			s := RunServerOnPort(-1)
			defer s.Shutdown()

			nc, err := nats.Connect(s.ClientURL(),
				nats.ErrorHandler(test.natsErrorHandler),
				nats.ClosedHandler(test.natsClosedHandler),
			)
			if err != nil {
				t.Fatalf("Expected to connect to server, got %v", err)
			}
			defer nc.Close()

			srv, err := Add(nc, test.givenConfig)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}

			pingSubject, err := ControlSubject(PingVerb, srv.Name, srv.ID())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			pingResp, err := nc.Request(pingSubject, nil, 1*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var ping Ping
			if err := json.Unmarshal(pingResp.Data, &ping); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			test.expectedPing.ID = srv.ID()
			if test.expectedPing != ping {
				t.Fatalf("Invalid ping response; want: %+v; got: %+v", test.expectedPing, ping)
			}

			if test.givenConfig.DoneHandler != nil {
				go nc.Opts.ClosedCB(nc)
				select {
				case <-doneService:
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout on DoneHandler")
				}
				if test.natsClosedHandler != nil {
					select {
					case <-closedNats:
					case <-time.After(1 * time.Second):
						t.Fatalf("Timeout on ClosedHandler")
					}
				}
			}

			if test.givenConfig.ErrorHandler != nil {
				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: "test.sub"}, fmt.Errorf("oops"))
				select {
				case <-errService:
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout on ErrorHandler")
				}
				if test.natsErrorHandler != nil {
					select {
					case <-errNats:
					case <-time.After(1 * time.Second):
						t.Fatalf("Timeout on AsyncErrHandler")
					}
				}
			}

			if err := srv.Stop(); err != nil {
				t.Fatalf("Unexpected error when stopping the service: %v", err)
			}
			if test.natsClosedHandler != nil {
				go nc.Opts.ClosedCB(nc)
				select {
				case <-doneService:
					t.Fatalf("Expected to restore nats closed handler")
				case <-time.After(50 * time.Millisecond):
				}
				select {
				case <-closedNats:
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout on ClosedHandler")
				}
			}
			if test.natsErrorHandler != nil {
				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: "test.sub"}, fmt.Errorf("oops"))
				select {
				case <-errService:
					t.Fatalf("Expected to restore nats error handler")
				case <-time.After(50 * time.Millisecond):
				}
				select {
				case <-errNats:
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout on AsyncErrHandler")
				}
			}
		})
	}
}

func TestRequestRespond(t *testing.T) {
	type x struct {
		A string `json:"a"`
		B int    `json:"b"`
	}

	tests := []struct {
		name             string
		respondData      interface{}
		errDescription   string
		errCode          string
		expectedMessage  string
		expectedCode     string
		expectedResponse []byte
		withRespondError error
	}{
		{
			name:             "byte response",
			respondData:      []byte("OK"),
			expectedResponse: []byte("OK"),
		},
		{
			name:             "byte response, connection closed",
			respondData:      []byte("OK"),
			withRespondError: ErrRespond,
		},
		{
			name:             "struct response",
			respondData:      x{"abc", 5},
			expectedResponse: []byte(`{"a":"abc","b":5}`),
		},
		{
			name:             "invalid response data",
			respondData:      func() {},
			withRespondError: ErrMarshalResponse,
		},
		{
			name:            "generic error",
			errDescription:  "oops",
			errCode:         "500",
			expectedMessage: "oops",
			expectedCode:    "500",
		},
		{
			name:             "missing error code",
			errDescription:   "oops",
			withRespondError: ErrArgRequired,
		},
		{
			name:             "missing error description",
			errCode:          "500",
			withRespondError: ErrArgRequired,
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

			respData := test.respondData
			respError := test.withRespondError
			errCode := test.errCode
			errDesc := test.errDescription
			// Stub service.
			handler := func(req *Request) {
				if errors.Is(test.withRespondError, ErrRespond) {
					nc.Close()
				}
				if errCode == "" && errDesc == "" {
					if resp, ok := respData.([]byte); ok {
						err := req.Respond(resp)
						if respError != nil {
							if !errors.Is(err, respError) {
								t.Fatalf("Expected error: %v; got: %v", respError, err)
							}
							return
						}
						if err != nil {
							t.Fatalf("Unexpected error when sending response: %v", err)
						}
					} else {
						err := req.RespondJSON(respData)
						if respError != nil {
							if !errors.Is(err, respError) {
								t.Fatalf("Expected error: %v; got: %v", respError, err)
							}
							return
						}
						if err != nil {
							t.Fatalf("Unexpected error when sending response: %v", err)
						}
					}
					return
				}

				err := req.Error(errCode, errDesc)
				if respError != nil {
					if !errors.Is(err, respError) {
						t.Fatalf("Expected error: %v; got: %v", respError, err)
					}
					return
				}
				if err != nil {
					t.Fatalf("Unexpected error when sending response: %v", err)
				}
			}

			svc, err := Add(nc, Config{
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

			resp, err := nc.Request("svc.fail", nil, 50*time.Millisecond)
			if test.withRespondError != nil {
				return
			}
			if err != nil {
				t.Fatalf("request error: %v", err)
			}

			if test.errCode != "" {
				description := resp.Header.Get("Nats-Service-Error")
				if description != test.expectedMessage {
					t.Fatalf("Invalid response message; want: %q; got: %q", test.expectedMessage, description)
				}
				code := resp.Header.Get("Nats-Service-Error-Code")
				if code != test.expectedCode {
					t.Fatalf("Invalid response code; want: %q; got: %q", test.expectedCode, code)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !bytes.Equal(bytes.TrimSpace(resp.Data), bytes.TrimSpace(test.expectedResponse)) {
				t.Fatalf("Invalid response; want: %s; got: %s", string(test.expectedResponse), string(resp.Data))
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
