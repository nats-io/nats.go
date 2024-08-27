// Copyright 2022-2023 The NATS Authors
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

package micro_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"

	natsserver "github.com/nats-io/nats-server/v2/test"
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
	doAdd := func(req micro.Request) {
		if rand.Intn(10) == 0 {
			if err := req.Error("500", "Unexpected error!", nil); err != nil {
				t.Fatalf("Unexpected error when sending error response: %v", err)
			}
			return
		}
		// Happy Path.
		// Random delay between 5-10ms
		time.Sleep(5*time.Millisecond + time.Duration(rand.Intn(5))*time.Millisecond)
		if err := req.Respond([]byte("42")); err != nil {
			if err := req.Error("500", "Unexpected error!", nil); err != nil {
				t.Fatalf("Unexpected error when sending error response: %v", err)
			}
			return
		}
	}

	var svcs []micro.Service

	// Create 5 service responders.
	config := micro.Config{
		Name:        "CoolAddService",
		Version:     "0.1.0",
		Description: "Add things together",
		Metadata:    map[string]string{"basic": "metadata"},
		Endpoint: &micro.EndpointConfig{
			Subject: "svc.add",
			Handler: micro.HandlerFunc(doAdd),
		},
	}

	for i := 0; i < 5; i++ {
		svc, err := micro.AddService(nc, config)
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
		info := svc.Info()
		if info.Name != "CoolAddService" {
			t.Fatalf("Expected %q, got %q", "CoolAddService", info.Name)
		}
		if len(info.Description) == 0 || len(info.Version) == 0 {
			t.Fatalf("Expected non empty description and version")
		}
		if !reflect.DeepEqual(info.Metadata, map[string]string{"basic": "metadata"}) {
			t.Fatalf("invalid metadata: %v", info.Metadata)
		}
	}

	// Make sure we can request info, 1 response.
	// This could be exported as well as main ServiceImpl.
	subj, err := micro.ControlSubject(micro.InfoVerb, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Failed to building info subject %v", err)
	}
	info, err := nc.Request(subj, nil, time.Second)
	if err != nil {
		t.Fatalf("Expected a response, got %v", err)
	}
	var inf micro.Info
	if err := json.Unmarshal(info.Data, &inf); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// Ping all services. Multiple responses.
	inbox := nats.NewInbox()
	sub, err := nc.SubscribeSync(inbox)
	if err != nil {
		t.Fatalf("subscribe failed: %s", err)
	}
	pingSubject, err := micro.ControlSubject(micro.PingVerb, "CoolAddService", "")
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
	statsSubject, err := micro.ControlSubject(micro.StatsVerb, "CoolAddService", "")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := nc.PublishRequest(statsSubject, statsInbox, nil); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	stats := make([]micro.Stats, 0)
	var requestsNum int
	for {
		resp, err := sub.NextMsg(250 * time.Millisecond)
		if err != nil {
			break
		}
		var srvStats micro.Stats
		if err := json.Unmarshal(resp.Data, &srvStats); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		requestsNum += srvStats.Endpoints[0].NumRequests
		stats = append(stats, srvStats)
	}
	if len(stats) != 5 {
		t.Fatalf("Expected stats for 5 services, got: %d", len(stats))
	}

	// Services should process 50 requests total
	if requestsNum != 50 {
		t.Fatalf("Expected a total of 50 requests processed, got: %d", requestsNum)
	}
	// Reset stats for a service
	svcs[0].Reset()

	if svcs[0].Stats().Endpoints[0].NumRequests != 0 {
		t.Fatalf("Expected empty stats after reset; got: %+v", svcs[0].Stats())
	}
}

func TestAddService(t *testing.T) {
	testHandler := func(micro.Request) {}
	errNats := make(chan struct{})
	errService := make(chan struct{})
	closedNats := make(chan struct{})
	doneService := make(chan struct{})

	tests := []struct {
		name              string
		givenConfig       micro.Config
		endpoints         []string
		natsClosedHandler nats.ConnHandler
		natsErrorHandler  nats.ErrHandler
		asyncErrorSubject string
		expectedPing      micro.Ping
		withError         error
	}{
		{
			name: "minimal config",
			givenConfig: micro.Config{
				Name:     "test_service",
				Version:  "0.1.0",
				Metadata: map[string]string{"basic": "metadata"},
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{"basic": "metadata"},
				},
			},
		},
		{
			name: "with single base endpoint",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: &micro.EndpointConfig{
					Subject:  "test",
					Handler:  micro.HandlerFunc(testHandler),
					Metadata: map[string]string{"basic": "endpoint_metadata"},
				},
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{},
				},
			},
		},
		{
			name: "with base endpoint and additional endpoints",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: &micro.EndpointConfig{
					Subject: "test",
					Handler: micro.HandlerFunc(testHandler),
				},
			},
			endpoints: []string{"func1", "func2", "func3"},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{},
				},
			},
		},
		{
			name: "with done handler, no handlers on nats connection",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				DoneHandler: func(micro.Service) {
					doneService <- struct{}{}
				},
			},
			endpoints: []string{"func"},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{},
				},
			},
		},
		{
			name: "with error handler, no handlers on nats connection",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				ErrorHandler: func(micro.Service, *micro.NATSError) {
					errService <- struct{}{}
				},
			},
			endpoints: []string{"func"},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{},
				},
			},
			asyncErrorSubject: "func",
		},
		{
			name: "with error handler, no handlers on nats connection, error on monitoring subject",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				ErrorHandler: func(micro.Service, *micro.NATSError) {
					errService <- struct{}{}
				},
			},
			endpoints: []string{"func"},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{},
				},
			},
			asyncErrorSubject: "$SRV.PING.test_service",
		},
		{
			name: "with done handler, append to nats handlers",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				DoneHandler: func(micro.Service) {
					doneService <- struct{}{}
				},
			},
			endpoints: []string{"func"},
			natsClosedHandler: func(c *nats.Conn) {
				closedNats <- struct{}{}
			},
			natsErrorHandler: func(*nats.Conn, *nats.Subscription, error) {
				errNats <- struct{}{}
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{},
				},
			},
			asyncErrorSubject: "test.sub",
		},
		{
			name: "with error handler, append to nats handlers",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				DoneHandler: func(micro.Service) {
					doneService <- struct{}{}
				},
			},
			endpoints: []string{"func"},
			natsClosedHandler: func(c *nats.Conn) {
				closedNats <- struct{}{}
			},
			natsErrorHandler: func(*nats.Conn, *nats.Subscription, error) {
				errNats <- struct{}{}
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{},
				},
			},
		},
		{
			name: "with error handler, append to nats handlers, error on monitoring subject",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				DoneHandler: func(micro.Service) {
					doneService <- struct{}{}
				},
			},
			endpoints: []string{"func"},
			natsClosedHandler: func(c *nats.Conn) {
				closedNats <- struct{}{}
			},
			natsErrorHandler: func(*nats.Conn, *nats.Subscription, error) {
				errNats <- struct{}{}
			},
			expectedPing: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					Metadata: map[string]string{},
				},
			},
			asyncErrorSubject: "$SRV.PING.TEST_SERVICE",
		},
		{
			name: "validation error, invalid service name",
			givenConfig: micro.Config{
				Name:    "test_service!",
				Version: "0.1.0",
			},
			endpoints: []string{"func"},
			withError: micro.ErrConfigValidation,
		},
		{
			name: "validation error, invalid version",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "abc",
			},
			endpoints: []string{"func"},
			withError: micro.ErrConfigValidation,
		},
		{
			name: "validation error, invalid endpoint subject",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.0.1",
				Endpoint: &micro.EndpointConfig{
					Subject: "endpoint subject",
				},
			},
			withError: micro.ErrConfigValidation,
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

			srv, err := micro.AddService(nc, test.givenConfig)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for _, endpoint := range test.endpoints {
				if err := srv.AddEndpoint(endpoint, micro.HandlerFunc(testHandler)); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}

			info := srv.Info()
			subjectsNum := len(test.endpoints)
			if test.givenConfig.Endpoint != nil {
				subjectsNum += 1
			}
			if subjectsNum != len(info.Endpoints) {
				t.Fatalf("Invalid number of registered endpoints; want: %d; got: %d", subjectsNum, len(info.Endpoints))
			}
			pingSubject, err := micro.ControlSubject(micro.PingVerb, info.Name, info.ID)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			pingResp, err := nc.Request(pingSubject, nil, 1*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			var ping micro.Ping
			if err := json.Unmarshal(pingResp.Data, &ping); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			test.expectedPing.ID = info.ID
			if !reflect.DeepEqual(test.expectedPing, ping) {
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
				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: test.asyncErrorSubject}, errors.New("oops"))
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
				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: test.asyncErrorSubject}, errors.New("oops"))
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

func TestErrHandlerSubjectMatch(t *testing.T) {
	tests := []struct {
		name             string
		endpointSubject  string
		errSubject       string
		expectServiceErr bool
	}{
		{
			name:             "exact match",
			endpointSubject:  "foo.bar.baz",
			errSubject:       "foo.bar.baz",
			expectServiceErr: true,
		},
		{
			name:             "match with *",
			endpointSubject:  "foo.*.baz",
			errSubject:       "foo.bar.baz",
			expectServiceErr: true,
		},
		{
			name:             "match with >",
			endpointSubject:  "foo.bar.>",
			errSubject:       "foo.bar.baz.1",
			expectServiceErr: true,
		},
		{
			name:             "monitoring handler",
			endpointSubject:  "foo.bar.>",
			errSubject:       "$SRV.PING",
			expectServiceErr: true,
		},
		{
			name:             "endpoint longer than subject",
			endpointSubject:  "foo.bar.baz",
			errSubject:       "foo.bar",
			expectServiceErr: false,
		},
		{
			name:             "no match",
			endpointSubject:  "foo.bar.baz",
			errSubject:       "foo.baz.bar",
			expectServiceErr: false,
		},
		{
			name:             "no match with *",
			endpointSubject:  "foo.*.baz",
			errSubject:       "foo.bar.foo",
			expectServiceErr: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			coreNatsAsyncErrors := []nats.ErrHandler{nil, func(c *nats.Conn, s *nats.Subscription, err error) {}}
			for _, cb := range coreNatsAsyncErrors {
				errChan := make(chan struct{})
				errHandler := func(s micro.Service, err *micro.NATSError) {
					errChan <- struct{}{}
				}
				s := RunServerOnPort(-1)
				defer s.Shutdown()

				nc, err := nats.Connect(s.ClientURL())
				if err != nil {
					t.Fatalf("Expected to connect to server, got %v", err)
				}
				defer nc.Close()
				nc.SetErrorHandler(cb)
				svc, err := micro.AddService(nc, micro.Config{
					Name:         "test_service",
					Version:      "0.0.1",
					ErrorHandler: micro.ErrHandler(errHandler),
					Endpoint: &micro.EndpointConfig{
						Subject: test.endpointSubject,
						Handler: micro.HandlerFunc(func(r micro.Request) {}),
					},
				})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				defer svc.Stop()

				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: test.errSubject}, errors.New("oops"))
				if test.expectServiceErr {
					select {
					case <-errChan:
					case <-time.After(10 * time.Millisecond):
						t.Fatalf("Expected service error callback")
					}
				} else {
					select {
					case <-errChan:
						t.Fatalf("Expected no service error callback")
					case <-time.After(10 * time.Millisecond):
					}
				}
			}
		})
	}
}

func TestGroups(t *testing.T) {
	tests := []struct {
		name             string
		endpointName     string
		groups           []string
		expectedEndpoint micro.EndpointInfo
	}{
		{
			name:         "no groups",
			endpointName: "foo",
			expectedEndpoint: micro.EndpointInfo{
				Name:       "foo",
				Subject:    "foo",
				QueueGroup: "q",
			},
		},
		{
			name:         "single group",
			endpointName: "foo",
			groups:       []string{"g1"},
			expectedEndpoint: micro.EndpointInfo{
				Name:       "foo",
				Subject:    "g1.foo",
				QueueGroup: "q",
			},
		},
		{
			name:         "single empty group",
			endpointName: "foo",
			groups:       []string{""},
			expectedEndpoint: micro.EndpointInfo{
				Name:       "foo",
				Subject:    "foo",
				QueueGroup: "q",
			},
		},
		{
			name:         "empty groups",
			endpointName: "foo",
			groups:       []string{"", "g1", ""},
			expectedEndpoint: micro.EndpointInfo{
				Name:       "foo",
				Subject:    "g1.foo",
				QueueGroup: "q",
			},
		},
		{
			name:         "multiple groups",
			endpointName: "foo",
			groups:       []string{"g1", "g2", "g3"},
			expectedEndpoint: micro.EndpointInfo{
				Name:       "foo",
				Subject:    "g1.g2.g3.foo",
				QueueGroup: "q",
			},
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

			srv, err := micro.AddService(nc, micro.Config{
				Name:    "test_service",
				Version: "0.0.1",
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer srv.Stop()

			if len(test.groups) > 0 {
				group := srv.AddGroup(test.groups[0])
				for _, g := range test.groups[1:] {
					group = group.AddGroup(g)
				}
				err = group.AddEndpoint(test.endpointName, micro.HandlerFunc(func(r micro.Request) {}))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			} else {
				err = srv.AddEndpoint(test.endpointName, micro.HandlerFunc(func(r micro.Request) {}))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}

			info := srv.Info()
			if len(info.Endpoints) != 1 {
				t.Fatalf("Expected 1 registered endpoint; got: %d", len(info.Endpoints))
			}
			if !reflect.DeepEqual(info.Endpoints[0], test.expectedEndpoint) {
				t.Fatalf("Invalid endpoint; want: %s, got: %s", test.expectedEndpoint, info.Endpoints[0])
			}
		})
	}
}

func TestMonitoringHandlers(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	asyncErr := make(chan struct{})
	errHandler := func(s micro.Service, n *micro.NATSError) {
		asyncErr <- struct{}{}
	}

	config := micro.Config{
		Name:         "test_service",
		Version:      "0.1.0",
		ErrorHandler: errHandler,
		Endpoint: &micro.EndpointConfig{
			Subject:  "test.func",
			Handler:  micro.HandlerFunc(func(r micro.Request) {}),
			Metadata: map[string]string{"basic": "schema"},
		},
	}
	srv, err := micro.AddService(nc, config)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer func() {
		srv.Stop()
		if !srv.Stopped() {
			t.Fatalf("Expected service to be stopped")
		}
	}()

	info := srv.Info()

	tests := []struct {
		name             string
		subject          string
		withError        bool
		expectedResponse any
	}{
		{
			name:    "PING all",
			subject: "$SRV.PING",
			expectedResponse: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
			},
		},
		{
			name:    "PING name",
			subject: "$SRV.PING.test_service",
			expectedResponse: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
			},
		},
		{
			name:    "PING ID",
			subject: fmt.Sprintf("$SRV.PING.test_service.%s", info.ID),
			expectedResponse: micro.Ping{
				Type: micro.PingResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
			},
		},
		{
			name:    "INFO all",
			subject: "$SRV.INFO",
			expectedResponse: micro.Info{
				Type: micro.InfoResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
				Endpoints: []micro.EndpointInfo{
					{
						Name:       "default",
						Subject:    "test.func",
						QueueGroup: "q",
						Metadata:   map[string]string{"basic": "schema"},
					},
				},
			},
		},
		{
			name:    "INFO name",
			subject: "$SRV.INFO.test_service",
			expectedResponse: micro.Info{
				Type: micro.InfoResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
				Endpoints: []micro.EndpointInfo{
					{
						Name:       "default",
						Subject:    "test.func",
						QueueGroup: "q",
						Metadata:   map[string]string{"basic": "schema"},
					},
				},
			},
		},
		{
			name:    "INFO ID",
			subject: fmt.Sprintf("$SRV.INFO.test_service.%s", info.ID),
			expectedResponse: micro.Info{
				Type: micro.InfoResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
				Endpoints: []micro.EndpointInfo{
					{
						Name:       "default",
						Subject:    "test.func",
						QueueGroup: "q",
						Metadata:   map[string]string{"basic": "schema"},
					},
				},
			},
		},
		{
			name:      "PING error",
			subject:   "$SRV.PING",
			withError: true,
		},
		{
			name:      "INFO error",
			subject:   "$SRV.INFO",
			withError: true,
		},
		{
			name:      "STATS error",
			subject:   "$SRV.STATS",
			withError: true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.withError {
				// use publish instead of request, so Respond will fail inside the handler
				if err := nc.Publish(test.subject, nil); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				select {
				case <-asyncErr:
					return
				case <-time.After(1 * time.Second):
					t.Fatalf("Timeout waiting for async error")
				}
				return
			}

			resp, err := nc.Request(test.subject, nil, 1*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			respMap := make(map[string]any)
			if err := json.Unmarshal(resp.Data, &respMap); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectedResponseJSON, err := json.Marshal(test.expectedResponse)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectedRespMap := make(map[string]any)
			if err := json.Unmarshal(expectedResponseJSON, &expectedRespMap); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !reflect.DeepEqual(respMap, expectedRespMap) {
				t.Fatalf("Invalid response; want: %+v; got: %+v", expectedRespMap, respMap)
			}
		})
	}
}

func TestContextHandler(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	type key string
	ctx = context.WithValue(ctx, key("key"), []byte("val"))

	handler := func(ctx context.Context, req micro.Request) {
		select {
		case <-ctx.Done():
			req.Error("400", "context canceled", nil)
		default:
			v := ctx.Value(key("key"))
			req.Respond(v.([]byte))
		}
	}
	config := micro.Config{
		Name:    "test_service",
		Version: "0.1.0",
		Endpoint: &micro.EndpointConfig{
			Subject: "test.func",
			Handler: micro.ContextHandler(ctx, handler),
		},
	}

	srv, err := micro.AddService(nc, config)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer srv.Stop()

	resp, err := nc.Request("test.func", nil, 1*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if string(resp.Data) != "val" {
		t.Fatalf("Invalid response; want: %q; got: %q", "val", string(resp.Data))
	}
	cancel()
	resp, err = nc.Request("test.func", nil, 1*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %s", err)
	}
	if resp.Header.Get(micro.ErrorCodeHeader) != "400" {
		t.Fatalf("Expected error response after canceling context; got: %q", string(resp.Data))
	}
}

func TestAddEndpoint_Concurrency(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	ctx := context.Background()

	handler := func(ctx context.Context, req micro.Request) {
		req.RespondJSON(map[string]any{"hello": "world"})
	}
	config := micro.Config{
		Name:    "test_service",
		Version: "0.1.0",
	}

	srv, err := micro.AddService(nc, config)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer srv.Stop()

	res := make(chan error, 10)
	wg := sync.WaitGroup{}
	wg.Add(10)

	// now add a few endpoints concurrently
	// and make sure they are added successfully
	// and there is no race
	for i := 0; i < 10; i++ {
		go func(i int) {
			wg.Wait()
			res <- srv.AddEndpoint(fmt.Sprintf("test%d", i), micro.ContextHandler(ctx, handler))
		}(i)
		// after all goroutines are started, release the lock
	}
	wg.Add(-10)

	for i := 0; i < 10; i++ {
		select {
		case err := <-res:
			if err != nil {
				t.Fatalf("Unexpected error: %s", err)
			}
		case <-time.After(1 * time.Second):
			t.Fatalf("Timeout waiting for endpoint to be added")
		}
	}

	if len(srv.Info().Endpoints) != 10 {
		t.Fatalf("Expected 11 endpoints, got: %d", len(srv.Info().Endpoints))
	}

}

func TestServiceStats(t *testing.T) {
	handler := func(r micro.Request) {
		r.Respond([]byte("ok"))
	}
	tests := []struct {
		name          string
		config        micro.Config
		expectedStats map[string]any
	}{
		{
			name: "stats handler",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
			},
		},
		{
			name: "with stats handler",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				StatsHandler: func(e *micro.Endpoint) any {
					return map[string]any{
						"key": "val",
					}
				},
			},
			expectedStats: map[string]any{
				"key": "val",
			},
		},
		{
			name: "with default endpoint",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: &micro.EndpointConfig{
					Subject:  "test.func",
					Handler:  micro.HandlerFunc(handler),
					Metadata: map[string]string{"test": "value"},
				},
			},
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

			srv, err := micro.AddService(nc, test.config)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if test.config.Endpoint == nil {
				opts := []micro.EndpointOpt{micro.WithEndpointSubject("test.func")}
				if err := srv.AddEndpoint("func", micro.HandlerFunc(handler), opts...); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			defer srv.Stop()
			for i := 0; i < 10; i++ {
				if _, err := nc.Request("test.func", []byte("msg"), time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}

			// Malformed request, missing reply subjtct
			// This should be reflected in errors
			if err := nc.Publish("test.func", []byte("err")); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			time.Sleep(10 * time.Millisecond)

			info := srv.Info()
			resp, err := nc.Request(fmt.Sprintf("$SRV.STATS.test_service.%s", info.ID), nil, 1*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var stats micro.Stats
			if err := json.Unmarshal(resp.Data, &stats); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if len(stats.Endpoints) != 1 {
				t.Fatalf("Unexpected number of endpoints: want: %d; got: %d", 1, len(stats.Endpoints))
			}
			if stats.Name != info.Name {
				t.Errorf("Unexpected service name; want: %s; got: %s", info.Name, stats.Name)
			}
			if stats.ID != info.ID {
				t.Errorf("Unexpected service name; want: %s; got: %s", info.ID, stats.ID)
			}
			if test.config.Endpoint == nil && stats.Endpoints[0].Name != "func" {
				t.Errorf("Invalid endpoint name; want: %s; got: %s", "func", stats.Endpoints[0].Name)
			}
			if test.config.Endpoint != nil && stats.Endpoints[0].Name != "default" {
				t.Errorf("Invalid endpoint name; want: %s; got: %s", "default", stats.Endpoints[0].Name)
			}
			if stats.Endpoints[0].Subject != "test.func" {
				t.Errorf("Invalid endpoint subject; want: %s; got: %s", "test.func", stats.Endpoints[0].Subject)
			}
			if stats.Endpoints[0].NumRequests != 11 {
				t.Errorf("Unexpected num_requests; want: 11; got: %d", stats.Endpoints[0].NumRequests)
			}
			if stats.Endpoints[0].NumErrors != 1 {
				t.Errorf("Unexpected num_errors; want: 1; got: %d", stats.Endpoints[0].NumErrors)
			}
			if stats.Endpoints[0].AverageProcessingTime == 0 {
				t.Errorf("Expected non-empty AverageProcessingTime")
			}
			if stats.Endpoints[0].ProcessingTime == 0 {
				t.Errorf("Expected non-empty ProcessingTime")
			}
			if stats.Started.IsZero() {
				t.Errorf("Expected non-empty start time")
			}
			if stats.Type != micro.StatsResponseType {
				t.Errorf("Invalid response type; want: %s; got: %s", micro.StatsResponseType, stats.Type)
			}

			if test.expectedStats != nil {
				var data map[string]any
				if err := json.Unmarshal(stats.Endpoints[0].Data, &data); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if !reflect.DeepEqual(data, test.expectedStats) {
					t.Fatalf("Invalid data from stats handler; want: %v; got: %v", test.expectedStats, data)
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
		respondData      any
		respondHeaders   micro.Headers
		errDescription   string
		errCode          string
		errData          []byte
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
			name:             "byte response, with headers",
			respondHeaders:   micro.Headers{"key": []string{"value"}},
			respondData:      []byte("OK"),
			expectedResponse: []byte("OK"),
		},
		{
			name:             "byte response, connection closed",
			respondData:      []byte("OK"),
			withRespondError: micro.ErrRespond,
		},
		{
			name:             "struct response",
			respondData:      x{"abc", 5},
			expectedResponse: []byte(`{"a":"abc","b":5}`),
		},
		{
			name:             "invalid response data",
			respondData:      func() {},
			withRespondError: micro.ErrMarshalResponse,
		},
		{
			name:            "generic error",
			errDescription:  "oops",
			errCode:         "500",
			errData:         []byte("error!"),
			expectedMessage: "oops",
			expectedCode:    "500",
		},
		{
			name:            "generic error, with headers",
			respondHeaders:  micro.Headers{"key": []string{"value"}},
			errDescription:  "oops",
			errCode:         "500",
			errData:         []byte("error!"),
			expectedMessage: "oops",
			expectedCode:    "500",
		},
		{
			name:            "error without response payload",
			errDescription:  "oops",
			errCode:         "500",
			expectedMessage: "oops",
			expectedCode:    "500",
		},
		{
			name:             "missing error code",
			errDescription:   "oops",
			withRespondError: micro.ErrArgRequired,
		},
		{
			name:             "missing error description",
			errCode:          "500",
			withRespondError: micro.ErrArgRequired,
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
			errData := test.errData
			handler := func(req micro.Request) {
				if errors.Is(test.withRespondError, micro.ErrRespond) {
					nc.Close()
					return
				}
				if val := req.Headers().Get("key"); val != "value" {
					t.Fatalf("Expected headers in the request")
				}
				if !bytes.Equal(req.Data(), []byte("req")) {
					t.Fatalf("Invalid request data; want: %q; got: %q", "req", req.Data())
				}
				if errCode == "" && errDesc == "" {
					if resp, ok := respData.([]byte); ok {
						err := req.Respond(resp, micro.WithHeaders(test.respondHeaders))
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
						err := req.RespondJSON(respData, micro.WithHeaders(test.respondHeaders))
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

				err := req.Error(errCode, errDesc, errData, micro.WithHeaders(test.respondHeaders))
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

			svc, err := micro.AddService(nc, micro.Config{
				Name:        "CoolService",
				Version:     "0.1.0",
				Description: "test service",
				Endpoint: &micro.EndpointConfig{
					Subject: "test.func",
					Handler: micro.HandlerFunc(handler),
				},
			})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer svc.Stop()

			nfo := svc.Info()
			if nfo.Metadata == nil {
				t.Fatalf("Produced nil metadata")
			}

			resp, err := nc.RequestMsg(&nats.Msg{
				Subject: "test.func",
				Data:    []byte("req"),
				Header:  nats.Header{"key": []string{"value"}},
			}, 50*time.Millisecond)
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
				expectedHeaders := micro.Headers{
					"Nats-Service-Error-Code": []string{resp.Header.Get("Nats-Service-Error-Code")},
					"Nats-Service-Error":      []string{resp.Header.Get("Nats-Service-Error")},
				}
				for k, v := range test.respondHeaders {
					expectedHeaders[k] = v
				}
				if !reflect.DeepEqual(expectedHeaders, micro.Headers(resp.Header)) {
					t.Fatalf("Invalid response headers; want: %v; got: %v", test.respondHeaders, resp.Header)
				}
				return
			}

			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if !bytes.Equal(bytes.TrimSpace(resp.Data), bytes.TrimSpace(test.expectedResponse)) {
				t.Fatalf("Invalid response; want: %s; got: %s", string(test.expectedResponse), string(resp.Data))
			}

			if !reflect.DeepEqual(test.respondHeaders, micro.Headers(resp.Header)) {
				t.Fatalf("Invalid response headers; want: %v; got: %v", test.respondHeaders, resp.Header)
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

func TestControlSubject(t *testing.T) {
	tests := []struct {
		name            string
		verb            micro.Verb
		srvName         string
		id              string
		expectedSubject string
		withError       error
	}{
		{
			name:            "PING ALL",
			verb:            micro.PingVerb,
			expectedSubject: "$SRV.PING",
		},
		{
			name:            "PING name",
			verb:            micro.PingVerb,
			srvName:         "test",
			expectedSubject: "$SRV.PING.test",
		},
		{
			name:            "PING id",
			verb:            micro.PingVerb,
			srvName:         "test",
			id:              "123",
			expectedSubject: "$SRV.PING.test.123",
		},
		{
			name:      "invalid verb",
			verb:      micro.Verb(100),
			withError: micro.ErrVerbNotSupported,
		},
		{
			name:      "name not provided",
			verb:      micro.PingVerb,
			srvName:   "",
			id:        "123",
			withError: micro.ErrServiceNameRequired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := micro.ControlSubject(test.verb, test.srvName, test.id)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
			if res != test.expectedSubject {
				t.Errorf("Invalid subject; want: %q; got: %q", test.expectedSubject, res)
			}
		})
	}
}

func TestCustomQueueGroup(t *testing.T) {
	tests := []struct {
		name                string
		endpointInit        func(*testing.T, *nats.Conn) micro.Service
		expectedQueueGroups map[string]string
	}{
		{
			name: "default queue group",
			endpointInit: func(t *testing.T, nc *nats.Conn) micro.Service {
				srv, err := micro.AddService(nc, micro.Config{
					Name:    "test_service",
					Version: "0.0.1",
					Endpoint: &micro.EndpointConfig{
						Subject: "foo",
						Handler: micro.HandlerFunc(func(r micro.Request) {}),
					},
				})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				err = srv.AddEndpoint("bar", micro.HandlerFunc(func(r micro.Request) {}))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				return srv
			},
			expectedQueueGroups: map[string]string{
				"default": "q",
				"bar":     "q",
			},
		},
		{
			name: "custom queue group on service config",
			endpointInit: func(t *testing.T, nc *nats.Conn) micro.Service {
				srv, err := micro.AddService(nc, micro.Config{
					Name:       "test_service",
					Version:    "0.0.1",
					QueueGroup: "custom",
					Endpoint: &micro.EndpointConfig{
						Subject: "foo",
						Handler: micro.HandlerFunc(func(r micro.Request) {}),
					},
				})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// add endpoint on service directly, should have the same queue group
				err = srv.AddEndpoint("bar", micro.HandlerFunc(func(r micro.Request) {}))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// add group with queue group from service config
				g1 := srv.AddGroup("g1")

				// add endpoint on group, should have queue group from service config
				err = g1.AddEndpoint("baz", micro.HandlerFunc(func(r micro.Request) {}))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				return srv
			},
			expectedQueueGroups: map[string]string{
				"default": "custom",
				"bar":     "custom",
				"baz":     "custom",
			},
		},
		{
			name: "overwriting queue groups",
			endpointInit: func(t *testing.T, nc *nats.Conn) micro.Service {
				srv, err := micro.AddService(nc, micro.Config{
					Name:       "test_service",
					Version:    "0.0.1",
					QueueGroup: "q-config",
					Endpoint: &micro.EndpointConfig{
						Subject:    "foo",
						QueueGroup: "q-default",
						Handler:    micro.HandlerFunc(func(r micro.Request) {}),
					},
				})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				g1 := srv.AddGroup("g1", micro.WithGroupQueueGroup("q-g1"))

				// should have the same queue group as the parent group
				g2 := g1.AddGroup("g2")

				// overwrite parent group queue group
				g3 := g2.AddGroup("g3", micro.WithGroupQueueGroup("q-g3"))

				// add endpoint on service directly, overwriting the queue group
				err = srv.AddEndpoint("bar", micro.HandlerFunc(func(r micro.Request) {}), micro.WithEndpointQueueGroup("q-bar"))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// add endpoint on group, should have queue group from g1
				err = g2.AddEndpoint("baz", micro.HandlerFunc(func(r micro.Request) {}))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// add endpoint on group, overwriting the queue group
				err = g2.AddEndpoint("qux", micro.HandlerFunc(func(r micro.Request) {}), micro.WithEndpointQueueGroup("q-qux"))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// add endpoint on group, should have queue group from g3
				err = g3.AddEndpoint("quux", micro.HandlerFunc(func(r micro.Request) {}))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				return srv
			},
			expectedQueueGroups: map[string]string{
				"default": "q-default",
				"bar":     "q-bar",
				"baz":     "q-g1",
				"qux":     "q-qux",
				"quux":    "q-g3",
			},
		},
		{
			name: "empty queue group in option, inherit from parent",
			endpointInit: func(t *testing.T, nc *nats.Conn) micro.Service {
				srv, err := micro.AddService(nc, micro.Config{
					Name:       "test_service",
					Version:    "0.0.1",
					QueueGroup: "q-config",
				})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				// add endpoint on service directly, overwriting the queue group
				err = srv.AddEndpoint("bar", micro.HandlerFunc(func(r micro.Request) {}), micro.WithEndpointQueueGroup(""))
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}

				return srv
			},
			expectedQueueGroups: map[string]string{
				"bar": "q-config",
			},
		},
		{
			name: "invalid queue group on service config",
			endpointInit: func(t *testing.T, nc *nats.Conn) micro.Service {
				_, err := micro.AddService(nc, micro.Config{
					Name:       "test_service",
					Version:    "0.0.1",
					QueueGroup: ">.abc",
					Endpoint: &micro.EndpointConfig{
						Subject: "foo",
						Handler: micro.HandlerFunc(func(r micro.Request) {}),
					},
				})
				if !errors.Is(err, micro.ErrConfigValidation) {
					t.Fatalf("Expected error: %v; got: %v", micro.ErrConfigValidation, err)
				}
				return nil
			},
		},
		{
			name: "invalid queue group on endpoint",
			endpointInit: func(t *testing.T, nc *nats.Conn) micro.Service {
				_, err := micro.AddService(nc, micro.Config{
					Name:    "test_service",
					Version: "0.0.1",
					Endpoint: &micro.EndpointConfig{
						Subject:    "foo",
						QueueGroup: ">.abc",
						Handler:    micro.HandlerFunc(func(r micro.Request) {}),
					},
				})
				if !errors.Is(err, micro.ErrConfigValidation) {
					t.Fatalf("Expected error: %v; got: %v", micro.ErrConfigValidation, err)
				}
				return nil
			},
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
			srv := test.endpointInit(t, nc)
			if srv == nil {
				return
			}
			defer srv.Stop()
			info := srv.Info()
			endpoints := make(map[string]micro.EndpointInfo)
			for _, e := range info.Endpoints {
				endpoints[e.Name] = e
			}
			if len(endpoints) != len(test.expectedQueueGroups) {
				t.Fatalf("Expected %d endpoints; got: %d", len(test.expectedQueueGroups), len(endpoints))
			}
			for name, expectedGroup := range test.expectedQueueGroups {
				if endpoints[name].QueueGroup != expectedGroup {
					t.Fatalf("Invalid queue group for endpoint %q; want: %q; got: %q", name, expectedGroup, endpoints[name].QueueGroup)
				}
			}

			stats := srv.Stats()
			// make sure the same queue groups are on stats
			endpointStats := make(map[string]*micro.EndpointStats)

			for _, e := range stats.Endpoints {
				endpointStats[e.Name] = e
			}
			if len(endpointStats) != len(test.expectedQueueGroups) {
				t.Fatalf("Expected %d endpoints; got: %d", len(test.expectedQueueGroups), len(endpointStats))
			}
			for name, expectedGroup := range test.expectedQueueGroups {
				if endpointStats[name].QueueGroup != expectedGroup {
					t.Fatalf("Invalid queue group for endpoint %q; want: %q; got: %q", name, expectedGroup, endpointStats[name].QueueGroup)
				}
			}
		})
	}
}

func TestCustomQueueGroupMultipleResponses(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	for i := 0; i < 5; i++ {
		f := func(i int) func(r micro.Request) {
			return func(r micro.Request) {
				time.Sleep(10 * time.Millisecond)
				r.Respond([]byte(fmt.Sprintf("%d", i)))
			}
		}
		service, err := micro.AddService(nc, micro.Config{
			Name:       "test_service",
			Version:    "0.0.1",
			QueueGroup: fmt.Sprintf("q-%d", i),
			Endpoint: &micro.EndpointConfig{
				Subject: "foo",
				Handler: micro.HandlerFunc(f(i)),
			},
		})
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer service.Stop()
	}
	err = nc.PublishRequest("foo", "rply", []byte("req"))
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	sub, err := nc.SubscribeSync("rply")
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	expectedResponses := map[string]bool{
		"0": false,
		"1": false,
		"2": false,
		"3": false,
		"4": false,
	}
	defer sub.Unsubscribe()
	for i := 0; i < 5; i++ {
		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		expectedResponses[string(msg.Data)] = true
	}
	msg, err := sub.NextMsg(100 * time.Millisecond)
	if err == nil {
		t.Fatalf("Unexpected message: %v", string(msg.Data))
	}
	for k, v := range expectedResponses {
		if !v {
			t.Fatalf("Did not receive response from service %s", k)
		}
	}
}
