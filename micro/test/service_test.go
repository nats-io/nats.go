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
		t.Fatalf("Expected a total fo 50 requests processed, got: %d", requestsNum)
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
		{
			name: "validation error, invalid API URL",
			givenConfig: micro.Config{
				Name:    "test_service",
				Version: "0.0.1",
				APIURL:  "abc\r\n",
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
			if subjectsNum != len(info.Subjects) {
				t.Fatalf("Invalid number of registered endpoints; want: %d; got: %d", subjectsNum, len(info.Subjects))
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
				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: test.asyncErrorSubject}, fmt.Errorf("oops"))
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
				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: test.asyncErrorSubject}, fmt.Errorf("oops"))
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

				go nc.Opts.AsyncErrorCB(nc, &nats.Subscription{Subject: test.errSubject}, fmt.Errorf("oops"))
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
		name            string
		endpointName    string
		groups          []string
		expectedSubject string
	}{
		{
			name:            "no groups",
			endpointName:    "foo",
			expectedSubject: "foo",
		},
		{
			name:            "single group",
			endpointName:    "foo",
			groups:          []string{"g1"},
			expectedSubject: "g1.foo",
		},
		{
			name:            "single empty group",
			endpointName:    "foo",
			groups:          []string{""},
			expectedSubject: "foo",
		},
		{
			name:            "empty groups",
			endpointName:    "foo",
			groups:          []string{"", "g1", ""},
			expectedSubject: "g1.foo",
		},
		{
			name:            "multiple groups",
			endpointName:    "foo",
			groups:          []string{"g1", "g2", "g3"},
			expectedSubject: "g1.g2.g3.foo",
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
			if len(info.Subjects) != 1 {
				t.Fatalf("Expected 1 registered endpoint; got: %d", len(info.Subjects))
			}
			if info.Subjects[0] != test.expectedSubject {
				t.Fatalf("Invalid subject; want: %s, got: %s", test.expectedSubject, info.Subjects[0])
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
		APIURL:       "http://someapi.com/v1",
		ErrorHandler: errHandler,
		Endpoint: &micro.EndpointConfig{
			Subject:  "test.func",
			Handler:  micro.HandlerFunc(func(r micro.Request) {}),
			Metadata: map[string]string{"basic": "schema"},
			Schema: &micro.Schema{
				Request:  "request_schema",
				Response: "response_schema",
			},
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
		expectedResponse interface{}
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
				Subjects: []string{"test.func"},
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
				Subjects: []string{"test.func"},
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
				Subjects: []string{"test.func"},
			},
		},
		{
			name:    "SCHEMA all",
			subject: "$SRV.SCHEMA",
			expectedResponse: micro.SchemaResp{
				Type: micro.SchemaResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
				APIURL: "http://someapi.com/v1",
				Endpoints: []micro.EndpointSchema{
					{
						Name:     "default",
						Subject:  "test.func",
						Metadata: map[string]string{"basic": "schema"},
						Schema: micro.Schema{
							Request:  "request_schema",
							Response: "response_schema",
						},
					},
				},
			},
		},
		{
			name:    "SCHEMA name",
			subject: "$SRV.SCHEMA.test_service",
			expectedResponse: micro.SchemaResp{
				Type: micro.SchemaResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
				APIURL: "http://someapi.com/v1",
				Endpoints: []micro.EndpointSchema{
					{
						Name:     "default",
						Subject:  "test.func",
						Metadata: map[string]string{"basic": "schema"},
						Schema: micro.Schema{
							Request:  "request_schema",
							Response: "response_schema",
						},
					},
				},
			},
		},
		{
			name:    "SCHEMA ID",
			subject: fmt.Sprintf("$SRV.SCHEMA.test_service.%s", info.ID),
			expectedResponse: micro.SchemaResp{
				Type: micro.SchemaResponseType,
				ServiceIdentity: micro.ServiceIdentity{
					Name:     "test_service",
					Version:  "0.1.0",
					ID:       info.ID,
					Metadata: map[string]string{},
				},
				APIURL: "http://someapi.com/v1",
				Endpoints: []micro.EndpointSchema{
					{
						Name:     "default",
						Subject:  "test.func",
						Metadata: map[string]string{"basic": "schema"},
						Schema: micro.Schema{
							Request:  "request_schema",
							Response: "response_schema",
						},
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
		{
			name:      "SCHEMA error",
			subject:   "$SRV.SCHEMA",
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

			respMap := make(map[string]interface{})
			if err := json.Unmarshal(resp.Data, &respMap); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectedResponseJSON, err := json.Marshal(test.expectedResponse)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			expectedRespMap := make(map[string]interface{})
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
		APIURL:  "http://someapi.com/v1",
		Endpoint: &micro.EndpointConfig{
			Subject: "test.func",
			Handler: micro.ContextHandler(ctx, handler),
			Schema: &micro.Schema{
				Request:  "request_schema",
				Response: "response_schema",
			},
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

func TestServiceNilSchema(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	config := micro.Config{
		Name:    "test_service",
		Version: "0.1.0",
		APIURL:  "http://someapi.com/v1",
		Endpoint: &micro.EndpointConfig{
			Subject: "test.func",
			Handler: micro.HandlerFunc(func(r micro.Request) {}),
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

	resp, err := nc.Request("$SRV.SCHEMA.test_service", nil, 1*time.Second)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	schema := micro.SchemaResp{}
	err = json.Unmarshal(resp.Data, &schema)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	if schema.Endpoints[0].Schema.Response != "" {
		t.Fatalf("Invalid schema response: %#v", schema)
	}

	if schema.Endpoints[0].Schema.Response != "" {
		t.Fatalf("Invalid schema response: %#v", schema)
	}
}

func TestServiceStats(t *testing.T) {
	handler := func(r micro.Request) {
		r.Respond([]byte("ok"))
	}
	tests := []struct {
		name          string
		config        micro.Config
		schema        *micro.Schema
		expectedStats map[string]interface{}
	}{
		{
			name: "without schema or stats handler",
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
				StatsHandler: func(e *micro.Endpoint) interface{} {
					return map[string]interface{}{
						"key": "val",
					}
				},
			},
			expectedStats: map[string]interface{}{
				"key": "val",
			},
		},
		{
			name: "with schema",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				APIURL:  "http://someapi.com/v1",
			},
			schema: &micro.Schema{
				Request: "some_request",
			},
		},
		{
			name: "with default endpoint",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				APIURL:  "http://someapi.com/v1",
				Endpoint: &micro.EndpointConfig{
					Subject:  "test.func",
					Handler:  micro.HandlerFunc(handler),
					Metadata: map[string]string{"test": "value"},
					Schema: &micro.Schema{
						Request:  "some_request",
						Response: "some_response",
					},
				},
			},
			schema: &micro.Schema{
				Request:  "some_request",
				Response: "some_response",
			},
		},
		{
			name: "with schema and stats handler",
			config: micro.Config{
				Name:    "test_service",
				Version: "0.1.0",
				APIURL:  "http://someapi.com/v1",
				StatsHandler: func(e *micro.Endpoint) interface{} {
					return map[string]interface{}{
						"key": "val",
					}
				},
			},
			schema: &micro.Schema{
				Request: "some_request",
			},
			expectedStats: map[string]interface{}{
				"key": "val",
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
				if test.schema != nil {
					opts = append(opts, micro.WithEndpointSchema(test.schema))
				}
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
			if test.config.Endpoint != nil && test.config.Endpoint.Metadata != nil {
				if !reflect.DeepEqual(test.config.Endpoint.Metadata, stats.Endpoints[0].Metadata) {
					t.Errorf("invalid endpoint metadata: %v", stats.Endpoints[0].Metadata)
				}
			}

			if test.expectedStats != nil {
				var data map[string]interface{}
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
		respondData      interface{}
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
