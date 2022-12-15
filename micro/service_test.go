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

package micro

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
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
	doAdd := func(req *Request) error {
		if rand.Intn(10) == 0 {
			if err := req.Error("400", "client error!", nil); err != nil {
				t.Fatalf("Unexpected error when sending error response: %v", err)
			}

			// for client-side errors, return nil to avoid tracking the errors in stats
			return nil
		}
		// Happy Path.
		// Random delay between 5-10ms
		time.Sleep(5*time.Millisecond + time.Duration(rand.Intn(5))*time.Millisecond)
		if err := req.Respond([]byte("42")); err != nil {
			if err := req.Error("500", "Unexpected error!", nil); err != nil {
				t.Fatalf("Unexpected error when sending error response: %v", err)
			}
			return err
		}
		return nil
	}

	var svcs []Service

	// Create 5 service responders.
	config := Config{
		Name:        "CoolAddService",
		Version:     "0.1.0",
		Description: "Add things together",
		Endpoint: Endpoint{
			Subject: "svc.add",
			Handler: doAdd,
		},
		Schema: Schema{Request: "", Response: ""},
	}

	for i := 0; i < 5; i++ {
		svc, err := AddService(nc, config)
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
		requestsNum += srvStats.NumRequests
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
	emptyStats := Stats{
		ServiceIdentity: svcs[0].Info().ServiceIdentity,
	}

	if svcs[0].Stats() != emptyStats {
		t.Fatalf("Expected empty stats after reset; got: %+v", svcs[0].Stats())
	}

}

func TestAddService(t *testing.T) {
	testHandler := func(*Request) error { return nil }
	errNats := make(chan struct{})
	errService := make(chan struct{})
	closedNats := make(chan struct{})
	doneService := make(chan struct{})

	tests := []struct {
		name              string
		givenConfig       Config
		natsClosedHandler nats.ConnHandler
		natsErrorHandler  nats.ErrHandler
		asyncErrorSubject string
		expectedPing      Ping
		withError         error
	}{
		{
			name: "minimal config",
			givenConfig: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
			},
			expectedPing: Ping{
				Name:    "test_service",
				Version: "0.1.0",
			},
		},
		{
			name: "with done handler, no handlers on nats connection",
			givenConfig: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				DoneHandler: func(Service) {
					doneService <- struct{}{}
				},
			},
			expectedPing: Ping{
				Name:    "test_service",
				Version: "0.1.0",
			},
		},
		{
			name: "with error handler, no handlers on nats connection",
			givenConfig: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				ErrorHandler: func(Service, *NATSError) {
					errService <- struct{}{}
				},
			},
			expectedPing: Ping{
				Name:    "test_service",
				Version: "0.1.0",
			},
			asyncErrorSubject: "test.sub",
		},
		{
			name: "with error handler, no handlers on nats connection, error on monitoring subject",
			givenConfig: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				ErrorHandler: func(Service, *NATSError) {
					errService <- struct{}{}
				},
			},
			expectedPing: Ping{
				Name:    "test_service",
				Version: "0.1.0",
			},
			asyncErrorSubject: "$SVC.PING.TEST_SERVICE",
		},
		{
			name: "with done handler, append to nats handlers",
			givenConfig: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				DoneHandler: func(Service) {
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
				Name:    "test_service",
				Version: "0.1.0",
			},
			asyncErrorSubject: "test.sub",
		},
		{
			name: "with error handler, append to nats handlers",
			givenConfig: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				DoneHandler: func(Service) {
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
				Name:    "test_service",
				Version: "0.1.0",
			},
		},
		{
			name: "with error handler, append to nats handlers, error on monitoring subject",
			givenConfig: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
				DoneHandler: func(Service) {
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
				Name:    "test_service",
				Version: "0.1.0",
			},
			asyncErrorSubject: "$SVC.PING.TEST_SERVICE",
		},
		{
			name: "validation error, invalid service name",
			givenConfig: Config{
				Name:    "test_service!",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: testHandler,
				},
			},
			withError: ErrConfigValidation,
		},
		{
			name: "validation error, invalid version",
			givenConfig: Config{
				Name:    "test_service!",
				Version: "abc",
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
				Name:    "test_service",
				Version: "0.1.0",
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
				Name:    "test_service",
				Version: "0.1.0",
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

			srv, err := AddService(nc, test.givenConfig)
			if test.withError != nil {
				if !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			info := srv.Info()
			pingSubject, err := ControlSubject(PingVerb, info.Name, info.ID)
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

			test.expectedPing.ID = info.ID
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

func TestMonitoringHandlers(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	asyncErr := make(chan struct{})
	errHandler := func(s Service, n *NATSError) {
		asyncErr <- struct{}{}
	}

	config := Config{
		Name:    "test_service",
		Version: "0.1.0",
		Endpoint: Endpoint{
			Subject: "test.sub",
			Handler: func(*Request) error { return nil },
		},
		Schema: Schema{
			Request: "some_schema",
		},
		ErrorHandler: errHandler,
	}
	srv, err := AddService(nc, config)
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
			expectedResponse: Ping{
				Name:    "test_service",
				Version: "0.1.0",
				ID:      info.ID,
			},
		},
		{
			name:    "PING name",
			subject: "$SRV.PING.TEST_SERVICE",
			expectedResponse: Ping{
				Name:    "test_service",
				Version: "0.1.0",
				ID:      info.ID,
			},
		},
		{
			name:    "PING ID",
			subject: fmt.Sprintf("$SRV.PING.TEST_SERVICE.%s", info.ID),
			expectedResponse: Ping{
				Name:    "test_service",
				Version: "0.1.0",
				ID:      info.ID,
			},
		},
		{
			name:    "INFO all",
			subject: "$SRV.INFO",
			expectedResponse: Info{
				ServiceIdentity: ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Subject: "test.sub",
			},
		},
		{
			name:    "INFO name",
			subject: "$SRV.INFO.TEST_SERVICE",
			expectedResponse: Info{
				ServiceIdentity: ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Subject: "test.sub",
			},
		},
		{
			name:    "INFO ID",
			subject: fmt.Sprintf("$SRV.INFO.TEST_SERVICE.%s", info.ID),
			expectedResponse: Info{
				ServiceIdentity: ServiceIdentity{
					Name:    "test_service",
					Version: "0.1.0",
					ID:      info.ID,
				},
				Subject: "test.sub",
			},
		},
		{
			name:    "SCHEMA all",
			subject: "$SRV.SCHEMA",
			expectedResponse: Schema{
				Request: "some_schema",
			},
		},
		{
			name:    "SCHEMA name",
			subject: "$SRV.SCHEMA.TEST_SERVICE",
			expectedResponse: Schema{
				Request: "some_schema",
			},
		},
		{
			name:    "SCHEMA ID",
			subject: fmt.Sprintf("$SRV.SCHEMA.TEST_SERVICE.%s", info.ID),
			expectedResponse: Schema{
				Request: "some_schema",
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

func TestServiceStats(t *testing.T) {
	handler := func(r *Request) error {
		if bytes.Equal(r.Data, []byte("err")) {
			r.Error("500", "oops", nil)
			return fmt.Errorf("oops")
		}

		// client errors (validation etc.) should not be accounted for in stats
		if bytes.Equal(r.Data, []byte("client_err")) {
			r.Error("400", "bad request", nil)
			return nil
		}
		r.Respond([]byte("ok"))
		return nil
	}
	tests := []struct {
		name          string
		config        Config
		expectedStats map[string]interface{}
	}{
		{
			name: "without schema or stats handler",
			config: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: handler,
				},
			},
		},
		{
			name: "with stats handler",
			config: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: handler,
				},
				StatsHandler: func(e Endpoint) interface{} {
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
			config: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: handler,
				},
				Schema: Schema{
					Request: "some_schema",
				},
			},
		},
		{
			name: "with schema and stats handler",
			config: Config{
				Name:    "test_service",
				Version: "0.1.0",
				Endpoint: Endpoint{
					Subject: "test.sub",
					Handler: handler,
				},
				Schema: Schema{
					Request: "some_schema",
				},
				StatsHandler: func(e Endpoint) interface{} {
					return map[string]interface{}{
						"key": "val",
					}
				},
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

			srv, err := AddService(nc, test.config)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer srv.Stop()
			for i := 0; i < 10; i++ {
				if _, err := nc.Request(srv.Info().Subject, []byte("msg"), time.Second); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			if _, err := nc.Request(srv.Info().Subject, []byte("client_err"), time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if _, err := nc.Request(srv.Info().Subject, []byte("err"), time.Second); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			info := srv.Info()
			resp, err := nc.Request(fmt.Sprintf("$SRV.STATS.TEST_SERVICE.%s", info.ID), nil, 1*time.Second)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			var stats Stats
			if err := json.Unmarshal(resp.Data, &stats); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			if stats.Name != info.Name {
				t.Errorf("Unexpected service name; want: %s; got: %s", info.Name, stats.Name)
			}
			if stats.ID != info.ID {
				t.Errorf("Unexpected service name; want: %s; got: %s", info.ID, stats.ID)
			}
			if stats.NumRequests != 12 {
				t.Errorf("Unexpected num_requests; want: 12; got: %d", stats.NumRequests)
			}
			if stats.NumErrors != 1 {
				t.Errorf("Unexpected num_requests; want: 1; got: %d", stats.NumErrors)
			}
			if test.expectedStats != nil {
				if val, ok := stats.Data.(map[string]interface{}); !ok || !reflect.DeepEqual(val, test.expectedStats) {
					t.Fatalf("Invalid data from stats handler; want: %v; got: %v", test.expectedStats, val)
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
			errData := test.errData
			// Stub service.
			handler := func(req *Request) error {
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
							return nil
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
							return nil
						}
						if err != nil {
							t.Fatalf("Unexpected error when sending response: %v", err)
						}
					}
					return nil
				}

				err := req.Error(errCode, errDesc, errData)
				if respError != nil {
					if !errors.Is(err, respError) {
						t.Fatalf("Expected error: %v; got: %v", respError, err)
					}
					return nil
				}
				if err != nil {
					t.Fatalf("Unexpected error when sending response: %v", err)
				}
				return nil
			}

			svc, err := AddService(nc, Config{
				Name:        "CoolService",
				Version:     "0.1.0",
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
				if !bytes.Equal(resp.Data, test.errData) {
					t.Fatalf("Invalid response payload; want: %q; got: %q", string(test.errData), resp.Data)
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

func TestControlSubject(t *testing.T) {
	tests := []struct {
		name            string
		verb            Verb
		srvName         string
		id              string
		expectedSubject string
		withError       error
	}{
		{
			name:            "PING ALL",
			verb:            PingVerb,
			expectedSubject: "$SRV.PING",
		},
		{
			name:            "PING name",
			verb:            PingVerb,
			srvName:         "test",
			expectedSubject: "$SRV.PING.TEST",
		},
		{
			name:            "PING id",
			verb:            PingVerb,
			srvName:         "test",
			id:              "123",
			expectedSubject: "$SRV.PING.TEST.123",
		},
		{
			name:      "invalid verb",
			verb:      Verb(100),
			withError: ErrVerbNotSupported,
		},
		{
			name:      "name not provided",
			verb:      PingVerb,
			srvName:   "",
			id:        "123",
			withError: ErrServiceNameRequired,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := ControlSubject(test.verb, test.srvName, test.id)
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
