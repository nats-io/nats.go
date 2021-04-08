// Copyright 2020-2021 The NATS Authors
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

package test

import (
	"fmt"
	"net/http"
	"reflect"
	"testing"
	"time"

	"net/http/httptest"

	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestBasicHeaders(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	subject := "headers.test"
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("Could not subscribe to %q: %v", subject, err)
	}
	defer sub.Unsubscribe()

	m := nats.NewMsg(subject)
	m.Header.Add("Accept-Encoding", "json")
	m.Header.Add("Authorization", "s3cr3t")
	m.Data = []byte("Hello Headers!")

	nc.PublishMsg(m)
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}

	// Blank out the sub since its not present in the original.
	msg.Sub = nil
	if !reflect.DeepEqual(m, msg) {
		t.Fatalf("Messages did not match! \n%+v\n%+v\n", m, msg)
	}
}

func TestRequestMsg(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	subject := "headers.test"
	sub, err := nc.Subscribe(subject, func(m *nats.Msg) {
		if m.Header.Get("Hdr-Test") != "1" {
			m.Respond([]byte("-ERR"))
		}

		r := nats.NewMsg(m.Reply)
		r.Header = m.Header
		r.Data = []byte("+OK")
		m.RespondMsg(r)
	})
	if err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	defer sub.Unsubscribe()

	msg := nats.NewMsg(subject)
	msg.Header.Add("Hdr-Test", "1")
	resp, err := nc.RequestMsg(msg, time.Second)
	if err != nil {
		t.Fatalf("Expected request to be published: %v", err)
	}
	if string(resp.Data) != "+OK" {
		t.Fatalf("Headers were not published to the requestor")
	}
	if resp.Header.Get("Hdr-Test") != "1" {
		t.Fatalf("Did not receive header in response")
	}
}

func TestNoHeaderSupport(t *testing.T) {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.NoHeaderSupport = true
	s := RunServerWithOptions(opts)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	m := nats.NewMsg("foo")
	m.Header.Add("Authorization", "s3cr3t")
	m.Data = []byte("Hello Headers!")

	if err := nc.PublishMsg(m); err != nats.ErrHeadersNotSupported {
		t.Fatalf("Expected an error, got %v", err)
	}

	if _, err := nc.RequestMsg(m, time.Second); err != nats.ErrHeadersNotSupported {
		t.Fatalf("Expected an error, got %v", err)
	}
}

func TestMsgHeadersCasePreserving(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	subject := "headers.test"
	sub, err := nc.SubscribeSync(subject)
	if err != nil {
		t.Fatalf("Could not subscribe to %q: %v", subject, err)
	}
	defer sub.Unsubscribe()

	m := nats.NewMsg(subject)

	// Avoid canonicalizing headers by creating headers manually.
	//
	// To not use canonical keys, Go recommends accessing the map directly.
	// https://golang.org/pkg/net/http/#Header.Set
	m.Header = http.Header{
		"CorrelationID": []string{"123"},
		"Msg-ID":        []string{"456"},
		"X-NATS-Keys":   []string{"A", "B", "C"},
		"X-Test-Keys":   []string{"D", "E", "F"},
	}

	// Users can opt-in to canonicalize an http.Header
	// by using http.Header.Add()
	m.Header.Add("Accept-Encoding", "json")
	m.Header.Add("Authorization", "s3cr3t")

	// Multi Value Header
	m.Header.Set("X-Test", "First")
	m.Header.Add("X-Test", "Second")
	m.Header.Add("X-Test", "Third")
	m.Data = []byte("Simple Headers")
	nc.PublishMsg(m)

	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Did not receive response: %v", err)
	}

	// Blank out the sub since its not present in the original.
	msg.Sub = nil
	if !reflect.DeepEqual(m, msg) {
		t.Fatalf("Messages did not match! \n%+v\n%+v\n", m, msg)
	}

	for _, test := range []struct {
		Header    string
		Values    []string
		Canonical bool
	}{
		{"Accept-Encoding", []string{"json"}, true},
		{"Authorization", []string{"s3cr3t"}, true},
		{"X-Test", []string{"First", "Second", "Third"}, true},
		{"CorrelationID", []string{"123"}, false},
		{"Msg-ID", []string{"456"}, false},
		{"X-NATS-Keys", []string{"A", "B", "C"}, false},
		{"X-Test-Keys", []string{"D", "E", "F"}, true},
	} {
		// Accessing directly will always work.
		v, ok := msg.Header[test.Header]
		if !ok {
			t.Errorf("Expected %v to be present", test.Header)
		}
		if len(v) != len(test.Values) {
			t.Errorf("Expected %v values in header, got: %v", len(test.Values), len(v))
		}

		for k, val := range test.Values {
			hdr := msg.Header[test.Header]
			vv := hdr[k]
			if val != vv {
				t.Errorf("Expected %v values in header, got: %v", val, vv)
			}
		}

		// Only canonical version of headers can be fetched with Add/Get/Values.
		// Need to access the map directly to get the non canonicalized version
		// as per the Go docs of textproto package.
		if !test.Canonical {
			continue
		}

		if len(test.Values) > 1 {
			if !reflect.DeepEqual(test.Values, msg.Header.Values(test.Header)) {
				t.Fatalf("Headers did not match! \n%+v\n%+v\n", test.Values, msg.Header.Values(test.Header))
			}
		} else {
			got := msg.Header.Get(test.Header)
			expected := test.Values[0]
			if got != expected {
				t.Errorf("Expected %v, got:%v", expected, got)
			}
		}
	}

	// Validate that headers processed by HTTP requests are not changed by NATS through many hops.
	errCh := make(chan error, 2)
	msgCh := make(chan *nats.Msg, 1)
	sub, err = nc.Subscribe("nats.svc.A", func(msg *nats.Msg) {
		//lint:ignore SA1008 non canonical form test
		hdr := msg.Header["x-trace-id"]
		hdr = append(hdr, "A")
		msg.Header["x-trace-id"] = hdr
		msg.Header.Add("X-Result-A", "A")
		msg.Subject = "nats.svc.B"
		resp, err := nc.RequestMsg(msg, 2*time.Second)
		if err != nil {
			errCh <- err
			return
		}

		resp.Subject = msg.Reply
		err = nc.PublishMsg(resp)
		if err != nil {
			errCh <- err
			return
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	defer sub.Unsubscribe()

	sub, err = nc.Subscribe("nats.svc.B", func(msg *nats.Msg) {
		//lint:ignore SA1008 non canonical form test
		hdr := msg.Header["x-trace-id"]
		hdr = append(hdr, "B")
		msg.Header["x-trace-id"] = hdr
		msg.Header.Add("X-Result-B", "B")
		msg.Subject = msg.Reply
		msg.Data = []byte("OK!")
		err := nc.PublishMsg(msg)
		if err != nil {
			errCh <- err
			return
		}
	})
	if err != nil {
		t.Fatal(err)
	}

	defer sub.Unsubscribe()

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		msg := nats.NewMsg("nats.svc.A")
		msg.Header = r.Header.Clone()
		msg.Header["x-trace-id"] = []string{"S"}
		msg.Header["Result-ID"] = []string{"OK"}
		resp, err := nc.RequestMsg(msg, 2*time.Second)
		if err != nil {
			errCh <- err
			return
		}
		msgCh <- resp

		for k, v := range resp.Header {
			w.Header()[k] = v
		}

		// Remove Date for testing.
		w.Header()["Date"] = nil

		w.WriteHeader(200)
		fmt.Fprintln(w, string(resp.Data))
	}))
	defer ts.Close()

	req, err := http.NewRequest("GET", ts.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: 3 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	result := resp.Header.Get("X-Result-A")
	if result != "A" {
		t.Errorf("Unexpected header value, got: %+v", result)
	}
	result = resp.Header.Get("X-Result-B")
	if result != "B" {
		t.Errorf("Unexpected header value, got: %+v", result)
	}

	select {
	case <-time.After(1 * time.Second):
		t.Fatal("Timeout waiting for message.")
	case err = <-errCh:
		if err != nil {
			t.Fatal(err)
		}
	case msg = <-msgCh:
	}
	if len(msg.Header) != 6 {
		t.Errorf("Wrong number of headers in NATS message, got: %v", len(msg.Header))
	}

	//lint:ignore SA1008 non canonical form test
	v, ok := msg.Header["x-trace-id"]
	if !ok {
		t.Fatal("Missing headers in message")
	}
	if !reflect.DeepEqual(v, []string{"S", "A", "B"}) {
		t.Fatal("Missing headers in message")
	}
}
