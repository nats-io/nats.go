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
	"context"
	"fmt"
	"net/http"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"net/http/httptest"

	"github.com/nats-io/nats-server/v2/server"
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

	if err = nc.PublishMsg(nil); err != nats.ErrInvalidMsg {
		t.Errorf("Unexpected error: %v", err)
	}
	if _, err = nc.RequestMsg(nil, time.Second); err != nats.ErrInvalidMsg {
		t.Errorf("Unexpected error: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	if _, err = nc.RequestMsgWithContext(ctx, nil); err != nats.ErrInvalidMsg {
		t.Errorf("Unexpected error: %v", err)
	}
}

func TestRequestMsgRaceAsyncInfo(t *testing.T) {
	s1Opts := natsserver.DefaultTestOptions
	s1Opts.Host = "127.0.0.1"
	s1Opts.Port = -1
	s1Opts.Cluster.Name = "CLUSTER"
	s1Opts.Cluster.Host = "127.0.0.1"
	s1Opts.Cluster.Port = -1
	s := natsserver.RunServer(&s1Opts)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	// Extra client with old request.
	nc2, err := nats.Connect(s.ClientURL(), nats.UseOldRequestStyle())
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc2.Close()

	subject := "headers.test"
	if _, err := nc.Subscribe(subject, func(m *nats.Msg) {
		r := nats.NewMsg(m.Reply)
		r.Header["Hdr-Test"] = []string{"bar"}
		r.Data = []byte("+OK")
		m.RespondMsg(r)
	}); err != nil {
		t.Fatalf("subscribe failed: %v", err)
	}
	nc.Flush()

	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{})
	go func() {
		defer wg.Done()

		s2Opts := natsserver.DefaultTestOptions
		s2Opts.Host = "127.0.0.1"
		s2Opts.Port = -1
		s2Opts.Cluster.Name = "CLUSTER"
		s2Opts.Cluster.Host = "127.0.0.1"
		s2Opts.Cluster.Port = -1
		s2Opts.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", s.ClusterAddr().Port))
		for {
			s := natsserver.RunServer(&s2Opts)
			s.Shutdown()
			select {
			case <-ch:
				return
			default:
			}
		}
	}()

	msg := nats.NewMsg(subject)
	msg.Header["Hdr-Test"] = []string{"quux"}
	for i := 0; i < 100; i++ {
		nc.RequestMsg(msg, time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		nc.RequestMsgWithContext(ctx, msg)
		cancel()

		// Check with old style requests as well.
		nc2.RequestMsg(msg, time.Second)
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		nc2.RequestMsgWithContext(ctx2, msg)
		cancel2()
	}

	close(ch)
	wg.Wait()
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

	// http.Header preserves the original keys and allows case-sensitive
	// lookup by accessing the map directly.
	hdr := http.Header{
		"CorrelationID": []string{"123"},
		"Msg-ID":        []string{"456"},
		"X-NATS-Keys":   []string{"A", "B", "C"},
		"X-Test-Keys":   []string{"D", "E", "F"},
	}

	// Validate that can be used interchangeably with http.Header
	type HeaderInterface interface {
		Add(key, value string)
		Del(key string)
		Get(key string) string
		Set(key, value string)
		Values(key string) []string
	}
	var _ HeaderInterface = http.Header{}
	var _ HeaderInterface = nats.Header{}

	// A NATS Header is the same type as http.Header so simple casting
	// works to use canonical form used in Go HTTP servers if needed,
	// and it also preserves the same original keys like Go HTTP requests.
	m.Header = nats.Header(hdr)
	http.Header(m.Header).Set("accept-encoding", "json")
	http.Header(m.Header).Add("AUTHORIZATION", "s3cr3t")

	// Multi Value using the same matching key.
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

	// Confirm that received message is just like the one originally sent.
	if !reflect.DeepEqual(m, msg) {
		t.Fatalf("Messages did not match! \n%+v\n%+v\n", m, msg)
	}

	for _, test := range []struct {
		Header string
		Values []string
	}{
		{"Accept-Encoding", []string{"json"}},
		{"Authorization", []string{"s3cr3t"}},
		{"X-Test", []string{"First", "Second", "Third"}},
		{"CorrelationID", []string{"123"}},
		{"Msg-ID", []string{"456"}},
		{"X-NATS-Keys", []string{"A", "B", "C"}},
		{"X-Test-Keys", []string{"D", "E", "F"}},
	} {
		// Accessing directly will always work.
		v1, ok := msg.Header[test.Header]
		if !ok {
			t.Errorf("Expected %v to be present", test.Header)
		}
		if len(v1) != len(test.Values) {
			t.Errorf("Expected %v values in header, got: %v", len(test.Values), len(v1))
		}

		// Exact match is preferred and fastest for Get.
		v2 := msg.Header.Get(test.Header)
		if v2 == "" {
			t.Errorf("Expected %v to be present", test.Header)
		}
		if v1[0] != v2 {
			t.Errorf("Expected: %s, got: %v", v1, v2)
		}

		for k, val := range test.Values {
			hdr := msg.Header[test.Header]
			vv := hdr[k]
			if val != vv {
				t.Errorf("Expected %v values in header, got: %v", val, vv)
			}
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
		msg.Header = nats.Header(r.Header.Clone())
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

		// Remove Date from response header for testing.
		w.Header()["Date"] = nil

		w.WriteHeader(200)
		fmt.Fprintln(w, string(resp.Data))
	}))
	defer ts.Close()

	req, err := http.NewRequest("GET", ts.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	client := &http.Client{Timeout: 2 * time.Second}
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

	v, ok := msg.Header["x-trace-id"]
	if !ok {
		t.Fatal("Missing headers in message")
	}
	if !reflect.DeepEqual(v, []string{"S", "A", "B"}) {
		t.Fatal("Missing headers in message")
	}
	for _, key := range []string{"x-trace-id"} {
		v = msg.Header.Values(key)
		if v == nil {
			t.Fatal("Missing headers in message")
		}
		if !reflect.DeepEqual(v, []string{"S", "A", "B"}) {
			t.Fatal("Missing headers in message")
		}
	}

	t.Run("multi value header", func(t *testing.T) {
		getHeader := func() nats.Header {
			return nats.Header{
				"foo": []string{"A"},
				"Foo": []string{"B"},
				"FOO": []string{"C"},
			}
		}

		hdr := getHeader()
		got := hdr.Get("foo")
		expected := "A"
		if got != expected {
			t.Errorf("Expected: %v, got: %v", expected, got)
		}
		got = hdr.Get("Foo")
		expected = "B"
		if got != expected {
			t.Errorf("Expected: %v, got: %v", expected, got)
		}
		got = hdr.Get("FOO")
		expected = "C"
		if got != expected {
			t.Errorf("Expected: %v, got: %v", expected, got)
		}

		// No match.
		got = hdr.Get("fOo")
		if got != "" {
			t.Errorf("Unexpected result, got: %v", got)
		}

		// Only match explicitly.
		for _, test := range []struct {
			key            string
			expectedValues []string
		}{
			{"foo", []string{"A"}},
			{"Foo", []string{"B"}},
			{"FOO", []string{"C"}},
			{"fOO", nil},
			{"foO", nil},
		} {
			t.Run("", func(t *testing.T) {
				hdr := getHeader()
				result := hdr.Values(test.key)
				sort.Strings(result)

				if !reflect.DeepEqual(result, test.expectedValues) {
					t.Errorf("Expected: %+v, got: %+v", test.expectedValues, result)
				}
				if hdr.Get(test.key) == "" {
					return
				}

				// Cleanup all the matching keys.
				hdr.Del(test.key)

				got := len(hdr)
				expected := 2
				if got != expected {
					t.Errorf("Expected: %v, got: %v", expected, got)
				}
				result = hdr.Values(test.key)
				if result != nil {
					t.Errorf("Expected to cleanup all matching keys, got: %+v", result)
				}
				if v := hdr.Get(test.key); v != "" {
					t.Errorf("Expected to cleanup all matching keys, got: %v", v)
				}
			})
		}
	})
}
