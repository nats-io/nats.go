// Copyright 2020-2026 The NATS Authors
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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

func TestBasicHeaders(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
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

		if !m.Equal(msg) {
			t.Fatalf("Messages did not match! \n%+v\n%+v\n", m, msg)
		}
	})
}

func TestRequestMsg(t *testing.T) {
	withServer(t, func(t *testing.T, nc *nats.Conn) {
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
	})
}

func TestRequestMsgRaceAsyncInfo(t *testing.T) {
	// Original spun up a 2-node cluster and then in a goroutine repeatedly
	// started/shut down the second node to provoke async INFO churn. The
	// testservice equivalent: create a 2-node cluster up front and Stop/Start
	// the second node in a loop.
	c := newTester(t)
	inst := c.CreateCluster(t, 2, false)
	t.Cleanup(func() { inst.Destroy(t) })

	primary := inst.Servers[0]
	churn := inst.Servers[1]

	nc, err := nats.Connect(primary.URL)
	if err != nil {
		t.Fatalf("Error connecting to server: %v", err)
	}
	defer nc.Close()

	nc2, err := nats.Connect(primary.URL, nats.UseOldRequestStyle())
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

	// Churn goroutine bypasses inst.StopServer/StartServer because those
	// fail via t.Fatalf from a non-test goroutine, which is illegal per the
	// testing docs (FailNow → runtime.Goexit only terminates the calling
	// goroutine; the parent test may continue obliviously). Drive the same
	// tester RPCs directly and surface errors via an error channel that the
	// test goroutine drains after the workload.
	testerNc, err := nats.Connect(os.Getenv("TESTER_NATS_URL"))
	if err != nil {
		t.Fatalf("could not dial tester: %v", err)
	}
	defer testerNc.Close()

	churnOp := func(subject, name string) error {
		body, _ := json.Marshal(map[string]string{"name": name})
		m, err := testerNc.Request(subject, body, 10*time.Second)
		if err != nil {
			return fmt.Errorf("%s: %w", subject, err)
		}
		if e := m.Header.Get("Nats-Service-Error"); e != "" {
			return fmt.Errorf("%s: %s", subject, e)
		}
		return nil
	}

	errCh := make(chan error, 1)
	wg := sync.WaitGroup{}
	wg.Add(1)
	ch := make(chan struct{})
	go func() {
		defer wg.Done()
		for {
			if err := churnOp("tester.stop.server", churn.Name); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			if err := churnOp("tester.start.server", churn.Name); err != nil {
				select {
				case errCh <- err:
				default:
				}
				return
			}
			select {
			case <-ch:
				return
			default:
			}
		}
	}()

	msg := nats.NewMsg(subject)
	msg.Header["Hdr-Test"] = []string{"quux"}
	for range 100 {
		nc.RequestMsg(msg, time.Second)
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		nc.RequestMsgWithContext(ctx, msg)
		cancel()

		nc2.RequestMsg(msg, time.Second)
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
		nc2.RequestMsgWithContext(ctx2, msg)
		cancel2()
	}

	close(ch)
	wg.Wait()
	select {
	case err := <-errCh:
		t.Fatalf("churn goroutine failed: %v", err)
	default:
	}
}

func TestNoHeaderSupport(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, testservice.WithTopLevel("no_header_support: true"))
	t.Cleanup(func() { inst.Destroy(t) })

	nc := dialInstance(t, inst)

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
	withServer(t, func(t *testing.T, nc *nats.Conn) {
		subject := "headers.test"
		sub, err := nc.SubscribeSync(subject)
		if err != nil {
			t.Fatalf("Could not subscribe to %q: %v", subject, err)
		}
		defer sub.Unsubscribe()

		m := nats.NewMsg(subject)

		hdr := http.Header{
			"CorrelationID": []string{"123"},
			"Msg-ID":        []string{"456"},
			"X-NATS-Keys":   []string{"A", "B", "C"},
			"X-Test-Keys":   []string{"D", "E", "F"},
		}

		type HeaderInterface interface {
			Add(key, value string)
			Del(key string)
			Get(key string) string
			Set(key, value string)
			Values(key string) []string
		}
		var _ HeaderInterface = http.Header{}
		var _ HeaderInterface = nats.Header{}

		m.Header = nats.Header(hdr)
		http.Header(m.Header).Set("accept-encoding", "json")
		http.Header(m.Header).Add("AUTHORIZATION", "s3cr3t")

		m.Header.Set("X-Test", "First")
		m.Header.Add("X-Test", "Second")
		m.Header.Add("X-Test", "Third")

		m.Data = []byte("Simple Headers")
		nc.PublishMsg(m)
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Did not receive response: %v", err)
		}
		if !m.Equal(msg) {
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
			v1, ok := msg.Header[test.Header]
			if !ok {
				t.Errorf("Expected %v to be present", test.Header)
			}
			if len(v1) != len(test.Values) {
				t.Errorf("Expected %v values in header, got: %v", len(test.Values), len(v1))
			}

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

		// Validate that headers processed by HTTP requests are not changed by
		// NATS through many hops.
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
			if err := nc.PublishMsg(resp); err != nil {
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
			if err := nc.PublishMsg(msg); err != nil {
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
		resp.Body.Close()
		if result := resp.Header.Get("X-Result-A"); result != "A" {
			t.Errorf("Unexpected header value, got: %+v", result)
		}
		if result := resp.Header.Get("X-Result-B"); result != "B" {
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
			if got := hdr.Get("foo"); got != "A" {
				t.Errorf("Expected: %v, got: %v", "A", got)
			}
			if got := hdr.Get("Foo"); got != "B" {
				t.Errorf("Expected: %v, got: %v", "B", got)
			}
			if got := hdr.Get("FOO"); got != "C" {
				t.Errorf("Expected: %v, got: %v", "C", got)
			}
			if got := hdr.Get("fOo"); got != "" {
				t.Errorf("Unexpected result, got: %v", got)
			}

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

					hdr.Del(test.key)

					if got := len(hdr); got != 2 {
						t.Errorf("Expected: %v, got: %v", 2, got)
					}
					if result := hdr.Values(test.key); result != nil {
						t.Errorf("Expected to cleanup all matching keys, got: %+v", result)
					}
					if v := hdr.Get(test.key); v != "" {
						t.Errorf("Expected to cleanup all matching keys, got: %v", v)
					}
				})
			}
		})
	})
}
