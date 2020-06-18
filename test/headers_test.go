// Copyright 2020 The NATS Authors
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
	"reflect"
	"testing"
	"time"

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
