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

package test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestCreateOrUpdateConsumer(t *testing.T) {
	tests := []struct {
		name           string
		consumerConfig jetstream.ConsumerConfig
		shouldCreate   bool
		withError      error
	}{
		{
			name:           "create durable pull consumer",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur"},
			shouldCreate:   true,
		},
		{
			name:           "create ephemeral pull consumer",
			consumerConfig: jetstream.ConsumerConfig{AckPolicy: jetstream.AckNonePolicy},
			shouldCreate:   true,
		},
		{
			name:           "with filter subject",
			consumerConfig: jetstream.ConsumerConfig{FilterSubject: "FOO.A"},
			shouldCreate:   true,
		},
		{
			name:           "with multiple filter subjects",
			consumerConfig: jetstream.ConsumerConfig{FilterSubjects: []string{"FOO.A", "FOO.B"}},
			shouldCreate:   true,
		},
		{
			name:           "consumer already exists, update",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur", Description: "test consumer"},
		},
		{
			name:           "consumer already exists, illegal update",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur", AckPolicy: jetstream.AckNonePolicy},
			withError:      jetstream.ErrConsumerCreate,
		},
		{
			name:           "invalid durable name",
			consumerConfig: jetstream.ConsumerConfig{Durable: "dur.123"},
			withError:      jetstream.ErrInvalidConsumerName,
		},
	}

	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			var sub *nats.Subscription
			if test.consumerConfig.FilterSubject != "" {
				sub, err = nc.SubscribeSync(fmt.Sprintf("$JS.API.CONSUMER.CREATE.foo.*.%s", test.consumerConfig.FilterSubject))
			} else {
				sub, err = nc.SubscribeSync("$JS.API.CONSUMER.CREATE.foo.*")
			}
			c, err := s.CreateOrUpdateConsumer(ctx, test.consumerConfig)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if test.shouldCreate {
				if _, err := sub.NextMsgWithContext(ctx); err != nil {
					t.Fatalf("Expected request on %s; got %s", sub.Subject, err)
				}
			}
			ci, err := s.Consumer(ctx, c.CachedInfo().Name)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if ci.CachedInfo().Config.AckPolicy != test.consumerConfig.AckPolicy {
				t.Fatalf("Invalid ack policy; want: %s; got: %s", test.consumerConfig.AckPolicy, ci.CachedInfo().Config.AckPolicy)
			}
		})
	}
}

func TestConsumer(t *testing.T) {
	tests := []struct {
		name      string
		durable   string
		withError error
	}{
		{
			name:    "get existing consumer",
			durable: "dur",
		},
		{
			name:      "consumer does not exist",
			durable:   "abc",
			withError: jetstream.ErrConsumerNotFound,
		},
		{
			name:      "invalid durable name",
			durable:   "dur.123",
			withError: jetstream.ErrInvalidConsumerName,
		},
	}

	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "dur", AckPolicy: jetstream.AckAllPolicy, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			c, err := s.Consumer(ctx, test.durable)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if c.CachedInfo().Name != test.durable {
				t.Fatalf("Unexpected consumer fetched; want: %s; got: %s", test.durable, c.CachedInfo().Name)
			}
		})
	}
}

func TestDeleteConsumer(t *testing.T) {
	tests := []struct {
		name      string
		durable   string
		withError error
	}{
		{
			name:    "delete existing consumer",
			durable: "dur",
		},
		{
			name:      "consumer does not exist",
			durable:   "dur",
			withError: jetstream.ErrConsumerNotFound,
		},
		{
			name:      "invalid durable name",
			durable:   "dur.123",
			withError: jetstream.ErrInvalidConsumerName,
		},
	}

	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	_, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{Durable: "dur", AckPolicy: jetstream.AckAllPolicy, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := s.DeleteConsumer(ctx, test.durable)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			_, err = s.Consumer(ctx, test.durable)
			if err == nil || !errors.Is(err, jetstream.ErrConsumerNotFound) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrConsumerNotFound, err)
			}
		})
	}
}

func TestStreamInfo(t *testing.T) {
	tests := []struct {
		name                string
		subjectsFilter      string
		expectedSubjectMsgs map[string]uint64
		deletedDetails      bool
		withError           error
	}{
		{
			name: "info without opts",
		},
		{
			name:           "with deleted details",
			deletedDetails: true,
		},
		{
			name:                "with subjects filter, one subject",
			subjectsFilter:      "FOO.A",
			expectedSubjectMsgs: map[string]uint64{"FOO.A": 8},
		},
		{
			name:                "with subjects filter, wildcard subject",
			subjectsFilter:      "FOO.*",
			expectedSubjectMsgs: map[string]uint64{"FOO.A": 8, "FOO.B": 10},
		},
		{
			name:                "with subjects filter, and deleted details",
			subjectsFilter:      "FOO.A",
			expectedSubjectMsgs: map[string]uint64{"FOO.A": 8},
		},
	}

	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 0; i < 10; i++ {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	if err := s.DeleteMsg(ctx, 3); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s.DeleteMsg(ctx, 5); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			opts := make([]jetstream.StreamInfoOpt, 0)
			if test.deletedDetails {
				opts = append(opts, jetstream.WithDeletedDetails(test.deletedDetails))
			}
			if test.subjectsFilter != "" {
				opts = append(opts, jetstream.WithSubjectFilter(test.subjectsFilter))
			}
			info, err := s.Info(ctx, opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if info.Config.Description != "desc" {
				t.Fatalf("Unexpected description value fetched; want: foo; got: %s", info.Config.Description)
			}
			if test.deletedDetails {
				if info.State.NumDeleted != 2 {
					t.Fatalf("Expected 2 deleted messages; got: %d", info.State.NumDeleted)
				}
				if len(info.State.Deleted) != 2 || !reflect.DeepEqual(info.State.Deleted, []uint64{3, 5}) {
					t.Fatalf("Invalid value for deleted details; want: [3 5] got: %v", info.State.Deleted)
				}
			}
			if test.subjectsFilter != "" {
				if !reflect.DeepEqual(test.expectedSubjectMsgs, info.State.Subjects) {
					t.Fatalf("Invalid value for subjects filter; want: %v; got: %v", test.expectedSubjectMsgs, info.State.Subjects)
				}
			}
		})
	}
}

func TestStreamCachedInfo(t *testing.T) {
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	s, err := js.CreateStream(ctx, jetstream.StreamConfig{
		Name:        "foo",
		Subjects:    []string{"FOO.*"},
		Description: "desc",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info := s.CachedInfo()

	if info.Config.Name != "foo" {
		t.Fatalf("Invalid stream name; expected: 'foo'; got: %s", info.Config.Name)
	}
	if info.Config.Description != "desc" {
		t.Fatalf("Invalid stream description; expected: 'desc'; got: %s", info.Config.Description)
	}

	// update consumer and see if info is updated
	_, err = js.UpdateStream(ctx, jetstream.StreamConfig{
		Name:        "foo",
		Subjects:    []string{"FOO.*"},
		Description: "updated desc",
	})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	info = s.CachedInfo()

	if info.Config.Name != "foo" {
		t.Fatalf("Invalid stream name; expected: 'foo'; got: %s", info.Config.Name)
	}

	// description should not be updated when using cached values
	if info.Config.Description != "desc" {
		t.Fatalf("Invalid stream description; expected: 'updated desc'; got: %s", info.Config.Description)
	}
}

func TestGetMsg(t *testing.T) {
	tests := []struct {
		name            string
		seq             uint64
		opts            []jetstream.GetMsgOpt
		expectedData    string
		expectedHeaders nats.Header
		withError       error
	}{
		{
			name:         "get existing msg",
			seq:          2,
			expectedData: "msg 1 on subject B",
		},
		{
			name:      "get deleted msg",
			seq:       3,
			withError: jetstream.ErrMsgNotFound,
		},
		{
			name:      "get non existing msg",
			seq:       50,
			withError: jetstream.ErrMsgNotFound,
		},
		{
			name:         "with next for subject",
			seq:          1,
			opts:         []jetstream.GetMsgOpt{jetstream.WithGetMsgSubject("*.C")},
			expectedData: "msg with headers",
			expectedHeaders: map[string][]string{
				"X-Nats-Test-Data": {"test_data"},
				"X-Nats-Key":       {"123"},
			},
		},
		{
			name:         "get msg with headers",
			seq:          9,
			expectedData: "msg with headers",
			expectedHeaders: map[string][]string{
				"X-Nats-Test-Data": {"test_data"},
				"X-Nats-Key":       {"123"},
			},
		},
	}

	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s1, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	if _, err := js.PublishMsg(ctx, &nats.Msg{
		Data: []byte("msg with headers"),
		Header: map[string][]string{
			"X-Nats-Test-Data": {"test_data"},
			"X-Nats-Key":       {"123"},
		},
		Subject: "FOO.C",
	}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s1.DeleteMsg(ctx, 3); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s1.DeleteMsg(ctx, 5); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	// same stream, but with allow direct
	s2, err := js.CreateStream(ctx,
		jetstream.StreamConfig{Name: "bar",
			Subjects:    []string{"BAR.*"},
			Description: "desc",
			AllowDirect: true,
		})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(ctx, "BAR.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "BAR.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}
	if _, err := js.PublishMsg(ctx, &nats.Msg{
		Data: []byte("msg with headers"),
		Header: map[string][]string{
			"X-Nats-Test-Data": {"test_data"},
			"X-Nats-Key":       {"123"},
		},
		Subject: "BAR.C",
	}); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s2.DeleteMsg(ctx, 3); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := s2.DeleteMsg(ctx, 5); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s - %s", test.name, "allow direct: false"), func(t *testing.T) {
			msg, err := s1.GetMsg(ctx, test.seq, test.opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(msg.Data) != test.expectedData {
				t.Fatalf("Invalid message data; want: %s; got: %s", test.expectedData, string(msg.Data))
			}
			if !reflect.DeepEqual(msg.Header, test.expectedHeaders) {
				t.Fatalf("Invalid message headers; want: %v; got: %v", test.expectedHeaders, msg.Header)
			}
		})
		t.Run(fmt.Sprintf("%s - %s", test.name, "allow direct: true"), func(t *testing.T) {
			msg, err := s2.GetMsg(ctx, test.seq, test.opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(msg.Data) != test.expectedData {
				t.Fatalf("Invalid message data; want: %s; got: %s", test.expectedData, string(msg.Data))
			}
			for k, v := range test.expectedHeaders {
				if !reflect.DeepEqual(msg.Header[k], v) {
					t.Fatalf("Expected header: %v; got: %v", v, msg.Header[k])
				}
			}
		})
	}
}

func TestGetLastMsgForSubject(t *testing.T) {
	tests := []struct {
		name         string
		subject      string
		expectedData string
		allowDirect  bool
		withError    error
	}{
		{
			name:         "get existing msg",
			subject:      "*.A",
			expectedData: "msg 4 on subject A",
		},
		{
			name:         "get last msg from stream",
			subject:      ">",
			expectedData: "msg 4 on subject B",
		},
		{
			name:      "no messages on subject",
			subject:   "*.Z",
			withError: jetstream.ErrMsgNotFound,
		},
	}

	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s1, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	// same stream, but with allow direct
	s2, err := js.CreateStream(ctx,
		jetstream.StreamConfig{Name: "bar",
			Subjects:    []string{"BAR.*"},
			Description: "desc",
			AllowDirect: true,
		})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(ctx, "BAR.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "BAR.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	for _, test := range tests {
		t.Run(fmt.Sprintf("%s - %s", test.name, "allow direct: false"), func(t *testing.T) {
			msg, err := s1.GetLastMsgForSubject(ctx, test.subject)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(msg.Data) != test.expectedData {
				t.Fatalf("Invalid message data; want: %s; got: %s", test.expectedData, string(msg.Data))
			}
		})
		t.Run(fmt.Sprintf("%s - %s", test.name, "allow direct: true"), func(t *testing.T) {
			msg, err := s2.GetLastMsgForSubject(ctx, test.subject)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if string(msg.Data) != test.expectedData {
				t.Fatalf("Invalid message data; want: %s; got: %s", test.expectedData, string(msg.Data))
			}
		})
	}
}

func TestDeleteMsg(t *testing.T) {
	tests := []struct {
		name      string
		seq       uint64
		withError error
	}{
		{
			name: "delete message",
			seq:  3,
		},
		{
			name:      "msg not found",
			seq:       10,
			withError: jetstream.ErrMsgDeleteUnsuccessful,
		},
	}
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.STREAM.MSG.DELETE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			err = s.DeleteMsg(ctx, test.seq)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			deleteMsg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if !strings.Contains(string(deleteMsg.Data), `"no_erase":true`) {
				t.Fatalf("Expected no_erase on request; got: %q", string(deleteMsg.Data))
			}
			if _, err = s.GetMsg(ctx, test.seq); err == nil || !errors.Is(err, jetstream.ErrMsgNotFound) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgNotFound, err)
			}
		})
	}
}

func TestSecureDeleteMsg(t *testing.T) {
	tests := []struct {
		name      string
		seq       uint64
		withError error
	}{
		{
			name: "delete message",
			seq:  3,
		},
		{
			name:      "msg not found",
			seq:       10,
			withError: jetstream.ErrMsgDeleteUnsuccessful,
		},
	}
	srv := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, srv)
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	defer nc.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}, Description: "desc"})
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	for i := 1; i < 5; i++ {
		if _, err := js.Publish(ctx, "FOO.A", []byte(fmt.Sprintf("msg %d on subject A", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if _, err := js.Publish(ctx, "FOO.B", []byte(fmt.Sprintf("msg %d on subject B", i))); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sub, err := nc.SubscribeSync("$JS.API.STREAM.MSG.DELETE.foo")
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			err = s.SecureDeleteMsg(ctx, test.seq)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			deleteMsg, err := sub.NextMsgWithContext(ctx)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if strings.Contains(string(deleteMsg.Data), `"no_erase":true`) {
				t.Fatalf("Expected no_erase to be set to false on request; got: %q", string(deleteMsg.Data))
			}
			if _, err = s.GetMsg(ctx, test.seq); err == nil || !errors.Is(err, jetstream.ErrMsgNotFound) {
				t.Fatalf("Expected error: %v; got: %v", jetstream.ErrMsgNotFound, err)
			}
		})
	}
}

func TestListConsumers(t *testing.T) {
	tests := []struct {
		name         string
		consumersNum int
		withError    error
	}{
		{
			name:         "list consumers",
			consumersNum: 500,
		},
		{
			name:         "no consumers available",
			consumersNum: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, srv)
			nc, err := nats.Connect(srv.ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()
			s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for i := 0; i < test.consumersNum; i++ {
				_, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			consumersList := s.ListConsumers(ctx)
			consumers := make([]*jetstream.ConsumerInfo, 0)
		Loop:
			for {
				select {
				case s := <-consumersList.Info():
					consumers = append(consumers, s)
				case err := <-consumersList.Err():
					if !errors.Is(err, jetstream.ErrEndOfData) {
						t.Fatalf("Unexpected error: %v", err)
					}
					break Loop
				}
			}
			if len(consumers) != test.consumersNum {
				t.Fatalf("Wrong number of streams; want: %d; got: %d", test.consumersNum, len(consumers))
			}
		})
	}
}

func TestConsumerNames(t *testing.T) {
	tests := []struct {
		name         string
		consumersNum int
		withError    error
	}{
		{
			name:         "list consumer names",
			consumersNum: 500,
		},
		{
			name:         "no consumers available",
			consumersNum: 0,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, srv)
			nc, err := nats.Connect(srv.ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()
			s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			for i := 0; i < test.consumersNum; i++ {
				_, err = s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			consumersList := s.ConsumerNames(ctx)
			consumers := make([]string, 0)
		Loop:
			for {
				select {
				case s := <-consumersList.Name():
					consumers = append(consumers, s)
				case err := <-consumersList.Err():
					if !errors.Is(err, jetstream.ErrEndOfData) {
						t.Fatalf("Unexpected error: %v", err)
					}
					break Loop
				}
			}
			if len(consumers) != test.consumersNum {
				t.Fatalf("Wrong number of streams; want: %d; got: %d", test.consumersNum, len(consumers))
			}
		})
	}
}

func TestPurgeStream(t *testing.T) {
	tests := []struct {
		name        string
		opts        []jetstream.StreamPurgeOpt
		expectedSeq []uint64
		withError   error
	}{
		{
			name:        "purge all messages",
			expectedSeq: []uint64{},
		},
		{
			name:        "purge on subject",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject("FOO.2")},
			expectedSeq: []uint64{1, 3, 5, 7, 9},
		},
		{
			name:        "purge with sequence",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeSequence(5)},
			expectedSeq: []uint64{5, 6, 7, 8, 9, 10},
		},
		{
			name:        "purge with keep",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeKeep(3)},
			expectedSeq: []uint64{8, 9, 10},
		},
		{
			name:        "purge with filter and sequence",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject("FOO.2"), jetstream.WithPurgeSequence(8)},
			expectedSeq: []uint64{1, 3, 5, 7, 8, 9, 10},
		},
		{
			name:        "purge with filter and keep",
			opts:        []jetstream.StreamPurgeOpt{jetstream.WithPurgeSubject("FOO.2"), jetstream.WithPurgeKeep(3)},
			expectedSeq: []uint64{1, 3, 5, 6, 7, 8, 9, 10},
		},
		{
			name:      "with sequence and keep",
			opts:      []jetstream.StreamPurgeOpt{jetstream.WithPurgeSequence(5), jetstream.WithPurgeKeep(3)},
			withError: jetstream.ErrInvalidOption,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			srv := RunBasicJetStreamServer()
			defer shutdownJSServerAndRemoveStorage(t, srv)
			nc, err := nats.Connect(srv.ClientURL())
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()
			js, err := jetstream.New(nc)
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			defer nc.Close()

			s, err := js.CreateStream(ctx, jetstream.StreamConfig{Name: "foo", Subjects: []string{"FOO.*"}})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			for i := 0; i < 5; i++ {
				if _, err := js.Publish(ctx, "FOO.1", []byte(fmt.Sprintf("msg %d on FOO.1", i))); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				if _, err := js.Publish(ctx, "FOO.2", []byte(fmt.Sprintf("msg %d on FOO.2", i))); err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
			}
			err = s.Purge(ctx, test.opts...)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			c, err := s.CreateOrUpdateConsumer(ctx, jetstream.ConsumerConfig{AckPolicy: jetstream.AckExplicitPolicy})
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			seqs := make([]uint64, 0)
		Loop:
			for {
				msgs, err := c.FetchNoWait(1)
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				msg := <-msgs.Messages()
				if msg == nil {
					break Loop
				}
				if err := msgs.Error(); err != nil {
					t.Fatalf("unexpected error during fetch: %v", err)
				}
				meta, err := msg.Metadata()
				if err != nil {
					t.Fatalf("Unexpected error: %v", err)
				}
				seqs = append(seqs, meta.Sequence.Stream)
			}
			if !reflect.DeepEqual(seqs, test.expectedSeq) {
				t.Fatalf("Invalid result; want: %v; got: %v", test.expectedSeq, seqs)
			}
		})
	}
}
