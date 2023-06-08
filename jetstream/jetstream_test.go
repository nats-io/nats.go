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

package jetstream

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestMessageMetadata(t *testing.T) {
	tests := []struct {
		name             string
		givenReply       string
		expectedMetadata MsgMetadata
		withError        error
	}{
		{
			name:       "valid metadata",
			givenReply: "$JS.ACK.domain.hash-123.stream.cons.5.10.20.123456789.1.token",
			expectedMetadata: MsgMetadata{
				Sequence: SequencePair{
					Consumer: 20,
					Stream:   10,
				},
				NumDelivered: 5,
				NumPending:   1,
				Timestamp:    time.Unix(0, 123456789),
				Stream:       "stream",
				Consumer:     "cons",
				Domain:       "domain",
			},
		},
		{
			name:       "no reply subject",
			givenReply: "",
			withError:  ErrMsgNoReply,
		},
		{
			name:       "not a JetStream message",
			givenReply: "ABC",
			withError:  ErrNotJSMessage,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			msg := &jetStreamMsg{
				msg: &nats.Msg{
					Reply: test.givenReply,
					Sub:   &nats.Subscription{},
				},
			}
			res, err := msg.Metadata()
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
			if *res != test.expectedMetadata {
				t.Fatalf("Invalid metadata; want: %v; got: %v", test.expectedMetadata, res)
			}
		})
	}
}

func TestValidateSubject(t *testing.T) {
	tests := []struct {
		subject   string
		withError bool
	}{
		{"test.A", false},
		{"test.*", false},
		{"*", false},
		{"*.*", false},
		{"test.*.A", false},
		{"test.>", false},
		{">", false},
		{">.", true},
		{"test.>.A", true},
		{"", true},
		{"test A", true},
	}

	for _, test := range tests {
		tName := fmt.Sprintf("subj=%s,err=%t", test.subject, test.withError)
		t.Run(tName, func(t *testing.T) {
			err := validateSubject(test.subject)
			if test.withError {
				if err == nil {
					t.Fatal("Expected error; got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}
		})
	}
}
