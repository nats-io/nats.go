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

func TestRetryWithBackoff(t *testing.T) {
	tests := []struct {
		name                  string
		givenOpts             backoffOpts
		withError             bool
		timeout               time.Duration
		cancelAfter           time.Duration
		successfulAttemptNum  int
		expectedAttemptsCount int
	}{
		{
			name: "infinite attempts, 5 tries before success",
			givenOpts: backoffOpts{
				attempts:        -1,
				initialInterval: 10 * time.Millisecond,
				maxInterval:     60 * time.Millisecond,
			},
			withError:            false,
			successfulAttemptNum: 5,
			// 0ms + 10ms + 20ms + 40ms + 60ms = 130ms
			timeout:               200 * time.Millisecond,
			expectedAttemptsCount: 5,
		},
		{
			name: "infinite attempts, 5 tries before success, without initial execution",
			givenOpts: backoffOpts{
				attempts:                -1,
				initialInterval:         10 * time.Millisecond,
				disableInitialExecution: true,
				factor:                  2,
				maxInterval:             60 * time.Millisecond,
			},
			withError:            false,
			successfulAttemptNum: 5,
			// 10ms + 20ms + 40ms + 60ms + 60 = 190ms
			timeout:               250 * time.Millisecond,
			expectedAttemptsCount: 5,
		},
		{
			name: "5 attempts, unsuccessful",
			givenOpts: backoffOpts{
				attempts:        5,
				initialInterval: 10 * time.Millisecond,
				factor:          2,
				maxInterval:     60 * time.Millisecond,
			},
			withError: true,
			// 0ms + 10ms + 20ms + 40ms + 60ms = 130ms
			timeout:               200 * time.Millisecond,
			expectedAttemptsCount: 5,
		},
		{
			name: "custom backoff values, should override other settings",
			givenOpts: backoffOpts{
				initialInterval: 2 * time.Second,
				factor:          2,
				maxInterval:     100 * time.Millisecond,
				customBackoff:   []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 30 * time.Millisecond, 40 * time.Millisecond, 50 * time.Millisecond},
			},
			withError:            false,
			successfulAttemptNum: 4,
			// 10ms + 20ms + 30ms + 40ms = 100ms
			timeout:               150 * time.Millisecond,
			expectedAttemptsCount: 4,
		},
		{
			name: "no custom backoff, with cancel",
			givenOpts: backoffOpts{
				attempts:        -1,
				initialInterval: 100 * time.Millisecond,
				factor:          1,
			},
			withError:             false,
			cancelAfter:           150 * time.Millisecond,
			timeout:               200 * time.Millisecond,
			expectedAttemptsCount: 2,
		},
		{
			name: "custom backoff, with cancel",
			givenOpts: backoffOpts{
				customBackoff: []time.Duration{100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
			},
			cancelAfter:           150 * time.Millisecond,
			expectedAttemptsCount: 1,
			timeout:               200 * time.Millisecond,
		},
		{
			name: "attempts num not provided",
			givenOpts: backoffOpts{
				initialInterval: 100 * time.Millisecond,
				factor:          1,
			},
			withError:             true,
			timeout:               1 * time.Second,
			expectedAttemptsCount: 1,
		},
		{
			name: "custom backoff, but attempts num provided",
			givenOpts: backoffOpts{
				customBackoff: []time.Duration{100 * time.Millisecond, 100 * time.Millisecond, 100 * time.Millisecond},
				attempts:      5,
			},
			withError:             true,
			timeout:               1 * time.Second,
			expectedAttemptsCount: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ok := make(chan struct{})
			errs := make(chan error, 1)

			var cancelChan chan struct{}
			if test.cancelAfter != 0 {
				cancelChan = make(chan struct{})
				test.givenOpts.cancel = cancelChan
			}
			var count int
			go func() {
				err := retryWithBackoff(func(attempt int) (bool, error) {
					count = attempt
					if test.successfulAttemptNum != 0 && attempt == test.successfulAttemptNum-1 {
						return false, nil
					}
					return true, fmt.Errorf("error %d", attempt)
				}, test.givenOpts)
				if err != nil {
					errs <- err
					return
				}
				close(ok)
			}()
			if test.cancelAfter > 0 {
				go func() {
					time.Sleep(test.cancelAfter)
					close(cancelChan)
				}()
			}
			select {
			case <-ok:
				if test.withError {
					t.Fatal("Expected error; got nil")
				}
			case err := <-errs:
				if !test.withError {
					t.Fatalf("Unexpected error: %v", err)
				}
			case <-time.After(test.timeout):
				t.Fatalf("Timeout after %v", test.timeout)
			}
			if count != test.expectedAttemptsCount-1 {
				t.Fatalf("Invalid count; want: %d; got: %d", test.expectedAttemptsCount, count)
			}
		})
	}
}

func TestPullConsumer_checkPending(t *testing.T) {

	tests := []struct {
		name                string
		givenSub            *pullSubscription
		fetchInProgress     bool
		shouldSend          bool
		expectedPullRequest *pullRequest
	}{
		{
			name: "msgs threshold not reached, bytes not set, no pull request",
			givenSub: &pullSubscription{
				pending: pendingMsgs{
					msgCount: 10,
				},
				consumeOpts: &consumeOpts{
					ThresholdMessages: 5,
					MaxMessages:       10,
				},
			},
			shouldSend: false,
		},
		{
			name: "pending msgs below threshold, send pull request",
			givenSub: &pullSubscription{
				pending: pendingMsgs{
					msgCount:  4,
					byteCount: 400, // byte count should be ignored
				},
				consumeOpts: &consumeOpts{
					ThresholdMessages: 5,
					MaxMessages:       10,
				},
			},
			shouldSend: true,
			expectedPullRequest: &pullRequest{
				Batch:    6,
				MaxBytes: 0,
			},
		},
		{
			name: "pending msgs below threshold but PR in progress",
			givenSub: &pullSubscription{
				pending: pendingMsgs{
					msgCount: 4,
				},
				consumeOpts: &consumeOpts{
					ThresholdMessages: 5,
					MaxMessages:       10,
				},
			},
			fetchInProgress: true,
			shouldSend:      false,
		},
		{
			name: "pending bytes below threshold, send pull request",
			givenSub: &pullSubscription{
				pending: pendingMsgs{
					byteCount: 400,
					msgCount:  1000000, // msgs count should be ignored
				},
				consumeOpts: &consumeOpts{
					MaxMessages:    1000000,
					ThresholdBytes: 500,
					MaxBytes:       1000,
				},
			},
			shouldSend: true,
			expectedPullRequest: &pullRequest{
				Batch:    1000000,
				MaxBytes: 600,
			},
		},
		{
			name: "pending bytes above threshold, no pull request",
			givenSub: &pullSubscription{
				pending: pendingMsgs{
					byteCount: 600,
				},
				consumeOpts: &consumeOpts{
					ThresholdBytes: 500,
					MaxBytes:       1000,
				},
			},
			shouldSend: false,
		},
		{
			name: "pending bytes below threshold, fetch in progress, no pull request",
			givenSub: &pullSubscription{
				pending: pendingMsgs{
					byteCount: 400,
				},
				consumeOpts: &consumeOpts{
					ThresholdBytes: 500,
					MaxBytes:       1000,
				},
			},
			fetchInProgress: true,
			shouldSend:      false,
		},
		{
			name: "StopAfter set, pending msgs below StopAfter, send pull request",
			givenSub: &pullSubscription{
				pending: pendingMsgs{
					msgCount: 4,
				},
				consumeOpts: &consumeOpts{
					ThresholdMessages: 5,
					MaxMessages:       10,
					StopAfter:         8,
				},
				delivered: 2,
			},
			shouldSend: true,
			expectedPullRequest: &pullRequest{
				Batch:    2, // StopAfter (8) - delivered (2) - pending (4)
				MaxBytes: 0,
			},
		},
		{
			name: "StopAfter set, pending msgs equal to StopAfter, no pull request",
			givenSub: &pullSubscription{
				pending: pendingMsgs{
					msgCount: 6,
				},
				consumeOpts: &consumeOpts{
					ThresholdMessages: 5,
					MaxMessages:       10,
					StopAfter:         6,
				},
				delivered: 0,
			},
			shouldSend: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			prChan := make(chan *pullRequest, 1)
			test.givenSub.fetchNext = prChan
			if test.fetchInProgress {
				test.givenSub.fetchInProgress.Store(1)
			}
			errs := make(chan error, 1)
			ok := make(chan struct{}, 1)
			go func() {
				if test.shouldSend {
					select {
					case pr := <-prChan:
						if *pr != *test.expectedPullRequest {
							errs <- fmt.Errorf("Invalid pull request; want: %#v; got: %#v", test.expectedPullRequest, pr)
							return
						}
						ok <- struct{}{}
					case <-time.After(1 * time.Second):
						errs <- errors.New("Timeout")
						return
					}
				} else {
					select {
					case <-prChan:
						errs <- errors.New("Unexpected pull request")
					case <-time.After(100 * time.Millisecond):
						ok <- struct{}{}
						return
					}
				}
			}()

			test.givenSub.checkPending()
			select {
			case <-ok:
				// ok
			case err := <-errs:
				t.Fatal(err)
			}

		})
	}
}

func TestKV_keyValid(t *testing.T) {
	tests := []struct {
		key string
		ok  bool
	}{
		{key: "foo123", ok: true},
		{key: "foo.bar", ok: true},
		{key: "Foo.123=bar_baz-abc", ok: true},
		{key: "foo.*.bar", ok: false},
		{key: "foo.>", ok: false},
		{key: ">", ok: false},
		{key: "*", ok: false},
		{key: "foo!", ok: false},
		{key: "foo bar", ok: false},
		{key: "", ok: false},
		{key: " ", ok: false},
		{key: ".", ok: false},
		{key: ".foo", ok: false},
		{key: "foo.", ok: false},
	}

	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			res := keyValid(test.key)
			if res != test.ok {
				t.Fatalf("Invalid result; want: %v; got: %v", test.ok, res)
			}
		})
	}
}

func TestKV_searchKeyValid(t *testing.T) {
	tests := []struct {
		key string
		ok  bool
	}{
		{key: "foo123", ok: true},
		{key: "foo.bar", ok: true},
		{key: "Foo.123=bar_baz-abc", ok: true},
		{key: "foo.*.bar", ok: true},
		{key: "foo.>", ok: true},
		{key: ">", ok: true},
		{key: "*", ok: true},
		{key: "foo!", ok: false},
		{key: "foo bar", ok: false},
		{key: "", ok: false},
		{key: " ", ok: false},
		{key: ".", ok: false},
		{key: ".foo", ok: false},
		{key: "foo.", ok: false},
	}

	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			res := searchKeyValid(test.key)
			if res != test.ok {
				t.Fatalf("Invalid result; want: %v; got: %v", test.ok, res)
			}
		})
	}
}

func TestKV_bucketValid(t *testing.T) {
	tests := []struct {
		key string
		ok  bool
	}{
		{key: "foo123", ok: true},
		{key: "Foo123-bar_baz", ok: true},
		{key: "foo.bar", ok: false},
		{key: "foo.*.bar", ok: false},
		{key: "foo.>", ok: false},
		{key: ">", ok: false},
		{key: "*", ok: false},
		{key: "foo!", ok: false},
		{key: "foo bar", ok: false},
		{key: "", ok: false},
		{key: " ", ok: false},
		{key: ".", ok: false},
		{key: ".foo", ok: false},
		{key: "foo.", ok: false},
	}

	for _, test := range tests {
		t.Run(test.key, func(t *testing.T) {
			res := bucketValid(test.key)
			if res != test.ok {
				t.Fatalf("Invalid result; want: %v; got: %v", test.ok, res)
			}
		})
	}
}
