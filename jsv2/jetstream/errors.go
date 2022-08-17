// Copyright 2020-2022 The NATS Authors
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
)

type (
	// APIError is included in all API responses if there was an error.
	APIError struct {
		Code        int       `json:"code"`
		ErrorCode   ErrorCode `json:"err_code"`
		Description string    `json:"description,omitempty"`
	}

	// ErrorCode represents `error_code` returned in response from JetStream API
	ErrorCode uint16
)

const (
	JetStreamNotEnabledForAccount ErrorCode = 10039
	JetStreamNotEnabled           ErrorCode = 10076

	StreamNotFound  ErrorCode = 10059
	StreamNameInUse ErrorCode = 10058

	ConsumerNotFound ErrorCode = 10014

	MessageNotFound ErrorCode = 10037
)

var (
	// JetStream general errors

	// ErrJetStreamNotEnabled is when JetStream is disabled on server (globally)
	ErrJetStreamNotEnabled = errors.New("nats: jetstream not enabled")
	// ErrJetStreamNotEnabledForAccount is when JetStream is disabled for the account
	ErrJetStreamNotEnabledForAccount = errors.New("nats: jetstream not enabled")
	// ErrEndOfData is returned when iterating over paged API from JetStream reaches end of data
	ErrEndOfData = errors.New("nats: end of data reached")

	// Stream errors

	// ErrStreamNameAlreadyInUse is returned when stream with given name already exists
	ErrStreamNameAlreadyInUse = errors.New("nats: stream name already in use")
	// ErrStreamNameRequired is returned when the provided stream name is empty
	ErrStreamNameRequired = errors.New("nats: stream name is required")
	// ErrInvalidStreamName is returned when provided stream name is invalid
	ErrInvalidStreamName = errors.New("nats: invalid stream name")
	// ErrStreamNotFound is returned when stream with provided name does not exist
	ErrStreamNotFound = errors.New("nats: stream not found")

	// Consumer errors

	// ErrInvalidDurableName is returned when the provided durable name for the consumer is invalid
	ErrInvalidDurableName = errors.New("nats: invalid durable name")
	// ErrConsumerExists is returned when consumer with provided name already exists on the server
	ErrConsumerExists = errors.New("nats: consumer with given name already exists")
	// ErrNoMessages is returned when no messages are currectly available for a consumer
	ErrNoMessages = errors.New("nats: no messages")
	// ErrConsumerNotFound is returned when a consumer with given name is not found
	ErrConsumerNotFound = errors.New("nats: consumer not found")
	// ErrHandlerRequired is returned when no handler func is provided in Stream()
	ErrHandlerRequired = errors.New("nats: handler cannot be empty")
	// ErrNoHeartbeat is received when no message is received in IdleHeartbeat time (if set)
	ErrNoHeartbeat = errors.New("nats: no heartbeat received, canceling subscription")
	// ErrConsumerHasActiveSubscription is returned when a consumer is already subscribed to a stream
	ErrConsumerHasActiveSubscription = errors.New("nats: consumer has active subscription")

	// Message errors

	// ErrNotJSMessage is returned when a message is not from JetStream (does not contain valid metadata)
	ErrNotJSMessage = errors.New("nats: not a jetstream message")
	// ErrMsgAlreadyAckd is returned on an attempt to ack a message which was previously acknowledged
	ErrMsgAlreadyAckd = errors.New("nats: message was already acknowledged")
	// ErrMsgNotBound is returned when given message is not bound to any subscription
	ErrMsgNotBound = errors.New("nats: message is not bound to subscription/connection")
	// ErrMsgNoReply is returned when attempting to reply to a message without a reply subject
	ErrMsgNoReply = errors.New("nats: message does not have a reply")
	// ErrMsgDeleteUnsuccessful is returned when an attempt to delete a message is unsuccessful
	ErrMsgDeleteUnsuccessful = errors.New("nats: message deletion unsuccessful")
	// ErrMsgNotFound is returned when a message with provided sequence does not exist on stream
	ErrMsgNotFound = errors.New("nats: message not found")
	// ErrInvalidJSAck is returned when the ack response from server is invalid
	ErrInvalidJSAck = errors.New("nats: invalid jetstream publish response")
	// ErrNoStreamResponse is retured when stream does not respond with ack after the defined retry attempts are reached
	ErrNoStreamResponse = errors.New("nats: no response from stream")
	// ErrAsyncPublishReplySubjectSet is returned when reply subject is set on async message publish
	ErrAsyncPublishReplySubjectSet = errors.New("nats: reply subject should be empty")
	// ErrTooManyStalledMsgs is returned when too many outstanding async messages are waiting for ack
	ErrTooManyStalledMsgs = errors.New("nats: stalled with too many outstanding async published messages")
)

// Error prints the API error conde and description
func (e *APIError) Error() string {
	return fmt.Sprintf("nats: API error %d: %s", e.ErrorCode, e.Description)
}
