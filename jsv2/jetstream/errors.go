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

	StreamNotFound  ErrorCode = 10059
	StreamNameInUse ErrorCode = 10058

	ConsumerNotFound ErrorCode = 10014

	MessageNotFound ErrorCode = 10037
)

var (
	// JetStream general errors

	// ErrJetStreamNotEnabled is when JetStream is disabled on either server (globally) or on a specific account
	ErrJetStreamNotEnabled = errors.New("nats: jetstream not enabled")
	// ErrEndOfData is returned when iterating over paged API from JetStream reaches end of data
	ErrEndOfData = errors.New("end of data reached")

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
)

// Error prints the API error conde and description
func (e *APIError) Error() string {
	return fmt.Sprintf("API error %d: %s", e.ErrorCode, e.Description)
}
