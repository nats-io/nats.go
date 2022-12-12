package service

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

type (
	Request struct {
		*nats.Msg
		errResponse bool
	}

	// RequestHandler is a function used as a Handler for a service.
	RequestHandler func(*Request)
)

var (
	ErrRespond         = errors.New("NATS error when sending response")
	ErrMarshalResponse = errors.New("marshaling response")
	ErrArgRequired     = errors.New("argument required")
)

func (r *Request) Respond(response []byte) error {
	if err := r.Msg.Respond(response); err != nil {
		return fmt.Errorf("%w: %s", ErrRespond, err)
	}

	return nil
}

func (r *Request) RespondJSON(response interface{}) error {
	resp, err := json.Marshal(response)
	if err != nil {
		return ErrMarshalResponse
	}

	return r.Respond(resp)
}

// Error prepares and publishes error response from a handler.
// A response error should be set containing an error code and description.
func (r *Request) Error(code, description string) error {
	if code == "" {
		return fmt.Errorf("%w: error code", ErrArgRequired)
	}
	if description == "" {
		return fmt.Errorf("%w: description", ErrArgRequired)
	}
	response := &nats.Msg{
		Header: nats.Header{
			ErrorHeader:     []string{description},
			ErrorCodeHeader: []string{code},
		},
	}
	r.errResponse = true
	return r.RespondMsg(response)
}
