// Copyright 2022 The NATS Authors
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

package micro

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/nats-io/nats.go"
)

type (
	Request struct {
		*nats.Msg
	}

	// RequestHandler is a function used as a Handler for a service.
	// It takes a request, which contains the data (payload and headers) of the request,
	// as well as exposes methods to respond to the request.
	//
	// RequestHandler returns an error - if returned, the request will be accounted form in stats (in num_requests),
	// and last_error will be set with the value.
	RequestHandler func(*Request) error
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
// Optionally, data can be set as response payload.
func (r *Request) Error(code, description string, data []byte) error {
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
	response.Data = data
	return r.RespondMsg(response)
}
