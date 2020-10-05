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

package nats

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

func (nc *Conn) createOrUpdateConsumer(opts *jsOpts) (*ConsumerInfo, error) {
	if opts.streamName == "" {
		return nil, ErrStreamNameRequired
	}
	if opts.consumer == nil {
		return nil, ErrConsumerConfigRequired
	}

	crj, err := json.Marshal(&JSApiConsumerCreateRequest{
		Stream: opts.streamName,
		Config: *opts.consumer,
	})
	if err != nil {
		return nil, err
	}

	ctx, cancel := opts.context(nc.Opts.JetStreamTimeout)
	defer cancel()

	var subj string
	switch len(opts.consumer.Durable) {
	case 0:
		subj = fmt.Sprintf(JSApiConsumerCreateT, opts.streamName)
	default:
		subj = fmt.Sprintf(JSApiDurableCreateT, opts.streamName, opts.consumer.Durable)
	}

	resp, err := nc.RequestWithContext(ctx, subj, crj)
	if err != nil {
		return nil, err
	}

	cresp := &JSApiConsumerCreateResponse{}
	err = json.Unmarshal(resp.Data, cresp)
	if err != nil {
		return nil, err
	}

	if cresp.IsError() {
		return nil, cresp.ToError()
	}

	return cresp.ConsumerInfo, nil
}

const (
	JSApiConsumerCreateT = "$JS.API.CONSUMER.CREATE.%s"
	JSApiDurableCreateT  = "$JS.API.CONSUMER.DURABLE.CREATE.%s.%s"
)

type ApiError struct {
	Code        int    `json:"code"`
	Description string `json:"description,omitempty"`
}

// Error implements error
func (e ApiError) Error() string {
	switch {
	case e.Description == "" && e.Code == 0:
		return "unknown JetStream Error"
	case e.Description == "" && e.Code > 0:
		return fmt.Sprintf("unknown JetStream %d Error", e.Code)
	default:
		return e.Description
	}
}

// NotFoundError is true when the error is one about a resource not found
func (e ApiError) NotFoundError() bool { return e.Code == 404 }

// ServerError is true when the server returns a 5xx error code
func (e ApiError) ServerError() bool { return e.Code >= 500 && e.Code < 600 }

// UserError is true when the server returns a 4xx error code
func (e ApiError) UserError() bool { return e.Code >= 400 && e.Code < 500 }

// ErrorCode is the JetStream error code
func (e ApiError) ErrorCode() int { return e.Code }

type JSApiResponse struct {
	Type  string    `json:"type"`
	Error *ApiError `json:"error,omitempty"`
}

// ToError extracts a standard error from a JetStream response
func (r JSApiResponse) ToError() error {
	if r.Error == nil {
		return nil
	}

	return *r.Error
}

// IsError determines if a standard JetStream API response is a error
func (r JSApiResponse) IsError() bool {
	return r.Error != nil
}

// io.nats.jetstream.api.v1.consumer_create_request
type JSApiConsumerCreateRequest struct {
	Stream string         `json:"stream_name"`
	Config ConsumerConfig `json:"config"`
}

// io.nats.jetstream.api.v1.consumer_create_response
type JSApiConsumerCreateResponse struct {
	JSApiResponse
	*ConsumerInfo
}

type AckPolicy int

const (
	AckNone AckPolicy = iota
	AckAll
	AckExplicit
)

func (p AckPolicy) String() string {
	switch p {
	case AckNone:
		return "None"
	case AckAll:
		return "All"
	case AckExplicit:
		return "Explicit"
	default:
		return "Unknown Acknowledgement Policy"
	}
}

func (p *AckPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString("none"):
		*p = AckNone
	case jsonString("all"):
		*p = AckAll
	case jsonString("explicit"):
		*p = AckExplicit
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}

	return nil
}

func (p AckPolicy) MarshalJSON() ([]byte, error) {
	switch p {
	case AckNone:
		return json.Marshal("none")
	case AckAll:
		return json.Marshal("all")
	case AckExplicit:
		return json.Marshal("explicit")
	default:
		return nil, fmt.Errorf("unknown acknowlegement policy %v", p)
	}
}

type ReplayPolicy int

const (
	ReplayInstant ReplayPolicy = iota
	ReplayOriginal
)

func (p ReplayPolicy) String() string {
	switch p {
	case ReplayInstant:
		return "Instant"
	case ReplayOriginal:
		return "Original"
	default:
		return "Unknown Replay Policy"
	}
}

func (p *ReplayPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString("instant"):
		*p = ReplayInstant
	case jsonString("original"):
		*p = ReplayOriginal
	default:
		return fmt.Errorf("can not unmarshal %q", data)
	}

	return nil
}

func (p ReplayPolicy) MarshalJSON() ([]byte, error) {
	switch p {
	case ReplayOriginal:
		return json.Marshal("original")
	case ReplayInstant:
		return json.Marshal("instant")
	default:
		return nil, fmt.Errorf("unknown replay policy %v", p)
	}
}

var (
	AckAck      = []byte("+ACK")
	AckNak      = []byte("-NAK")
	AckProgress = []byte("+WPI")
	AckNext     = []byte("+NXT")
	AckTerm     = []byte("+TERM")
)

type DeliverPolicy int

const (
	DeliverAll DeliverPolicy = iota
	DeliverLast
	DeliverNew
	DeliverByStartSequence
	DeliverByStartTime
)

func (p DeliverPolicy) String() string {
	switch p {
	case DeliverAll:
		return "All"
	case DeliverLast:
		return "Last"
	case DeliverNew:
		return "New"
	case DeliverByStartSequence:
		return "By Start Sequence"
	case DeliverByStartTime:
		return "By Start Time"
	default:
		return "Unknown Deliver Policy"
	}
}

func (p *DeliverPolicy) UnmarshalJSON(data []byte) error {
	switch string(data) {
	case jsonString("all"), jsonString("undefined"):
		*p = DeliverAll
	case jsonString("last"):
		*p = DeliverLast
	case jsonString("new"):
		*p = DeliverNew
	case jsonString("by_start_sequence"):
		*p = DeliverByStartSequence
	case jsonString("by_start_time"):
		*p = DeliverByStartTime
	}

	return nil
}

func (p DeliverPolicy) MarshalJSON() ([]byte, error) {
	switch p {
	case DeliverAll:
		return json.Marshal("all")
	case DeliverLast:
		return json.Marshal("last")
	case DeliverNew:
		return json.Marshal("new")
	case DeliverByStartSequence:
		return json.Marshal("by_start_sequence")
	case DeliverByStartTime:
		return json.Marshal("by_start_time")
	default:
		return nil, fmt.Errorf("unknown deliver policy %v", p)
	}
}

// ConsumerConfig is the configuration for a JetStream consumes
//
// NATS Schema Type io.nats.jetstream.api.v1.consumer_configuration
type ConsumerConfig struct {
	Durable         string        `json:"durable_name,omitempty"`
	DeliverSubject  string        `json:"deliver_subject,omitempty"`
	DeliverPolicy   DeliverPolicy `json:"deliver_policy"`
	OptStartSeq     uint64        `json:"opt_start_seq,omitempty"`
	OptStartTime    *time.Time    `json:"opt_start_time,omitempty"`
	AckPolicy       AckPolicy     `json:"ack_policy"`
	AckWait         time.Duration `json:"ack_wait,omitempty"`
	MaxDeliver      int           `json:"max_deliver,omitempty"`
	FilterSubject   string        `json:"filter_subject,omitempty"`
	ReplayPolicy    ReplayPolicy  `json:"replay_policy"`
	SampleFrequency string        `json:"sample_freq,omitempty"`
	RateLimit       uint64        `json:"rate_limit_bps,omitempty"`
}

type SequencePair struct {
	ConsumerSeq uint64 `json:"consumer_seq"`
	StreamSeq   uint64 `json:"stream_seq"`
}

type ConsumerInfo struct {
	Stream         string         `json:"stream_name"`
	Name           string         `json:"name"`
	Config         ConsumerConfig `json:"config"`
	Created        time.Time      `json:"created"`
	Delivered      SequencePair   `json:"delivered"`
	AckFloor       SequencePair   `json:"ack_floor"`
	NumPending     int            `json:"num_pending"`
	NumRedelivered int            `json:"num_redelivered"`
}

func jsonString(s string) string {
	return "\"" + s + "\""
}

func isValidJSName(n string) bool {
	return !(n == "" || strings.ContainsAny(n, ">*. "))
}
