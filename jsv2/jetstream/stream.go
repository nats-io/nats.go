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
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/textproto"
	"strings"
	"time"

	"github.com/nats-io/nats.go"
)

type (
	// Stream contains CRUD methods on a consumer, as well as operations on an existing stream
	Stream interface {
		streamConsumerManager

		// Info returns stream details
		Info(context.Context, ...StreamInfoOpt) (*nats.StreamInfo, error)
		// CachedInfo returns *nats.StreamInfo cached on a consumer struct
		CachedInfo() *nats.StreamInfo

		// Purge removes messages from a stream
		Purge(context.Context, ...StreamPurgeOpt) error

		// GetMsg retrieves a raw stream message stored in JetStream by sequence number
		GetMsg(context.Context, uint64) (*RawStreamMsg, error)
		// GetLastMsg  retrieves the last raw stream message stored in JetStream by subject
		GetLastMsgForSubject(context.Context, string) (*RawStreamMsg, error)
	}

	RawStreamMsg struct {
		Subject  string
		Sequence uint64
		Header   nats.Header
		Data     []byte
		Time     time.Time
	}

	streamConsumerManager interface {
		// CreateConsumer adds a new consumer to a stream
		CreateConsumer(context.Context, nats.ConsumerConfig) (Consumer, error)
		// UpdateConsumer updates an existing consumer
		UpdateConsumer(context.Context, nats.ConsumerConfig) (Consumer, error)
		// Consumer returns a Consumer interface for an existing consumer
		Consumer(context.Context, string) (Consumer, error)
		// DeleteConsumer removes a consumer
		DeleteConsumer(context.Context, string) error
	}

	stream struct {
		name      string
		info      *nats.StreamInfo
		jetStream *jetStream
	}

	StreamInfoOpt func(*streamInfoRequest) error

	streamInfoRequest struct {
		DeletedDetails bool `json:"deleted_details,omitempty"`
	}

	consumerInfoResponse struct {
		apiResponse
		*nats.ConsumerInfo
	}

	createConsumerRequest struct {
		Stream string               `json:"stream_name"`
		Config *nats.ConsumerConfig `json:"config"`
	}

	StreamPurgeOpt func(*StreamPurgeRequest) error

	StreamPurgeRequest struct {
		// Purge up to but not including sequence.
		Sequence uint64 `json:"seq,omitempty"`
		// Subject to match against messages for the purge command.
		Subject string `json:"filter,omitempty"`
		// Number of messages to keep.
		Keep uint64 `json:"keep,omitempty"`
	}

	streamPurgeResponse struct {
		apiResponse
		Success bool   `json:"success,omitempty"`
		Purged  uint64 `json:"purged"`
	}

	consumerDeleteResponse struct {
		apiResponse
		Success bool `json:"success,omitempty"`
	}

	apiMsgGetRequest struct {
		Seq     uint64 `json:"seq,omitempty"`
		LastFor string `json:"last_by_subj,omitempty"`
	}

	// apiMsgGetResponse is the response for a Stream get request.
	apiMsgGetResponse struct {
		apiResponse
		Message *storedMsg `json:"message,omitempty"`
	}

	// storedMsg is a raw message stored in JetStream.
	storedMsg struct {
		Subject  string    `json:"subject"`
		Sequence uint64    `json:"seq"`
		Header   []byte    `json:"hdrs,omitempty"`
		Data     []byte    `json:"data,omitempty"`
		Time     time.Time `json:"time"`
	}
)

var (
	ErrStreamNotFound     = errors.New("nats: stream not found")
	ErrInvalidDurableName = errors.New("nats: invalid durable name")
	ErrMsgNotFound        = errors.New("nats: message not found")
)

func (s *stream) CreateConsumer(ctx context.Context, cfg nats.ConsumerConfig) (Consumer, error) {
	return upsertConsumer(ctx, s.jetStream, s.name, cfg)
}

func (s *stream) UpdateConsumer(ctx context.Context, cfg nats.ConsumerConfig) (Consumer, error) {
	return upsertConsumer(ctx, s.jetStream, s.name, cfg)
}

func (s *stream) Consumer(ctx context.Context, name string) (Consumer, error) {
	return getConsumer(ctx, s.jetStream, s.name, name)
}

func (s *stream) DeleteConsumer(ctx context.Context, name string) error {
	return deleteConsumer(ctx, s.jetStream, s.name, name)
}

// Info fetches *nats.StreamInfo from server
//
// Available options:
// WithDeletedDetails() - use to display the information about messages deleted from a stream
func (s *stream) Info(ctx context.Context, opts ...StreamInfoOpt) (*nats.StreamInfo, error) {
	var infoReq *streamInfoRequest
	for _, opt := range opts {
		if infoReq == nil {
			infoReq = &streamInfoRequest{}
		}
		if err := opt(infoReq); err != nil {
			return nil, err
		}
	}
	var req []byte
	var err error
	if req != nil {
		req, err = json.Marshal(infoReq)
		if err != nil {
			return nil, err
		}
	}

	infoSubject := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiStreamInfoT, s.name))
	var resp streamInfoResponse

	if _, err = s.jetStream.apiRequestJSON(ctx, infoSubject, &resp, req); err != nil {
		return nil, err
	}
	if resp.Error != nil {
		if resp.Error.Code == 404 {
			return nil, ErrStreamNotFound
		}
		return nil, resp.Error
	}

	return resp.StreamInfo, nil
}

// CachedInfo returns *nats.StreamInfo cached on a stream struct
//
// NOTE: The returned object might not be up to date with the most recent updates on the server
// For up-to-date information, use `Info()`
func (s *stream) CachedInfo() *nats.StreamInfo {
	return s.info
}

// Purge removes messages from a stream
//
// Available options:
// WithSubject() - can be used set a sprecific subject for which messages on a stream will be purged
// WithSequence() - can be used to set a sprecific sequence number up to which (but not including) messages will be purged from a stream
// WithKeep() - can be used to set the number of messages to be kept in the stream after purge.
func (s *stream) Purge(ctx context.Context, opts ...StreamPurgeOpt) error {
	var purgeReq StreamPurgeRequest
	for _, opt := range opts {
		if err := opt(&purgeReq); err != nil {
			return err
		}
	}
	var req []byte
	var err error
	req, err = json.Marshal(purgeReq)
	if err != nil {
		return err
	}

	purgeSubject := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiStreamPurgeT, s.name))

	var resp streamPurgeResponse
	if _, err = s.jetStream.apiRequestJSON(ctx, purgeSubject, &resp, req); err != nil {
		return err
	}
	if resp.Error != nil {
		return resp.Error
	}

	return nil
}

func validateDurableName(dur string) error {
	if strings.Contains(dur, ".") {
		return fmt.Errorf("%w: '%s'", ErrInvalidDurableName, dur)
	}
	return nil
}

func (s *stream) GetMsg(ctx context.Context, seq uint64) (*RawStreamMsg, error) {
	return s.getMsg(ctx, &apiMsgGetRequest{Seq: seq})
}

func (s *stream) GetLastMsgForSubject(ctx context.Context, subject string) (*RawStreamMsg, error) {
	return s.getMsg(ctx, &apiMsgGetRequest{LastFor: subject})
}

func (s *stream) getMsg(ctx context.Context, mreq *apiMsgGetRequest) (*RawStreamMsg, error) {
	req, err := json.Marshal(mreq)
	if err != nil {
		return nil, err
	}

	var resp apiMsgGetResponse
	dsSubj := apiSubj(s.jetStream.apiPrefix, fmt.Sprintf(apiMsgGetT, s.name))
	_, err = s.jetStream.apiRequestJSON(ctx, dsSubj, req)
	if err != nil {
		return nil, err
	}

	if resp.Error != nil {
		if resp.Error.Code == 404 && strings.Contains(resp.Error.Description, "message") {
			return nil, ErrMsgNotFound
		}
		return nil, resp.Error
	}

	msg := resp.Message

	var hdr nats.Header
	if len(msg.Header) > 0 {
		hdr, err = decodeHeadersMsg(msg.Header)
		if err != nil {
			return nil, err
		}
	}

	return &RawStreamMsg{
		Subject:  msg.Subject,
		Sequence: msg.Sequence,
		Header:   hdr,
		Data:     msg.Data,
		Time:     msg.Time,
	}, nil
}

// decodeHeadersMsg will decode and headers.
func decodeHeadersMsg(data []byte) (nats.Header, error) {
	tp := textproto.NewReader(bufio.NewReader(bytes.NewReader(data)))
	l, err := tp.ReadLine()
	if err != nil || len(l) < hdrPreEnd || l[:hdrPreEnd] != hdrLine[:hdrPreEnd] {
		return nil, ErrBadHeaderMsg
	}

	mh, err := readMIMEHeader(tp)
	if err != nil {
		return nil, err
	}

	// Check if we have an inlined status.
	if len(l) > hdrPreEnd {
		var description string
		status := strings.TrimSpace(l[hdrPreEnd:])
		if len(status) != statusLen {
			description = strings.TrimSpace(status[statusLen:])
			status = status[:statusLen]
		}
		mh.Add(statusHdr, status)
		if len(description) > 0 {
			mh.Add(descrHdr, description)
		}
	}
	return nats.Header(mh), nil
}

// readMIMEHeader returns a MIMEHeader that preserves the
// original case of the MIME header, based on the implementation
// of textproto.ReadMIMEHeader.
//
// https://golang.org/pkg/net/textproto/#Reader.ReadMIMEHeader
func readMIMEHeader(tp *textproto.Reader) (textproto.MIMEHeader, error) {
	m := make(textproto.MIMEHeader)
	for {
		kv, err := tp.ReadLine()
		if len(kv) == 0 {
			return m, err
		}

		// Process key fetching original case.
		i := bytes.IndexByte([]byte(kv), ':')
		if i < 0 {
			return nil, ErrBadHeaderMsg
		}
		key := kv[:i]
		if key == "" {
			// Skip empty keys.
			continue
		}
		i++
		for i < len(kv) && (kv[i] == ' ' || kv[i] == '\t') {
			i++
		}
		value := string(kv[i:])
		m[key] = append(m[key], value)
		if err != nil {
			return m, err
		}
	}
}
