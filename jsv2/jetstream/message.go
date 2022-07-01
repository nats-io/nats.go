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
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jsv2/parser"
)

type (
	JetStreamMsg interface {
		JetStreamMessageReader
		Ack(...AckOpt) error
		DoubleAck(context.Context, ...AckOpt) error
		Nak(...NakOpt) error
		InProgress(...AckOpt) error
		Term(...AckOpt) error
	}

	JetStreamMessageReader interface {
		Metadata() (*nats.MsgMetadata, error)
		Data() []byte
		Headers() nats.Header
		Subject() string
	}

	jetStreamMsg struct {
		msg  *nats.Msg
		ackd bool
		js   *jetStream
		sync.Mutex
	}

	ackOpts struct {
		nakDelay time.Duration
		ctx      context.Context
	}

	AckOpt func(*ackOpts) error

	NakOpt func(*ackOpts) error

	ackType []byte
)

const (
	noResponders = "503"
	noMessages   = "404"
	reqTimeout   = "408"
	controlMsg   = "100"
)

const (
	MsgIDHeader               = "Nats-Msg-Id"
	ExpectedStreamHeader      = "Nats-Expected-Stream"
	ExpectedLastSeqHeader     = "Nats-Expected-Last-Sequence"
	ExpectedLastSubjSeqHeader = "Nats-Expected-Last-Subject-Sequence"
	ExpectedLastMsgIDHeader   = "Nats-Expected-Last-Msg-Id"
	MsgRollup                 = "Nats-Rollup"
)

// Rollups, can be subject only or all messages.
const (
	MsgRollupSubject = "sub"
	MsgRollupAll     = "all"
)

var (
	ErrNotJSMessage   = errors.New("nats: not a jetstream message")
	ErrMsgAlreadyAckd = errors.New("nats: message was already acknowledged")
	ErrMsgNotBound    = errors.New("nats: message is not bound to subscription/connection")
	ErrMsgNoReply     = errors.New("nats: message does not have a reply")
	ErrBadHeaderMsg   = errors.New("nats: message could not decode headers")
)

var (
	ackAck      ackType = []byte("+ACK")
	ackNak      ackType = []byte("-NAK")
	ackProgress ackType = []byte("+WPI")
	ackTerm     ackType = []byte("+TERM")
)

func (m *jetStreamMsg) Metadata() (*nats.MsgMetadata, error) {
	if err := m.checkReply(); err != nil {
		return nil, err
	}

	tokens, err := parser.GetMetadataFields(m.msg.Reply)
	if err != nil {
		return nil, fmt.Errorf("%w: %s", ErrNotJSMessage, err)
	}

	meta := &nats.MsgMetadata{
		Domain:       tokens[parser.AckDomainTokenPos],
		NumDelivered: uint64(parser.ParseNum(tokens[parser.AckNumDeliveredTokenPos])),
		NumPending:   uint64(parser.ParseNum(tokens[parser.AckNumPendingTokenPos])),
		Timestamp:    time.Unix(0, parser.ParseNum(tokens[parser.AckTimestampSeqTokenPos])),
		Stream:       tokens[parser.AckStreamTokenPos],
		Consumer:     tokens[parser.AckConsumerTokenPos],
	}
	meta.Sequence.Stream = uint64(parser.ParseNum(tokens[parser.AckStreamSeqTokenPos]))
	meta.Sequence.Consumer = uint64(parser.ParseNum(tokens[parser.AckConsumerSeqTokenPos]))
	return meta, nil
}

func (m *jetStreamMsg) Data() []byte {
	return m.msg.Data
}

func (m *jetStreamMsg) Headers() nats.Header {
	return m.msg.Header
}

func (m *jetStreamMsg) Subject() string {
	return m.msg.Subject
}

func (m *jetStreamMsg) Ack(opts ...AckOpt) error {
	return m.ackReply(ackAck, false, ackOpts{})
}

func (m *jetStreamMsg) DoubleAck(ctx context.Context, opts ...AckOpt) error {
	o := ackOpts{
		ctx: ctx,
	}
	return m.ackReply(ackAck, true, o)
}

func (m *jetStreamMsg) Nak(opts ...NakOpt) error {
	var o ackOpts
	for _, opt := range opts {
		if err := opt(&o); err != nil {
			return err
		}
	}
	return m.ackReply(ackNak, false, o)
}

func (m *jetStreamMsg) InProgress(_ ...AckOpt) error {
	return m.ackReply(ackProgress, false, ackOpts{})
}

func (m *jetStreamMsg) Term(_ ...AckOpt) error {
	return m.ackReply(ackTerm, false, ackOpts{})
}

func (m *jetStreamMsg) ackReply(ackType ackType, sync bool, opts ackOpts) error {
	err := m.checkReply()
	if err != nil {
		return err
	}

	m.Lock()
	if m.ackd {
		return ErrMsgAlreadyAckd
	}
	m.Unlock()

	var hasDeadline bool
	if opts.ctx != nil {
		_, hasDeadline = opts.ctx.Deadline()
	}
	if sync && !hasDeadline {
		return fmt.Errorf("for synchronous acknowledgements, context with deadline has to be provided")
	}
	if opts.nakDelay != 0 && !bytes.Equal(ackType, ackNak) {
		return fmt.Errorf("delay can only be set for NAK")
	}

	var body []byte
	if opts.nakDelay > 0 {
		body = []byte(fmt.Sprintf("%s {\"delay\": %d}", ackType, opts.nakDelay.Nanoseconds()))
	} else {
		body = ackType
	}

	if sync {
		_, err = m.js.conn.RequestWithContext(opts.ctx, m.msg.Reply, body)
	} else {
		err = m.js.conn.Publish(m.msg.Reply, body)
	}
	if err != nil {
		return err
	}

	// Mark that the message has been acked unless it is ackProgress
	// which can be sent many times.
	if !bytes.Equal(ackType, ackProgress) {
		m.Lock()
		m.ackd = true
		m.Unlock()
	}
	return nil
}

func (m *jetStreamMsg) checkReply() error {
	if m == nil || m.msg.Sub == nil {
		return ErrMsgNotBound
	}
	if m.msg.Reply == "" {
		return ErrMsgNoReply
	}
	return nil
}

// Returns if the given message is a user message or not, and if
// `checkSts` is true, returns appropriate error based on the
// content of the status (404, etc..)
func checkMsg(msg *nats.Msg) (bool, error) {
	// If payload or no header, consider this a user message
	if len(msg.Data) > 0 || len(msg.Header) == 0 {
		return true, nil
	}
	// Look for status header
	val := msg.Header.Get("Status")
	// If not present, then this is considered a user message
	if val == "" {
		return true, nil
	}

	switch val {
	case noResponders:
		return false, nats.ErrNoResponders
	case noMessages:
		// 404 indicates that there are no messages.
		return false, ErrNoMessages
	case reqTimeout:
		return false, nats.ErrTimeout
	case controlMsg:
		return false, nil
	}
	return false, fmt.Errorf("nats: %s", msg.Header.Get("Description"))
}

// toJSMsg converts core `nats.Msg` to `jetStreamMsg`, wxposing JetStream-specific operations
func (js *jetStream) toJSMsg(msg *nats.Msg) *jetStreamMsg {
	return &jetStreamMsg{
		msg:   msg,
		js:    js,
		Mutex: sync.Mutex{},
	}
}
