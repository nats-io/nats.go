// Copyright 2012-2017 Apcera Inc. All rights reserved.

// +build go1.7

// A Go client for the NATS messaging system (https://nats.io).
package nats

import "context"

// RequestWithContext takes a context, a subject and payload in bytes
// and request expecting a single response.
func (nc *Conn) RequestWithContext(
	ctx context.Context,
	subj string,
	data []byte,
) (*Msg, error) {
	inbox := NewInbox()
	ch := make(chan *Msg, RequestChanLen)
	recvCh := make(chan *Msg, 1)
	var recvMsg *Msg

	s, err := nc.subscribe(inbox, _EMPTY_, func(msg *Msg) {
		recvCh <- msg
	}, ch)

	if err != nil {
		// If we errored here but context has been canceled
		// then still return the error from context.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		return nil, err
	}
	s.AutoUnsubscribe(1)
	defer s.Unsubscribe()
	defer close(ch)

	err = nc.PublishRequest(subj, inbox, data)
	if err != nil {
		// Still prefer error from context
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		return nil, err
	}

	select {
	case <-ctx.Done():
		// Context has been canceled so return opaque error
		return nil, ctx.Err()
	case recvMsg = <-recvCh:
		break
	}
	return recvMsg, nil
}

// SetContext makes the subscription be aware of context cancellation.
func (s *Subscription) SetContext(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ctx = ctx
}

// NextMsgWithContext takes a context and returns the next message available
// to a synchronous subscriber or block until one is available until context
// gets canceled.
func (s *Subscription) NextMsgWithContext(ctx context.Context) (*Msg, error) {
	s.SetContext(ctx)

	// Call NextMsg from subscription but disabling the timeout
	// as we rely on the context for the cancellation instead.
	msg, err := s.NextMsg(0)
	if err != nil {
		// Also prefer error from context in case it has occurred.
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
	}

	return msg, err
}
