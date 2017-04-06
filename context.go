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
