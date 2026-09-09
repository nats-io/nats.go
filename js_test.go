// Copyright 2012-2026 The NATS Authors
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

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestIsWrongLastSeqErr(t *testing.T) {
	// 10164 is the replicated-stream variant of the 10071 "wrong last
	// sequence" CAS conflict; both must be recognized (issue #2097).
	for _, code := range []ErrorCode{JSErrCodeStreamWrongLastSequence, JSErrCodeStreamWrongLastSequenceConstant} {
		if !isWrongLastSeqErr(&APIError{Code: 400, ErrorCode: code}) {
			t.Fatalf("err code %d should be recognized as wrong-last-sequence", code)
		}
	}
	if isWrongLastSeqErr(&APIError{Code: 404, ErrorCode: JSErrCodeStreamNotFound}) {
		t.Fatal("unrelated error code should not be recognized")
	}
}

func TestMapRevisionMismatch(t *testing.T) {
	// Both 10071 and its replicated-stream variant 10164 must map to
	// ErrKeyRevisionMismatch, while preserving the underlying
	// APIError for callers that inspect the code.
	for _, code := range []ErrorCode{JSErrCodeStreamWrongLastSequence, JSErrCodeStreamWrongLastSequenceConstant} {
		err := mapRevisionMismatch(&APIError{Code: 400, ErrorCode: code})
		if !errors.Is(err, ErrKeyRevisionMismatch) {
			t.Fatalf("err code %d should map to ErrKeyRevisionMismatch, got: %v", code, err)
		}
		var apiErr *APIError
		if !errors.As(err, &apiErr) || apiErr.ErrorCode != code {
			t.Fatalf("underlying APIError (code %d) should be preserved, got: %v", code, err)
		}
	}

	// Unrelated errors pass through untouched.
	other := &APIError{Code: 404, ErrorCode: JSErrCodeStreamNotFound}
	if err := mapRevisionMismatch(other); err != other || errors.Is(err, ErrKeyRevisionMismatch) {
		t.Fatalf("unrelated error should pass through unchanged, got: %v", err)
	}

	if mapRevisionMismatch(nil) != nil {
		t.Fatal("nil should map to nil")
	}
}

func TestJetStreamConvertDirectMsgResponseToMsg(t *testing.T) {
	// This test checks the conversion of a "direct get message" response
	// to a JS message based on the content of specific NATS headers.
	// It is very specific to the order headers retrieval is made in
	// convertDirectGetMsgResponseToMsg(), so it may need adjustment
	// if changes are made there.

	msg := NewMsg("inbox")

	check := func(errTxt string) {
		t.Helper()
		m, err := convertDirectGetMsgResponseToMsg("test", msg)
		if err == nil || !strings.Contains(err.Error(), errTxt) {
			t.Fatalf("Expected error contain %q, got %v", errTxt, err)
		}
		if m != nil {
			t.Fatalf("Expected nil message, got %v", m)
		}
	}

	check("should have headers")

	msg.Header.Set(statusHdr, noMessagesSts)
	check(ErrMsgNotFound.Error())

	msg.Header.Set(statusHdr, reqTimeoutSts)
	check("unable to get message")

	msg.Header.Set(descrHdr, "some error text")
	check("some error text")

	msg.Header.Del(statusHdr)
	msg.Header.Del(descrHdr)
	msg.Header.Set("some", "header")
	check("missing stream")

	msg.Header.Set(JSStream, "test")
	check("missing sequence")

	msg.Header.Set(JSSequence, "abc")
	check("invalid sequence")

	msg.Header.Set(JSSequence, "1")
	check("missing timestamp")

	msg.Header.Set(JSTimeStamp, "aaaaaaaaa bbbbbbbbbbbb cccccccccc ddddddddddd eeeeeeeeee ffffff")
	check("invalid timestamp")

	msg.Header.Set(JSTimeStamp, "2006-01-02 15:04:05.999999999 +0000 UTC")
	check("missing subject")

	msg.Header.Set(JSSubject, "foo")
	r, err := convertDirectGetMsgResponseToMsg("test", msg)
	if err != nil {
		t.Fatalf("Error during convert: %v", err)
	}
	if r.Subject != "foo" {
		t.Fatalf("Expected subject to be 'foo', got %q", r.Subject)
	}
	if r.Sequence != 1 {
		t.Fatalf("Expected sequence to be 1, got %v", r.Sequence)
	}
	if r.Time.UnixNano() != 0xFC4A4D639917BFF {
		t.Fatalf("Invalid timestamp: %v", r.Time.UnixNano())
	}
	if r.Header.Get("some") != "header" {
		t.Fatalf("Wrong header: %v", r.Header)
	}
}

func TestApplyNewSIDUnregisteredSub(t *testing.T) {
	newConn := func() (*Conn, *Subscription) {
		nc := &Conn{subs: make(map[int64]*Subscription)}
		sub := &Subscription{conn: nc}
		nc.ssid++
		sub.sid = nc.ssid
		nc.subs[sub.sid] = sub
		return nc, sub
	}

	t.Run("registered sub is re-keyed", func(t *testing.T) {
		nc, sub := newConn()
		sub.mu.Lock()
		osid, nsid, ok := sub.applyNewSID()
		sub.mu.Unlock()
		if !ok {
			t.Fatal("expected the sid swap to succeed for a registered sub")
		}
		if osid != 1 || nsid != 2 {
			t.Fatalf("expected sids 1 -> 2, got %d -> %d", osid, nsid)
		}
		if sub.sid != nsid || nc.subs[nsid] != sub || len(nc.subs) != 1 {
			t.Fatalf("sub not registered under the new sid only: sid=%d subs=%v", sub.sid, nc.subs)
		}
	})

	t.Run("removed sub is not re-registered", func(t *testing.T) {
		// Simulates removeSub winning the race: the sub was unsubscribed
		// while applyNewSID had released sub.mu.
		nc, sub := newConn()
		delete(nc.subs, sub.sid)
		sub.mu.Lock()
		_, _, ok := sub.applyNewSID()
		sub.mu.Unlock()
		if ok {
			t.Fatal("expected the sid swap to be refused for an unregistered sub")
		}
		if len(nc.subs) != 0 {
			t.Fatalf("unsubscribed sub was resurrected in the subs map: %v", nc.subs)
		}
		if sub.sid != 1 {
			t.Fatalf("sid of an unregistered sub should be untouched, got %d", sub.sid)
		}
	})

	t.Run("closed connection does not panic", func(t *testing.T) {
		// close() sets nc.subs to nil, and writing to a nil map panics.
		nc, sub := newConn()
		nc.subs = nil
		sub.mu.Lock()
		_, _, ok := sub.applyNewSID()
		sub.mu.Unlock()
		if ok {
			t.Fatal("expected the sid swap to be refused on a closed connection")
		}
	})
}

func TestRewireOrderedSub(t *testing.T) {
	const osid, nsid, deliver, maxStr = 1, 2, "_INBOX.new", "5"
	newConn := func() (*Conn, *Subscription) {
		// A writer with a large limit never flushes, so the protocol lines
		// stay in bufs for inspection.
		nc := &Conn{subs: make(map[int64]*Subscription), bw: &natsWriter{limit: 1 << 20}}
		sub := &Subscription{conn: nc, sid: nsid}
		nc.ssid = nsid
		nc.subs[nsid] = sub
		return nc, sub
	}
	unsubOld := fmt.Sprintf(unsubProto, osid, _EMPTY_)
	subNew := fmt.Sprintf(subProto, deliver, _EMPTY_, nsid)
	unsubMax := fmt.Sprintf(unsubProto, nsid, maxStr)

	t.Run("registered sub is moved to the new sid", func(t *testing.T) {
		nc, sub := newConn()
		if !nc.rewireOrderedSub(sub, osid, nsid, deliver, maxStr) {
			t.Fatal("expected the rewire to proceed for a registered sub")
		}
		if got, want := string(nc.bw.bufs), unsubOld+subNew+unsubMax; got != want {
			t.Fatalf("unexpected protocol:\n got %q\nwant %q", got, want)
		}
	})

	t.Run("removed sub only gets the old sid unsubscribed", func(t *testing.T) {
		// Simulates Unsubscribe landing between applyNewSID and the
		// goroutine that sends the protocol: it removed the sub under the
		// new sid, so no interest (nor consumer) must be created for it.
		nc, sub := newConn()
		delete(nc.subs, nsid)
		if nc.rewireOrderedSub(sub, osid, nsid, deliver, maxStr) {
			t.Fatal("expected the rewire to be refused for an unregistered sub")
		}
		if got, want := string(nc.bw.bufs), unsubOld; got != want {
			t.Fatalf("unexpected protocol:\n got %q\nwant %q", got, want)
		}
	})

	t.Run("draining sub only gets the old sid unsubscribed", func(t *testing.T) {
		// A draining sub stays in nc.subs until the drain completes, and it
		// is then removed without an UNSUB, so subscribing the new sid would
		// leave interest on the server for the life of the connection.
		nc, sub := newConn()
		sub.draining = true
		if nc.rewireOrderedSub(sub, osid, nsid, deliver, maxStr) {
			t.Fatal("expected the rewire to be refused for a draining sub")
		}
		if got, want := string(nc.bw.bufs), unsubOld; got != want {
			t.Fatalf("unexpected protocol:\n got %q\nwant %q", got, want)
		}
	})

	t.Run("closed connection only gets the old sid unsubscribed", func(t *testing.T) {
		nc, sub := newConn()
		nc.subs = nil
		if nc.rewireOrderedSub(sub, osid, nsid, deliver, maxStr) {
			t.Fatal("expected the rewire to be refused on a closed connection")
		}
		if got, want := string(nc.bw.bufs), unsubOld; got != want {
			t.Fatalf("unexpected protocol:\n got %q\nwant %q", got, want)
		}
	})
}
