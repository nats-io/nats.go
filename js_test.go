// Copyright 2012-2023 The NATS Authors
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
	"strings"
	"testing"
)

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
