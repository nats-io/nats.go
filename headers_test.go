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

import (
	"bytes"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
)

func TestHeaderParser(t *testing.T) {
	shouldErr := func(hdr string) {
		t.Helper()
		if _, err := DecodeHeadersMsg([]byte(hdr)); err == nil {
			t.Fatalf("Expected an error")
		}
	}
	shouldErr("NATS/1.0")
	shouldErr("NATS/1.0\r\n")
	shouldErr("NATS/1.0\r\nk1:v1")
	shouldErr("NATS/1.0\r\nk1:v1\r\n")
	shouldErr("NATS/1.0 5\r\n\r\n")
	shouldErr("NATS/1.0 41\r\n\r\n")
	shouldErr("NATS/1.0 \r\n\r\n")

	// Check that we can do inline status and descriptions
	checkStatus := func(hdr string, status int, description string) {
		t.Helper()
		hdrs, err := DecodeHeadersMsg([]byte(hdr + "\r\n\r\n"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if code, err := strconv.Atoi(hdrs.Get(statusHdr)); err != nil || code != status {
			t.Fatalf("Expected status of %d, got %s", status, hdrs.Get(statusHdr))
		}
		if len(description) > 0 {
			if descr := hdrs.Get(descrHdr); err != nil || descr != description {
				t.Fatalf("Expected description of %q, got %q", description, descr)
			}
		}
	}

	checkStatus("NATS/1.0 503", 503, "")
	checkStatus("NATS/1.0 503 No Responders", 503, "No Responders")
	checkStatus("NATS/1.0  404   No Messages", 404, "No Messages")
}

func TestHeaderMultiLine(t *testing.T) {
	m := NewMsg("foo")
	m.Header = Header{
		"CorrelationID": []string{"123"},
		"Msg-ID":        []string{"456"},
		"X-NATS-Keys":   []string{"A", "B", "C"},
		"X-Test-Keys":   []string{"D", "E", "F"},
	}
	// Users can opt-in to canonicalize like http.Header does
	// by using http.Header#Set or http.Header#Add.
	http.Header(m.Header).Set("accept-encoding", "json")
	http.Header(m.Header).Add("AUTHORIZATION", "s3cr3t")

	// Multi Value Header becomes represented as multi-lines in the wire
	m.Header.Set("X-Test", "First")
	m.Header.Add("X-Test", "Second")
	m.Header.Add("X-Test", "Third")

	b, err := m.headerBytes()
	if err != nil {
		t.Fatal(err)
	}
	result := string(b)

	// Require the header string to have a known prefix and suffix
	if !strings.HasPrefix(result, "NATS/1.0\r\n") {
		t.Fatalf("header prefix not present")
	}
	if !strings.HasSuffix(result, "\r\n\r\n") {
		t.Fatalf("header suffix not present")
	}

	// Require the header string to contain each of the following rows. Note
	// that headers can be written in any order.
	expectedHeaderLines := []string{
		"Accept-Encoding: json\r\n",
		"Authorization: s3cr3t\r\n",
		"CorrelationID: 123\r\n",
		"Msg-ID: 456\r\n",
		"X-NATS-Keys: A\r\n",
		"X-NATS-Keys: B\r\n",
		"X-NATS-Keys: C\r\n",
		"X-Test: First\r\n",
		"X-Test: Second\r\n",
		"X-Test: Third\r\n",
		"X-Test-Keys: D\r\n",
		"X-Test-Keys: E\r\n",
		"X-Test-Keys: F\r\n",
	}
	for _, h := range expectedHeaderLines {
		if !strings.Contains(result, h) {
			t.Fatalf("expected header line '%s' to be present", h)
		}
	}
}

func TestHeaderValidKeys(t *testing.T) {
	const validChars = "!#$%&'*+-.0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ^_`abcdefghijklmnopqrstuvwxyz|~"

	m := NewMsg("foo")
	m.Header = Header{
		validChars: []string{"value"},
	}
	if _, err := m.headerBytes(); err != nil {
		t.Fatal(err.Error())
	}
}

func TestHeaderInvalidKeys(t *testing.T) {
	testKeys := []string{
		"",       // Empty string
		":",      // Illegal printable ASCII character
		"\r",     // Non-printable ASCII character
		"\n",     // Non-printable ASCII character
		"\t",     // Non-printable ASCII character
		"\x00",   // Non-printable ASCII character
		"→",      // Standard Unicode character
		"\u200b", // Non-printable Unicode character
	}
	for _, key := range testKeys {
		m := NewMsg("foo")
		m.Header = Header{
			key: []string{"value"},
		}
		if _, err := m.headerBytes(); err == nil {
			t.Fatalf("expected error for key '%s'", key)
		}
	}
}

func TestHeaderValidValues(t *testing.T) {
	testValues := []string{
		"!\"#$%&'()*+,-./0123456789;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~",                              // Printable ASCII characters
		"\x00\x01\x02\x03\x04\x05\x06\x07\x08\x09\x0b\x0c\x0e\x0f\x10\x11\x12\x13\x14\x15\x16\x17\x18\x19\x1a\x1b\x1c\x1c\x1d\x1e\x1f", // Invisible ASCII characters (except CR/LF)
		"→",      // Standard Unicode characters
		"\u200b", // Non-printable Unicode characters
	}
	for _, v := range testValues {
		m := NewMsg(v)
		m.Header = Header{
			"key": []string{v},
		}
		if _, err := m.headerBytes(); err != nil {
			t.Fatal(err.Error())
		}
	}
}

func TestHeaderNewlineValue(t *testing.T) {
	m := NewMsg("foo")
	m.Header.Set("X-Test", "line1\nline2\rline3")

	b, err := m.headerBytes()
	if err != nil {
		t.Fatal(err.Error())
	}
	out := string(b)
	if !strings.Contains(out, "X-Test: line1 line2 line3") {
		t.Fatalf("expected sanitized 'X-Test: line1 line2 line3', got: %q", out)
	}
}

func TestPublishMsgHeaderBufferOwnership(t *testing.T) {
	for _, pending := range []bool{false, true} {
		t.Run(fmt.Sprintf("pending=%t", pending), func(t *testing.T) {
			nc := &Conn{
				status: CONNECTED,
				info:   ServerInfo{Headers: true, MaxPayload: 1 << 20},
				bw:     &natsWriter{w: io.Discard, limit: 1 << 20, plimit: 1 << 20},
			}
			nc.setup()
			if pending {
				nc.bw.switchToPending()
			}

			var expected bytes.Buffer
			for _, values := range [][]string{
				{strings.Repeat("a", 4096)},
				{"short", "second"},
				nil,
				{strings.Repeat("b", 70*1024)},
				{"after large"},
				{},
				{"last"},
			} {
				m := &Msg{Subject: "foo", Data: []byte("data")}
				var hdr string
				if values != nil {
					m.Header = Header{"X-Test": values}
					hdr = "NATS/1.0\r\n"
					for _, value := range values {
						hdr += "X-Test: " + value + "\r\n"
					}
					hdr += "\r\n"
				}
				if err := nc.PublishMsg(m); err != nil {
					t.Fatal(err)
				}
				if hdr == "" {
					fmt.Fprintf(&expected, "PUB foo %d\r\n", len(m.Data))
				} else {
					fmt.Fprintf(&expected, "HPUB foo %d %d\r\n%s", len(hdr), len(hdr)+len(m.Data), hdr)
				}
				expected.Write(m.Data)
				expected.WriteString("\r\n")

				// Encoding errors between publishes must not affect queued messages
				// or the next successful publish.
				if err := nc.PublishMsg(&Msg{Subject: "foo", Header: Header{":": {"invalid"}}}); err != ErrBadHeaderMsg {
					t.Fatalf("Expected ErrBadHeaderMsg, got %v", err)
				}
				actual := nc.bw.bufs
				if pending {
					actual = nc.bw.pending.Bytes()
				}
				if !bytes.Equal(actual, expected.Bytes()) {
					t.Fatal("Queued messages changed after a subsequent publish")
				}
			}
		})
	}
}

func BenchmarkPublishMsgHeaderEncoding(b *testing.B) {
	for _, n := range []int{0, 1, 10, 100} {
		b.Run(fmt.Sprintf("headers=%d", n), func(b *testing.B) {
			nc := &Conn{
				status: CONNECTED,
				info:   ServerInfo{Headers: true, MaxPayload: 1 << 20},
				bw:     &natsWriter{w: io.Discard, limit: defaultBufSize},
			}
			nc.setup()
			m := &Msg{Subject: "foo", Header: make(Header, n), Data: []byte("Hello World")}
			for i := range n {
				m.Header.Set(fmt.Sprintf("header_%d", i), strings.Repeat("a", 32))
			}
			b.ReportAllocs()
			b.ResetTimer()
			for range b.N {
				if err := nc.PublishMsg(m); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func BenchmarkHeaderDecode(b *testing.B) {
	benchmarks := []struct {
		name   string
		header Header
	}{
		{"Small - 25", Header{
			"Msg-ID": []string{"123"}},
		},
		{"Medium - 141", Header{
			"CorrelationID": []string{"123"},
			"Msg-ID":        []string{"456"},
			"X-NATS-Keys":   []string{"A", "B", "C"},
			"X-Test-Keys":   []string{"D", "E", "F"},
		}},
		{"Large - 368", Header{
			"CorrelationID":     []string{"123"},
			"Msg-ID":            []string{"456"},
			"X-NATS-Keys":       []string{"A", "B", "C"},
			"X-Test-Keys":       []string{"D", "E", "F"},
			"X-A-Long-Header-1": []string{strings.Repeat("A", 100)},
			"X-A-Long-Header-2": []string{strings.Repeat("A", 100)},
		}},
	}
	for _, bm := range benchmarks {
		b.Run(bm.name, func(b *testing.B) {
			b.ReportAllocs()

			m := NewMsg("foo")
			m.Header = bm.header
			hdr, err := m.headerBytes()
			if err != nil {
				b.Fatalf("Unexpected error: %v", err)
			}

			for i := 0; i < b.N; i++ {
				if _, err := DecodeHeadersMsg(hdr); err != nil {
					b.Fatalf("Unexpected error: %v", err)
				}
			}
		})
	}
}
