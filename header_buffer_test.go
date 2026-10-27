// Copyright 2026 The NATS Authors
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
	"strings"
	"testing"
)

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
