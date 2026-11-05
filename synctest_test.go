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
	"bufio"
	"net"
	"runtime"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// pipeServer answers a client over net.Pipe, whose deadlines follow a
// synctest bubble's clock where TCP's would not.
type pipeServer struct{}

func (pipeServer) InProcessConn() (net.Conn, error) {
	client, server := net.Pipe()
	go func() {
		defer server.Close()
		server.Write([]byte("INFO {\"server_id\":\"pipe\"}\r\n"))
		br := bufio.NewReader(server)
		for {
			line, err := br.ReadString('\n')
			if err != nil {
				return
			}
			if strings.HasPrefix(line, "PING") {
				server.Write([]byte("PONG\r\n"))
			}
		}
	}()
	return client, nil
}

func flushOverPipe(t *testing.T) {
	nc, err := Connect("", InProcessServer(pipeServer{}))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer nc.Close()
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatalf("Error flushing: %v", err)
	}
}

// A timer made in a synctest bubble belongs to it, and using it from
// anywhere else is a fatal error, so no timer may outlive the call that
// made it. One P makes any timer handed on reach the next caller.
func TestTimersStayInTheirSynctestBubble(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(1))
	synctest.Test(t, flushOverPipe)
	synctest.Test(t, flushOverPipe)
	flushOverPipe(t)
}
