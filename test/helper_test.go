// Copyright 2015-2019 The NATS Authors
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

package test

import (
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"

	natsserver "github.com/nats-io/nats-server/v2/test"
)

// So that we can pass tests and benchmarks...
type tLogger interface {
	Fatalf(format string, args ...interface{})
	Errorf(format string, args ...interface{})
}

// TestLogger
type TestLogger tLogger

// Dumb wait program to sync on callbacks, etc... Will timeout
func Wait(ch chan bool) error {
	return WaitTime(ch, 5*time.Second)
}

// Wait for a chan with a timeout.
func WaitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func stackFatalf(t tLogger, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers: Skip us and verify* frames.
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}
	t.Fatalf("%s", strings.Join(lines, "\n"))
}

////////////////////////////////////////////////////////////////////////////////
// Creating client connections
////////////////////////////////////////////////////////////////////////////////

// NewDefaultConnection
func NewDefaultConnection(t tLogger) *nats.Conn {
	return NewConnection(t, nats.DefaultPort)
}

// NewConnection forms connection on a given port.
func NewConnection(t tLogger, port int) *nats.Conn {
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v\n", err)
		return nil
	}
	return nc
}

// NewEConn
func NewEConn(t tLogger) *nats.EncodedConn {
	ec, err := nats.NewEncodedConn(NewDefaultConnection(t), nats.DEFAULT_ENCODER)
	if err != nil {
		t.Fatalf("Failed to create an encoded connection: %v\n", err)
	}
	return ec
}

////////////////////////////////////////////////////////////////////////////////
// Running nats server in separate Go routines
////////////////////////////////////////////////////////////////////////////////

// RunDefaultServer will run a server on the default port.
func RunDefaultServer() *server.Server {
	return RunServerOnPort(nats.DefaultPort)
}

// RunServerOnPort will run a server on the given port.
func RunServerOnPort(port int) *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = port
	opts.Cluster.Name = "testing"
	return RunServerWithOptions(opts)
}

// RunServerWithOptions will run a server with the given options.
func RunServerWithOptions(opts server.Options) *server.Server {
	return natsserver.RunServer(&opts)
}

// RunServerWithConfig will run a server with the given configuration file.
func RunServerWithConfig(configFile string) (*server.Server, *server.Options) {
	return natsserver.RunServerWithConfig(configFile)
}

func RunBasicJetStreamServer() *server.Server {
	opts := natsserver.DefaultTestOptions
	opts.Port = -1
	opts.JetStream = true
	return RunServerWithOptions(opts)
}

func createConfFile(t *testing.T, content []byte) string {
	t.Helper()
	conf, err := ioutil.TempFile("", "")
	if err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	fName := conf.Name()
	conf.Close()
	if err := ioutil.WriteFile(fName, content, 0666); err != nil {
		os.Remove(fName)
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}
