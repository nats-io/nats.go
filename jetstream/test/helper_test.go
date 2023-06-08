// Copyright 2022-2023 The NATS Authors
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
	"context"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	natsserver "github.com/nats-io/nats-server/v2/test"
)

type jsServer struct {
	*server.Server
	myopts  *server.Options
	restart sync.Mutex
}

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

////////////////////////////////////////////////////////////////////////////////
// Creating client connections
////////////////////////////////////////////////////////////////////////////////

// NewDefaultConnection
func NewDefaultConnection(t *testing.T) *nats.Conn {
	return NewConnection(t, nats.DefaultPort)
}

// NewConnection forms connection on a given port.
func NewConnection(t *testing.T, port int) *nats.Conn {
	url := fmt.Sprintf("nats://127.0.0.1:%d", port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Failed to create default connection: %v\n", err)
		return nil
	}
	return nc
}

// NewEConn
func NewEConn(t *testing.T) *nats.EncodedConn {
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
	conf, err := os.CreateTemp("", "")
	if err != nil {
		t.Fatalf("Error creating conf file: %v", err)
	}
	fName := conf.Name()
	if err := conf.Close(); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if err := os.WriteFile(fName, content, 0666); err != nil {
		if err := os.Remove(fName); err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		t.Fatalf("Error writing conf file: %v", err)
	}
	return fName
}

func shutdownJSServerAndRemoveStorage(t *testing.T, s *server.Server) {
	t.Helper()
	var sd string
	if config := s.JetStreamConfig(); config != nil {
		sd = config.StoreDir
	}
	s.Shutdown()
	if sd != "" {
		if err := os.RemoveAll(sd); err != nil {
			t.Fatalf("Unable to remove storage %q: %v", sd, err)
		}
	}
	s.WaitForShutdown()
}

func setupJSClusterWithSize(t *testing.T, clusterName string, size int) []*jsServer {
	t.Helper()
	nodes := make([]*jsServer, size)
	opts := make([]*server.Options, 0)

	var activeListeners []net.Listener
	getAddr := func(t *testing.T) (string, string, int) {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		addr := l.Addr()
		host := addr.(*net.TCPAddr).IP.String()
		port := addr.(*net.TCPAddr).Port
		time.Sleep(100 * time.Millisecond)

		// we cannot close the listener immediately to avoid duplicate port binding
		// the returned net.Listener has to be closed after all ports are drawn
		activeListeners = append(activeListeners, l)
		return addr.String(), host, port
	}

	routes := []string{}
	for i := 0; i < size; i++ {
		o := natsserver.DefaultTestOptions
		o.JetStream = true
		o.ServerName = fmt.Sprintf("NODE_%d", i)
		tdir, err := os.MkdirTemp(os.TempDir(), fmt.Sprintf("%s_%s-", o.ServerName, clusterName))
		if err != nil {
			t.Fatal(err)
		}
		o.StoreDir = tdir

		if size > 1 {
			o.Cluster.Name = clusterName
			_, host1, port1 := getAddr(t)
			o.Host = host1
			o.Port = port1

			addr2, host2, port2 := getAddr(t)
			o.Cluster.Host = host2
			o.Cluster.Port = port2
			o.Tags = []string{o.ServerName}
			routes = append(routes, fmt.Sprintf("nats://%s", addr2))
		}
		opts = append(opts, &o)
	}
	// close all connections used to randomize ports
	for _, l := range activeListeners {
		l.Close()
	}

	if size > 1 {
		routesStr := server.RoutesFromStr(strings.Join(routes, ","))

		for i, o := range opts {
			o.Routes = routesStr
			nodes[i] = &jsServer{Server: natsserver.RunServer(o), myopts: o}
		}
	} else {
		o := opts[0]
		nodes[0] = &jsServer{Server: natsserver.RunServer(o), myopts: o}
	}

	// Wait until JS is ready.
	srvA := nodes[0]
	nc, err := nats.Connect(srvA.ClientURL())
	if err != nil {
		t.Error(err)
	}
	waitForJSReady(t, nc)
	nc.Close()

	return nodes
}

func waitForJSReady(t *testing.T, nc *nats.Conn) {
	var err error
	timeout := time.Now().Add(10 * time.Second)
	for time.Now().Before(timeout) {
		// Use a smaller MaxWait here since if it fails, we don't want
		// to wait for too long since we are going to try again.
		js, err := nc.JetStream(nats.MaxWait(250 * time.Millisecond))
		if err != nil {
			t.Fatal(err)
		}
		_, err = js.AccountInfo()
		if err != nil {
			continue
		}
		return
	}
	t.Fatalf("Timeout waiting for JS to be ready: %v", err)
}

func withJSClusterAndStream(t *testing.T, clusterName string, size int, stream jetstream.StreamConfig, tfn func(t *testing.T, subject string, srvs ...*jsServer)) {
	t.Helper()

	withJSCluster(t, clusterName, size, func(t *testing.T, nodes ...*jsServer) {
		srvA := nodes[0]
		nc, err := nats.Connect(srvA.ClientURL())
		if err != nil {
			t.Error(err)
		}
		defer nc.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		jsm, err := jetstream.New(nc)
		if err != nil {
			t.Fatal(err)
		}
	CreateStream:
		for {
			select {
			case <-ctx.Done():
				if err != nil {
					t.Fatalf("Unexpected error creating stream: %v", err)
				}
				t.Fatalf("Unable to create stream on cluster")
			case <-time.After(500 * time.Millisecond):
				_, err = jsm.AccountInfo(ctx)
				if err != nil {
					// Backoff for a bit until cluster and resources are ready.
					time.Sleep(500 * time.Millisecond)
				}
				_, err = jsm.CreateStream(ctx, stream)
				if err != nil {
					continue CreateStream
				}
				break CreateStream
			}
		}

		tfn(t, stream.Name, nodes...)
	})
}

func withJSCluster(t *testing.T, clusterName string, size int, tfn func(t *testing.T, srvs ...*jsServer)) {
	t.Helper()

	nodes := setupJSClusterWithSize(t, clusterName, size)
	defer func() {
		// Ensure that they get shutdown and remove their state.
		for _, node := range nodes {
			node.restart.Lock()
			shutdownJSServerAndRemoveStorage(t, node.Server)
			node.restart.Unlock()
		}
	}()
	tfn(t, nodes...)
}

func restartBasicJSServer(t *testing.T, s *server.Server) *server.Server {
	opts := natsserver.DefaultTestOptions
	clientURL, err := url.Parse(s.ClientURL())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	port, err := strconv.Atoi(clientURL.Port())
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	opts.Port = port
	opts.JetStream = true
	opts.StoreDir = s.JetStreamConfig().StoreDir
	s.Shutdown()
	s.WaitForShutdown()
	return RunServerWithOptions(opts)
}

func checkFor(t *testing.T, totalWait, sleepDur time.Duration, f func() error) {
	t.Helper()
	timeout := time.Now().Add(totalWait)
	var err error
	for time.Now().Before(timeout) {
		err = f()
		if err == nil {
			return
		}
		time.Sleep(sleepDur)
	}
	if err != nil {
		t.Fatal(err.Error())
	}
}
