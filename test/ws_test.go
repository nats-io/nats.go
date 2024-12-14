// Copyright 2023 The NATS Authors
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
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"net"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natsserver "github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

func testWSGetDefaultOptions(t *testing.T, tls bool) *server.Options {
	t.Helper()
	sopts := natsserver.DefaultTestOptions
	sopts.Host = "127.0.0.1"
	sopts.Port = -1
	sopts.Websocket.Host = "127.0.0.1"
	sopts.Websocket.Port = -1
	sopts.Websocket.NoTLS = !tls
	if tls {
		tc := &server.TLSConfigOpts{
			CertFile: "./configs/certs/server.pem",
			KeyFile:  "./configs/certs/key.pem",
			CaFile:   "./configs/certs/ca.pem",
		}
		tlsConfig, err := server.GenTLSConfig(tc)
		if err != nil {
			t.Fatalf("Can't build TLCConfig: %v", err)
		}
		sopts.Websocket.TLSConfig = tlsConfig
	}
	return &sopts
}

func TestWSBasic(t *testing.T) {
	sopts := testWSGetDefaultOptions(t, false)
	s := RunServerWithOptions(sopts)
	defer s.Shutdown()

	url := fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port)
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	sub, err := nc.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}

	msgs := make([][]byte, 100)
	for i := 0; i < len(msgs); i++ {
		msg := make([]byte, rand.Intn(70000))
		for j := 0; j < len(msg); j++ {
			msg[j] = 'A' + byte(rand.Intn(26))
		}
		msgs[i] = msg
	}
	for i, msg := range msgs {
		if err := nc.Publish("foo", msg); err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		// Make sure that masking does not overwrite user data
		if !bytes.Equal(msgs[i], msg) {
			t.Fatalf("User content has been changed: %v, got %v", msgs[i], msg)
		}
	}

	for i := 0; i < len(msgs); i++ {
		msg, err := sub.NextMsg(time.Second)
		if err != nil {
			t.Fatalf("Error getting next message: %v", err)
		}
		if !bytes.Equal(msgs[i], msg.Data) {
			t.Fatalf("Expected message: %v, got %v", msgs[i], msg)
		}
	}
}

func TestWSControlFrames(t *testing.T) {
	sopts := testWSGetDefaultOptions(t, false)
	s := RunServerWithOptions(sopts)
	defer s.Shutdown()

	rch := make(chan bool, 10)
	ncSub, err := nats.Connect(s.ClientURL(),
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectHandler(func(_ *nats.Conn) { rch <- true }),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer ncSub.Close()

	sub, err := ncSub.SubscribeSync("foo")
	if err != nil {
		t.Fatalf("Error on subscribe: %v", err)
	}
	if err := ncSub.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	dch := make(chan error, 10)
	url := fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port)
	nc, err := nats.Connect(url,
		nats.ReconnectWait(50*time.Millisecond),
		nats.DisconnectErrHandler(func(_ *nats.Conn, err error) { dch <- err }),
		nats.ReconnectHandler(func(_ *nats.Conn) { rch <- true }),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Shutdown the server, which should send a close message, which by
	// spec the client will try to echo back.
	s.Shutdown()

	select {
	case <-dch:
		// OK
	case <-time.After(time.Second):
		t.Fatal("Should have been disconnected")
	}

	s = RunServerWithOptions(sopts)
	defer s.Shutdown()

	// Wait for both connections to reconnect
	if err := Wait(rch); err != nil {
		t.Fatalf("Should have reconnected: %v", err)
	}
	if err := Wait(rch); err != nil {
		t.Fatalf("Should have reconnected: %v", err)
	}
	// Even if the first connection reconnects, there is no guarantee
	// that the resend of SUB has yet been processed by the server.
	// Doing a flush here will give us the guarantee.
	if err := ncSub.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}

	// Publish and close connection.
	if err := nc.Publish("foo", []byte("msg")); err != nil {
		t.Fatalf("Error on publish: %v", err)
	}
	if err := nc.Flush(); err != nil {
		t.Fatalf("Error on flush: %v", err)
	}
	nc.Close()

	if _, err := sub.NextMsg(time.Second); err != nil {
		t.Fatalf("Did not get message: %v", err)
	}
}

func TestWSConcurrentConns(t *testing.T) {
	sopts := testWSGetDefaultOptions(t, false)
	s := RunServerWithOptions(sopts)
	defer s.Shutdown()

	url := fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port)

	total := 50
	errCh := make(chan error, total)
	wg := sync.WaitGroup{}
	wg.Add(total)
	for i := 0; i < total; i++ {
		go func() {
			defer wg.Done()

			nc, err := nats.Connect(url)
			if err != nil {
				errCh <- fmt.Errorf("Error on connect: %v", err)
				return
			}
			defer nc.Close()

			sub, err := nc.SubscribeSync(nuid.Next())
			if err != nil {
				errCh <- fmt.Errorf("Error on subscribe: %v", err)
				return
			}
			nc.Publish(sub.Subject, []byte("here"))
			if _, err := sub.NextMsg(time.Second); err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	select {
	case e := <-errCh:
		t.Fatal(e.Error())
	default:
	}
}

func TestWSCompression(t *testing.T) {
	msgSize := rand.Intn(40000)
	for _, test := range []struct {
		name           string
		srvCompression bool
		cliCompression bool
	}{
		{"srv_off_cli_off", false, false},
		{"srv_off_cli_on", false, true},
		{"srv_on_cli_off", true, false},
		{"srv_on_cli_on", true, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sopts := testWSGetDefaultOptions(t, false)
			sopts.Websocket.Compression = test.srvCompression
			s := RunServerWithOptions(sopts)
			defer s.Shutdown()

			url := fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port)
			var opts []nats.Option
			if test.cliCompression {
				opts = append(opts, nats.Compression(true))
			}
			nc, err := nats.Connect(url, opts...)
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			defer nc.Close()

			sub, err := nc.SubscribeSync("foo")
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}

			msgs := make([][]byte, 100)
			for i := 0; i < len(msgs); i++ {
				msg := make([]byte, msgSize)
				for j := 0; j < len(msg); j++ {
					msg[j] = 'A'
				}
				msgs[i] = msg
			}
			for i, msg := range msgs {
				if err := nc.Publish("foo", msg); err != nil {
					t.Fatalf("Error on publish: %v", err)
				}
				// Make sure that compression/masking does not touch user data
				if !bytes.Equal(msgs[i], msg) {
					t.Fatalf("User content has been changed: %v, got %v", msgs[i], msg)
				}
			}

			for i := 0; i < len(msgs); i++ {
				msg, err := sub.NextMsg(time.Second)
				if err != nil {
					t.Fatalf("Error getting next message (%d): %v", i+1, err)
				}
				if !bytes.Equal(msgs[i], msg.Data) {
					t.Fatalf("Expected message (%d): %v, got %v", i+1, msgs[i], msg)
				}
			}
		})
	}
}

func TestWSWithTLS(t *testing.T) {
	for _, test := range []struct {
		name        string
		compression bool
	}{
		{"without compression", false},
		{"with compression", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sopts := testWSGetDefaultOptions(t, true)
			sopts.Websocket.Compression = test.compression
			s := RunServerWithOptions(sopts)
			defer s.Shutdown()

			var copts []nats.Option
			if test.compression {
				copts = append(copts, nats.Compression(true))
			}

			// Check that we fail to connect without proper TLS configuration.
			nc, err := nats.Connect(fmt.Sprintf("ws://localhost:%d", sopts.Websocket.Port), copts...)
			if err == nil {
				if nc != nil {
					nc.Close()
				}
				t.Fatal("Expected error, got none")
			}

			// Same but with wss protocol, which should translate to TLS, however,
			// since we used self signed certificates, this should fail without
			// asking to skip server cert verification.
			nc, err = nats.Connect(fmt.Sprintf("wss://localhost:%d", sopts.Websocket.Port), copts...)
			// Since Go 1.18, we had to regenerate certs to not have to use GODEBUG="x509sha1=1"
			// But on macOS, with our test CA certs, no SCTs included, it will fail
			// for the reason "x509: “localhost” certificate is not standards compliant"
			// instead of "unknown authority".
			if err == nil || (!strings.Contains(err.Error(), "authority") && !strings.Contains(err.Error(), "compliant")) {
				if nc != nil {
					nc.Close()
				}
				t.Fatalf("Expected error about unknown authority: %v", err)
			}

			// Skip server verification and we should be good.
			copts = append(copts, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
			nc, err = nats.Connect(fmt.Sprintf("wss://localhost:%d", sopts.Websocket.Port), copts...)
			if err != nil {
				t.Fatalf("Error on connect: %v", err)
			}
			defer nc.Close()

			sub, err := nc.SubscribeSync("foo")
			if err != nil {
				t.Fatalf("Error on subscribe: %v", err)
			}
			if err := nc.Publish("foo", []byte("hello")); err != nil {
				t.Fatalf("Error on publish: %v", err)
			}
			if msg, err := sub.NextMsg(time.Second); err != nil {
				t.Fatalf("Did not get message: %v", err)
			} else if got := string(msg.Data); got != "hello" {
				t.Fatalf("Expected %q, got %q", "hello", got)
			}
		})
	}
}

type testSkipTLSDialer struct {
	dialer  *net.Dialer
	skipTLS bool
}

func (sd *testSkipTLSDialer) Dial(network, address string) (net.Conn, error) {
	return sd.dialer.Dial(network, address)
}

func (sd *testSkipTLSDialer) SkipTLSHandshake() bool {
	return sd.skipTLS
}

func TestWSWithTLSCustomDialer(t *testing.T) {
	sopts := testWSGetDefaultOptions(t, true)
	s := RunServerWithOptions(sopts)
	defer s.Shutdown()

	sd := &testSkipTLSDialer{
		dialer: &net.Dialer{
			Timeout: 2 * time.Second,
		},
		skipTLS: true,
	}

	// Connect with CustomDialer that fails since TLSHandshake is disabled.
	copts := make([]nats.Option, 0)
	copts = append(copts, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
	copts = append(copts, nats.SetCustomDialer(sd))
	_, err := nats.Connect(fmt.Sprintf("wss://localhost:%d", sopts.Websocket.Port), copts...)
	if err == nil {
		t.Fatalf("Expected error on connect: %v", err)
	}
	if err.Error() != `invalid websocket connection` {
		t.Logf("Expected invalid websocket connection: %v", err)
	}

	// Retry with the dialer.
	copts = make([]nats.Option, 0)
	sd = &testSkipTLSDialer{
		dialer: &net.Dialer{
			Timeout: 2 * time.Second,
		},
		skipTLS: false,
	}
	copts = append(copts, nats.Secure(&tls.Config{InsecureSkipVerify: true}))
	copts = append(copts, nats.SetCustomDialer(sd))
	nc, err := nats.Connect(fmt.Sprintf("wss://localhost:%d", sopts.Websocket.Port), copts...)
	if err != nil {
		t.Fatalf("Unexpected error on connect: %v", err)
	}
	defer nc.Close()
}

func TestWSGossipAndReconnect(t *testing.T) {
	o1 := testWSGetDefaultOptions(t, false)
	o1.ServerName = "A"
	o1.Cluster.Host = "127.0.0.1"
	o1.Cluster.Name = "abc"
	o1.Cluster.Port = -1
	s1 := RunServerWithOptions(o1)
	defer s1.Shutdown()

	o2 := testWSGetDefaultOptions(t, false)
	o2.ServerName = "B"
	o2.Cluster.Host = "127.0.0.1"
	o2.Cluster.Name = "abc"
	o2.Cluster.Port = -1
	o2.Routes = server.RoutesFromStr(fmt.Sprintf("nats://127.0.0.1:%d", o1.Cluster.Port))
	s2 := RunServerWithOptions(o2)
	defer s2.Shutdown()

	rch := make(chan bool, 10)
	url := fmt.Sprintf("ws://127.0.0.1:%d", o1.Websocket.Port)
	nc, err := nats.Connect(url,
		nats.ReconnectWait(50*time.Millisecond),
		nats.ReconnectHandler(func(_ *nats.Conn) { rch <- true }),
	)
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	timeout := time.Now().Add(time.Second)
	for time.Now().Before(timeout) {
		if len(nc.Servers()) > 1 {
			break
		}
		time.Sleep(15 * time.Millisecond)
	}
	if len(nc.Servers()) == 1 {
		t.Fatal("Did not discover server 2")
	}
	s1.Shutdown()

	// Wait for reconnect
	if err := Wait(rch); err != nil {
		t.Fatalf("Did not reconnect: %v", err)
	}
}

func TestWSStress(t *testing.T) {
	// Enable this test only when wanting to stress test the system, say after
	// some changes in the library or if a bug is found. Also, don't run it
	// with the `-race` flag!
	t.SkipNow()
	// Total producers (there will be 2 per subject)
	prods := 4
	// Total messages sent
	total := int64(1000000)
	// Total messages received, there is 2 consumer per subject
	totalRecv := 2 * total
	// We will create a "golden" slice from which sent messages
	// will be a subset of. Receivers will check that the content
	// match the expected content.
	maxPayloadSize := 100000
	mainPayload := make([]byte, maxPayloadSize)
	for i := 0; i < len(mainPayload); i++ {
		mainPayload[i] = 'A' + byte(rand.Intn(26))
	}
	for _, test := range []struct {
		name     string
		compress bool
	}{
		{"no_compression", false},
		{"with_compression", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			sopts := testWSGetDefaultOptions(t, false)
			sopts.Websocket.Compression = test.compress
			s := RunServerWithOptions(sopts)
			defer s.Shutdown()

			var count int64
			consDoneCh := make(chan struct{}, 1)
			errCh := make(chan error, 1)
			prodDoneCh := make(chan struct{}, prods)

			pushErr := func(e error) {
				select {
				case errCh <- e:
				default:
				}
			}

			createConn := func() *nats.Conn {
				t.Helper()
				nc, err := nats.Connect(fmt.Sprintf("ws://127.0.0.1:%d", sopts.Websocket.Port),
					nats.Compression(test.compress),
					nats.ErrorHandler(func(_ *nats.Conn, sub *nats.Subscription, err error) {
						if sub != nil {
							err = fmt.Errorf("Subscription on %q - err=%v", sub.Subject, err)
						}
						pushErr(err)
					}))
				if err != nil {
					t.Fatalf("Error connecting: %v", err)
				}
				return nc
			}

			cb := func(m *nats.Msg) {
				if len(m.Data) < 4 {
					pushErr(fmt.Errorf("Message payload too small: %+v", m.Data))
					return
				}
				ps := int(binary.BigEndian.Uint32(m.Data[:4]))
				if ps > maxPayloadSize {
					pushErr(fmt.Errorf("Invalid message size: %v", ps))
					return
				}
				if !bytes.Equal(m.Data[4:4+ps], mainPayload[:ps]) {
					pushErr(errors.New("invalid content"))
					return
				}
				if atomic.AddInt64(&count, 1) == totalRecv {
					consDoneCh <- struct{}{}
				}
			}

			subjects := []string{"foo", "bar"}
			for _, subj := range subjects {
				for i := 0; i < 2; i++ {
					nc := createConn()
					defer nc.Close()
					sub, err := nc.Subscribe(subj, cb)
					if err != nil {
						t.Fatalf("Error on subscribe: %v", err)
					}
					sub.SetPendingLimits(-1, -1)
					if err := nc.Flush(); err != nil {
						t.Fatalf("Error on flush: %v", err)
					}
				}
			}

			msgsPerProd := int(total / int64(prods))
			prodPerSubj := prods / len(subjects)
			for _, subj := range subjects {
				for i := 0; i < prodPerSubj; i++ {
					go func(subj string) {
						defer func() { prodDoneCh <- struct{}{} }()

						nc := createConn()
						defer nc.Close()

						for i := 0; i < msgsPerProd; i++ {
							// Have 80% of messages being rather small (<=1024)
							maxSize := 1024
							if rand.Intn(100) > 80 {
								maxSize = maxPayloadSize
							}
							ps := rand.Intn(maxSize)
							msg := make([]byte, 4+ps)
							binary.BigEndian.PutUint32(msg, uint32(ps))
							copy(msg[4:], mainPayload[:ps])
							if err := nc.Publish(subj, msg); err != nil {
								pushErr(err)
								return
							}
						}
						nc.Flush()
					}(subj)
				}
			}

			for i := 0; i < prods; i++ {
				select {
				case <-prodDoneCh:
				case e := <-errCh:
					t.Fatal(e)
				}
			}
			// Now wait for all consumers to be done.
			<-consDoneCh
		})
	}
}

func TestWSNoDeadlockOnAuthFailure(t *testing.T) {
	o := testWSGetDefaultOptions(t, false)
	o.Username = "user"
	o.Password = "pwd"
	s := RunServerWithOptions(o)
	defer s.Shutdown()

	tm := time.AfterFunc(3*time.Second, func() {
		buf := make([]byte, 1000000)
		n := runtime.Stack(buf, true)
		panic(fmt.Sprintf("Test has probably deadlocked!\n%s\n", buf[:n]))
	})

	if _, err := nats.Connect(fmt.Sprintf("ws://127.0.0.1:%d", o.Websocket.Port)); err == nil {
		t.Fatal("Expected auth error, did not get any error")
	}

	tm.Stop()
}
