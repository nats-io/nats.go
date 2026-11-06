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

package test

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
)

func TestReconnectTLSVerificationError(t *testing.T) {
	// Cover both INFO-before-TLS and TLS-first connection setup.
	for _, handshakeFirst := range []bool{false, true} {
		name := "INFO before TLS"
		if handshakeFirst {
			name = "TLS handshake first"
		}
		t.Run(name, func(t *testing.T) {
			c := newTester(t)
			tlsOpts := []testservice.TLSOpt{testservice.TLSServerOnly()}
			if handshakeFirst {
				tlsOpts = append(tlsOpts, testservice.TLSHandshakeFirst())
			}
			inst := c.CreateServer(t, false, managedTLSOpts(t, tlsOpts...))
			t.Cleanup(func() { inst.Destroy(t) })

			// Trust the real test server's generated CA for the initial connection
			// and for recovery after the injected verification failure.
			trustedRoots := x509.NewCertPool()
			if !trustedRoots.AppendCertsFromPEM([]byte(inst.TLS.CAPEM)) {
				t.Fatal("Failed to parse server CA")
			}
			// The test arms this flag; the TLS handshake consumes it once.
			var rejectNext atomic.Bool
			var errorCallbacks atomic.Int32
			reconnectErrors := make(chan error, 2)
			reconnected := make(chan struct{}, 1)
			opts := []nats.Option{
				nats.ClientTLSConfig(nil, func() (*x509.CertPool, error) {
					// An empty CA pool rejects the server's certificate. Swap resets
					// the flag, so only this handshake fails and the next trusts the CA.
					// Recovery must not depend on the error callback being delivered.
					if rejectNext.Swap(false) {
						return x509.NewCertPool(), nil
					}
					return trustedRoots, nil
				}),
				nats.MaxReconnects(-1),
				nats.ReconnectWait(10 * time.Millisecond),
				nats.ReconnectJitter(0, 0),
				nats.ReconnectErrHandler(func(nc *nats.Conn, err error) {
					// Status would deadlock if this callback were invoked directly
					// while holding the connection's write lock.
					nc.Status()
					errorCallbacks.Add(1)
					select {
					case reconnectErrors <- err:
					default:
					}
				}),
				nats.ReconnectHandler(func(_ *nats.Conn) {
					select {
					case reconnected <- struct{}{}:
					default:
					}
				}),
			}
			if handshakeFirst {
				opts = append(opts, nats.TLSHandshakeFirst())
			}
			nc, err := nats.Connect(inst.Servers[0].URL, opts...)
			if err != nil {
				t.Fatalf("Failed to establish trusted TLS connection: %v", err)
			}
			t.Cleanup(nc.Close)

			// Keep the server running and force the established client to reconnect.
			// The next TLS handshake fails because its CA pool is empty.
			rejectNext.Store(true)
			if err := nc.ForceReconnect(); err != nil {
				t.Fatalf("Failed to force reconnection: %v", err)
			}
			select {
			case <-reconnected:
			case <-time.After(5 * time.Second):
				t.Fatal("Connection did not recover after restoring trusted roots")
			}

			// Both handlers run on the same FIFO callback queue. Receiving the
			// reconnect notification means earlier error callbacks have finished,
			// so the count includes all reported failures before recovery.
			if got := errorCallbacks.Load(); got != 1 {
				t.Fatalf("Expected one TLS reconnect error callback, got %d", got)
			}
			err = <-reconnectErrors
			// Check both the NATS TLS error and the underlying certificate cause,
			// so a TCP failure or an unrelated reconnect error cannot satisfy this.
			var verificationErr *tls.CertificateVerificationError
			var authorityErr x509.UnknownAuthorityError
			if !errors.Is(err, nats.ErrTLS) || !errors.As(err, &verificationErr) || !errors.As(err, &authorityErr) {
				t.Fatalf("Expected a TLS certificate verification error for an unknown CA, got %v", err)
			}
			// A successful PING/PONG exchange confirms the recovered connection works.
			if err := nc.FlushTimeout(time.Second); err != nil {
				t.Fatalf("Recovered connection is not usable: %v", err)
			}
		})
	}
}
