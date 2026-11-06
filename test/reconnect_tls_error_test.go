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
	"crypto/x509"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	testservice "github.com/synadia-io/orbit.go/ntf-client"
)

func TestReconnectTLSVerificationError(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false, managedTLSOpts(t, testservice.TLSServerOnly()))
	t.Cleanup(func() { inst.Destroy(t) })

	// Trust the test server's CA so the first connection succeeds.
	trustedRoots := x509.NewCertPool()
	if !trustedRoots.AppendCertsFromPEM([]byte(inst.TLS.CAPEM)) {
		t.Fatal("Failed to parse server CA")
	}
	var rejectNext atomic.Bool
	reconnectErrors := make(chan error, 1)
	reconnected := make(chan struct{}, 1)
	nc, err := nats.Connect(inst.Servers[0].URL,
		nats.ClientTLSConfig(nil, func() (*x509.CertPool, error) {
			// This function supplies trusted CAs for each TLS handshake.
			// Swap(false) reads the flag and clears it at the same time.
			// An empty pool makes this handshake reject the server certificate.
			// The next call trusts the CA again, even if no error callback runs.
			if rejectNext.Swap(false) {
				return x509.NewCertPool(), nil
			}
			return trustedRoots, nil
		}),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(10*time.Millisecond),
		nats.ReconnectJitter(0, 0),
		nats.ReconnectErrHandler(func(nc *nats.Conn, err error) {
			// Check that the callback can safely query the connection status.
			// Calling it directly while holding the connection's write lock would deadlock.
			nc.Status()
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
	)
	if err != nil {
		t.Fatalf("Failed to establish trusted TLS connection: %v", err)
	}
	t.Cleanup(nc.Close)

	// Disconnect the client while leaving the server running.
	// Fail the first retry, then allow the next retry to succeed.
	rejectNext.Store(true)
	if err := nc.ForceReconnect(); err != nil {
		t.Fatalf("Failed to force reconnection: %v", err)
	}
	select {
	case <-reconnected:
	case <-time.After(5 * time.Second):
		t.Fatal("Connection did not recover after restoring trusted roots")
	}

	// Callbacks run one at a time, in the order they were queued.
	// The recovery notification tells us any earlier error callback has finished.
	select {
	case err = <-reconnectErrors:
	default:
		t.Fatal("No TLS reconnect error callback received")
	}
	// The error must identify TLS verification failure caused by an unknown CA.
	var authorityErr x509.UnknownAuthorityError
	if !errors.Is(err, nats.ErrTLS) || !errors.As(err, &authorityErr) {
		t.Fatalf("Expected a TLS certificate verification error for an unknown CA, got %v", err)
	}
	// Flush sends PING and waits for PONG to check the connection still works.
	if err := nc.FlushTimeout(time.Second); err != nil {
		t.Fatalf("Recovered connection is not usable: %v", err)
	}
}
