// Copyright 2012-2025 The NATS Authors
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

//go:build !tinygo

package nats

import (
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"net"

	"github.com/nats-io/nats.go/util"
)

// makeTLSConn will wrap an existing Conn using TLS
func (nc *Conn) makeTLSConn() error {
	if nc.Opts.CustomDialer != nil {
		// we do nothing when asked to skip the TLS wrapper
		sd, ok := nc.Opts.CustomDialer.(skipTLSDialer)
		if ok && sd.SkipTLSHandshake() {
			return nil
		}
	}
	// Allow the user to configure their own tls.Config structure.
	tlsCopy := &tls.Config{}
	if nc.Opts.TLSConfig != nil {
		tlsCopy = util.CloneTLSConfig(nc.Opts.TLSConfig)
	}
	if nc.Opts.TLSCertCB != nil {
		cert, err := nc.Opts.TLSCertCB()
		if err != nil {
			return err
		}
		tlsCopy.Certificates = []tls.Certificate{cert}
	}
	if nc.Opts.RootCAsCB != nil {
		rootCAs, err := nc.Opts.RootCAsCB()
		if err != nil {
			return err
		}
		tlsCopy.RootCAs = rootCAs
	}
	// If its blank we will override it with the current host
	if tlsCopy.ServerName == _EMPTY_ {
		if nc.current.tlsName != _EMPTY_ {
			tlsCopy.ServerName = nc.current.tlsName
		} else {
			h, _, _ := net.SplitHostPort(nc.current.URL.Host)
			tlsCopy.ServerName = h
		}
	}
	nc.conn = tls.Client(nc.conn, tlsCopy)
	conn := nc.conn.(*tls.Conn)
	if err := conn.Handshake(); err != nil {
		return fmt.Errorf("%w: %w", ErrTLS, err)
	}
	nc.bindToNewConn()
	return nil
}

// TLSConnectionState retrieves the state of the TLS connection to the server
func (nc *Conn) TLSConnectionState() (tls.ConnectionState, error) {
	if !nc.isConnected() {
		return tls.ConnectionState{}, ErrDisconnected
	}

	nc.mu.RLock()
	conn := nc.conn
	nc.mu.RUnlock()

	tc, ok := conn.(*tls.Conn)
	if !ok {
		return tls.ConnectionState{}, ErrConnectionNotTLS
	}

	return tc.ConnectionState(), nil
}

// tlsHandshakeEOF wraps an error with context when it occurs right after
// a completed TLS handshake, which typically indicates the remote side
// rejected the client certificate (e.g. an mTLS proxy like nginx).
// Depending on timing, the error may be io.EOF (read from closed conn)
// or a "broken pipe"/"connection reset" (write to closed conn).
func (nc *Conn) tlsHandshakeEOF(err error) error {
	tlsConn, ok := nc.conn.(*tls.Conn)
	if !ok || !tlsConn.ConnectionState().HandshakeComplete {
		return err
	}
	if errors.Is(err, io.EOF) || isConnClosedError(err) {
		return fmt.Errorf("%w: connection closed by remote after TLS handshake: %w", ErrTLS, err)
	}
	return err
}
