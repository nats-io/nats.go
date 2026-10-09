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

package micro

import (
	"slices"
	"sync"

	"github.com/nats-io/nats.go"
)

// connHooks holds the services added on a connection and the connection's
// handlers that were set before the first of them was added.
type connHooks struct {
	services   []*service
	prevClosed nats.ConnHandler
	prevErr    nats.ErrHandler
}

var hooks = struct {
	sync.Mutex
	conns map[*nats.Conn]*connHooks
}{conns: make(map[*nats.Conn]*connHooks)}

// registerService adds s to its connection's hooks. The first service on a
// connection installs the connection's closed and error handlers.
func registerService(s *service) {
	hooks.Lock()
	defer hooks.Unlock()
	h, ok := hooks.conns[s.nc]
	if !ok {
		h = &connHooks{
			prevClosed: s.nc.ClosedHandler(),
			prevErr:    s.nc.ErrorHandler(),
		}
		hooks.conns[s.nc] = h
		s.nc.SetClosedHandler(func(c *nats.Conn) {
			h.connClosed(c)
		})
		s.nc.SetErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
			h.asyncErr(c, sub, err)
		})
	}
	h.services = append(h.services, s)
}

// unregisterService removes s from its connection's hooks. The last service
// on a connection restores the connection's previous handlers.
func unregisterService(s *service) {
	hooks.Lock()
	defer hooks.Unlock()
	h, ok := hooks.conns[s.nc]
	if !ok {
		return
	}
	h.services = slices.DeleteFunc(h.services, func(other *service) bool {
		return other == s
	})
	if len(h.services) > 0 {
		return
	}
	delete(hooks.conns, s.nc)
	if !s.nc.IsClosed() {
		s.nc.SetClosedHandler(h.prevClosed)
		s.nc.SetErrorHandler(h.prevErr)
	}
}

func (h *connHooks) connClosed(c *nats.Conn) {
	hooks.Lock()
	services := h.services
	h.services = nil
	if hooks.conns[c] == h {
		delete(hooks.conns, c)
	}
	hooks.Unlock()
	for _, s := range services {
		s.Stop()
	}
	if h.prevClosed != nil {
		h.prevClosed(c)
	}
}

func (h *connHooks) asyncErr(c *nats.Conn, sub *nats.Subscription, err error) {
	hooks.Lock()
	services := slices.Clone(h.services)
	hooks.Unlock()
	for _, s := range services {
		s.handleAsyncErr(sub, err)
	}
	if h.prevErr != nil {
		h.prevErr(c, sub, err)
	}
}
