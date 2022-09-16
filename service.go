// Copyright 2022 The NATS Authors
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
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nuid"
)

type Service interface {
	ID() string
	Name() string
	Description() string
	Version() string

	// Stats
	Stats() Stats
	Reset()

	Close()
}

// A request handler.
// TODO (could make error more and return more info to user automatically?)
type RequestHandler func(svc Service, req *Msg) error

// Clients can request as well.
type Stats struct {
	ID           string
	Started      time.Time
	NumRequests  int
	NumErrors    int
	TotalLatency time.Duration
}

// We can fix this, as versions will be on separate subjects and use account mapping to roll requests to new versions etc.
const QG = "svc"

// When a request for info comes in we return json of this.
type ServiceInfo struct {
	Name        string
	Description string
	Version     string
}

// Internal struct
type service struct {
	id          string
	name        string
	description string
	version     string
	stats       Stats
	// subs
	reqSub *Subscription
	// info
	infoSub *Subscription
	// stats
	pingSub *Subscription

	cb RequestHandler
}

// Add a microservice.
// NOTE we can do an OpenAPI version as well, but looking at it it was very involved. So I think keep simple version and
// also have a version that talkes full blown OpenAPI spec and we can pull these things out.
func (nc *Conn) AddService(name, description, version, subject, reqSchema, respSchema string, reqHandler RequestHandler) (Service, error) {

	// NOTE WIP
	svc := &service{id: nuid.Next(), name: name, description: description, version: version, cb: reqHandler}

	// Setup internal subscriptions.
	var err error

	svc.reqSub, err = nc.QueueSubscribe(subject, QG, func(m *Msg) { svc.reqHandler(m) })
	if err != nil {
		return nil, err
	}

	// Construct info sub from main subject.
	info := fmt.Sprintf("%s.INFO", subject)
	svc.infoSub, err = nc.QueueSubscribe(info, QG, func(m *Msg) {
		si := &ServiceInfo{
			Name:        svc.name,
			Description: svc.description,
			Version:     svc.version,
		}
		response, _ := json.MarshalIndent(si, "", "  ")
		m.Respond(response)
	})
	if err != nil {
		svc.Close()
		return nil, err
	}

	// Construct ping sub from main subject.
	// These will be responded by all handlers.
	ping := fmt.Sprintf("%s.PING", subject)
	svc.infoSub, err = nc.Subscribe(ping, func(m *Msg) {
		response, _ := json.MarshalIndent(svc.stats, "", "  ")
		m.Respond(response)
	})
	if err != nil {
		svc.Close()
		return nil, err
	}

	svc.stats.ID = svc.id
	svc.stats.Started = time.Now()
	return svc, nil
}

// reqHandler itself
func (svc *service) reqHandler(req *Msg) {
	start := time.Now()
	defer func() {
		svc.stats.NumRequests++
		svc.stats.TotalLatency += time.Since(start)
	}()

	if err := svc.cb(svc, req); err != nil {
		svc.stats.NumErrors++
		req.Sub.mu.Lock()
		nc := req.Sub.conn
		req.Sub.mu.Unlock()

		hdr := []byte(fmt.Sprintf("NATS/1.0 500 %s\r\n\r\n", err.Error()))
		nc.publish(req.Reply, _EMPTY_, hdr, nil)
	}
}

func (svc *service) Close() {
	if svc.reqSub != nil {
		svc.reqSub.Drain()
		svc.reqSub = nil
	}
	if svc.infoSub != nil {
		svc.infoSub.Drain()
		svc.infoSub = nil
	}
	if svc.pingSub != nil {
		svc.pingSub.Drain()
		svc.pingSub = nil
	}
}

func (svc *service) ID() string {
	return svc.id
}

func (svc *service) Name() string {
	return svc.name
}

func (svc *service) Description() string {
	return svc.description
}

func (svc *service) Version() string {
	return svc.version
}

func (svc *service) Stats() Stats {
	return svc.stats
}

func (svc *service) Reset() {
	svc.stats = Stats{}
}
