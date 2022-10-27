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

package services

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

// Notice: Experimental Preview
//
// This functionality is EXPERIMENTAL and may be changed in later releases.

type (

	// Service is an interface for sevice management.
	// It exposes methods to stop/reset a service, as well as get information on a service.
	Service interface {
		ID() string
		Name() string
		Description() string
		Version() string
		Stats() ServiceStats
		Reset()
		Stop()
	}

	// A request handler.
	// TODO (could make error more and return more info to user automatically?)
	ServiceHandler func(svc Service, req *nats.Msg) error

	// Clients can request as well.
	ServiceStats struct {
		Name      string    `json:"name"`
		ID        string    `json:"id"`
		Version   string    `json:"version"`
		Started   time.Time `json:"started"`
		Endpoints []Stats   `json:"stats"`
	}

	Stats struct {
		Name           string        `json:"name"`
		NumRequests    int           `json:"num_requests"`
		NumErrors      int           `json:"num_errors"`
		TotalLatency   time.Duration `json:"total_latency"`
		AverageLatency time.Duration `json:"average_latency"`
		Data           interface{}   `json:"data"`
	}

	// ServiceInfo is the basic information about a service type
	ServiceInfo struct {
		Name        string `json:"name"`
		ID          string `json:"id"`
		Description string `json:"description"`
		Version     string `json:"version"`
		Subject     string `json:"subject"`
	}

	ServiceSchema struct {
		Request  string `json:"request"`
		Response string `json:"response"`
	}

	Endpoint struct {
		Subject string `json:"subject"`
		Handler ServiceHandler
	}

	InternalEndpoint struct {
		Name    string
		Handler nats.MsgHandler
	}

	ServiceVerb int64

	ServiceConfig struct {
		Name          string        `json:"name"`
		Description   string        `json:"description"`
		Version       string        `json:"version"`
		Schema        ServiceSchema `json:"schema"`
		Endpoint      Endpoint      `json:"endpoint"`
		StatusHandler func(Endpoint) interface{}
	}

	// service is the internal implementation of a Service
	service struct {
		sync.Mutex
		ServiceConfig
		id string
		// subs
		reqSub   *nats.Subscription
		internal map[string]*nats.Subscription
		statuses map[string]*Stats
		stats    *ServiceStats
		conn     *nats.Conn
	}
)

const (
	// We can fix this, as versions will be on separate subjects and use account mapping to roll requests to new versions etc.
	QG = "svc"

	// ServiceApiPrefix is the root of all control subjects
	ServiceApiPrefix = "$SRV"

	ServiceErrorHeader = "Nats-Service-Error"
)

const (
	SrvPing ServiceVerb = iota
	SrvStatus
	SrvInfo
	SrvSchema
)

func (s *ServiceConfig) Valid() error {
	if s.Name == "" {
		return errors.New("name is required")
	}
	return s.Endpoint.Valid()
}

func (e *Endpoint) Valid() error {
	s := strings.TrimSpace(e.Subject)
	if len(s) == 0 {
		return errors.New("subject is required")
	}
	if e.Handler == nil {
		return errors.New("handler is required")
	}
	return nil
}

func (s ServiceVerb) String() string {
	switch s {
	case SrvPing:
		return "PING"
	case SrvStatus:
		return "STATUS"
	case SrvInfo:
		return "INFO"
	case SrvSchema:
		return "SCHEMA"
	default:
		return ""
	}
}

// Add adds a microservice.
// NOTE we can do an OpenAPI version as well, but looking at it it was very involved. So I think keep simple version and
// also have a version that talkes full blown OpenAPI spec and we can pull these things out.
func Add(nc *nats.Conn, config ServiceConfig) (Service, error) {
	if err := config.Valid(); err != nil {
		return nil, err
	}

	id := nuid.Next()
	svc := &service{
		ServiceConfig: config,
		conn:          nc,
		id:            id,
	}
	svc.internal = make(map[string]*nats.Subscription)
	svc.statuses = make(map[string]*Stats)
	svc.statuses[""] = &Stats{
		Name: config.Name,
	}

	svc.stats = &ServiceStats{
		Name:    config.Name,
		ID:      id,
		Version: config.Version,
		Started: time.Now(),
	}

	// Setup internal subscriptions.
	var err error

	svc.reqSub, err = nc.QueueSubscribe(config.Endpoint.Subject, QG, func(m *nats.Msg) {
		svc.reqHandler(m)
	})
	if err != nil {
		return nil, err
	}

	info := &ServiceInfo{
		Name:        config.Name,
		ID:          id,
		Description: config.Description,
		Version:     config.Version,
		Subject:     config.Endpoint.Subject,
	}

	infoHandler := func(m *nats.Msg) {
		response, _ := json.MarshalIndent(info, "", "  ")
		m.Respond(response)
	}

	pingHandler := func(m *nats.Msg) {
		infoHandler(m)
	}

	statusHandler := func(m *nats.Msg) {
		response, _ := json.MarshalIndent(svc.Stats(), "", "  ")
		m.Respond(response)
	}

	schemaHandler := func(m *nats.Msg) {
		response, _ := json.MarshalIndent(svc.ServiceConfig.Schema, "", "  ")
		m.Respond(response)
	}

	if err := svc.addInternalHandlerGroup(nc, SrvInfo, infoHandler); err != nil {
		return nil, err
	}
	if err := svc.addInternalHandlerGroup(nc, SrvPing, pingHandler); err != nil {
		return nil, err
	}
	if err := svc.addInternalHandlerGroup(nc, SrvStatus, statusHandler); err != nil {
		return nil, err
	}

	if svc.ServiceConfig.Schema.Request != "" || svc.ServiceConfig.Schema.Response != "" {
		if err := svc.addInternalHandlerGroup(nc, SrvSchema, schemaHandler); err != nil {
			return nil, err
		}
	}

	svc.stats.ID = id
	svc.stats.Started = time.Now()
	return svc, nil
}

// addInternalHandlerGroup generates control handlers for a specific verb
// each request generates 3 subscriptions, one for the general verb
// affecting all services written with the framework, one that handles
// all services of a particular kind, and finally a specific service.
func (svc *service) addInternalHandlerGroup(nc *nats.Conn, verb ServiceVerb, handler nats.MsgHandler) error {
	name := fmt.Sprintf("%s-all", verb.String())
	if err := svc.addInternalHandler(nc, verb, "", "", name, handler); err != nil {
		return err
	}
	name = fmt.Sprintf("%s-kind", verb.String())
	if err := svc.addInternalHandler(nc, verb, svc.Name(), "", name, handler); err != nil {
		return err
	}
	return svc.addInternalHandler(nc, verb, svc.Name(), svc.ID(), verb.String(), handler)
}

// addInternalHandler registers a control subject handler
func (svc *service) addInternalHandler(nc *nats.Conn, verb ServiceVerb, kind, id, name string, handler nats.MsgHandler) error {
	subj, err := SvcControlSubject(verb, kind, id)
	if err != nil {
		svc.Stop()
		return err
	}

	svc.internal[name], err = nc.Subscribe(subj, func(msg *nats.Msg) {
		start := time.Now()
		defer func() {
			svc.Lock()
			stats := svc.statuses[name]
			stats.NumRequests++
			stats.TotalLatency += time.Since(start)
			stats.AverageLatency = stats.TotalLatency / time.Duration(stats.NumRequests)
			svc.Unlock()
		}()
		handler(msg)
	})
	if err != nil {
		svc.Stop()
		return err
	}

	svc.statuses[name] = &Stats{
		Name: name,
	}
	return nil
}

// reqHandler itself
func (svc *service) reqHandler(req *nats.Msg) {
	start := time.Now()
	defer func() {
		svc.Lock()
		stats := svc.statuses[""]
		stats.NumRequests++
		stats.TotalLatency += time.Since(start)
		stats.AverageLatency = stats.TotalLatency / time.Duration(stats.NumRequests)
		svc.Unlock()
	}()

	if err := svc.ServiceConfig.Endpoint.Handler(svc, req); err != nil {
		hdr := make(nats.Header)
		apiErr := &ServiceAPIError{}
		if ok := errors.As(err, &apiErr); !ok {
			hdr[ServiceErrorHeader] = []string{fmt.Sprintf("%d %s", 500, err.Error())}
		} else {
			hdr[ServiceErrorHeader] = []string{apiErr.Error()}
		}
		svc.Lock()
		stats := svc.statuses[""]
		stats.NumErrors++
		svc.Unlock()

		svc.conn.PublishMsg(&nats.Msg{
			Subject: req.Reply,
			Header:  hdr,
		})
	}
}

func (svc *service) Stop() {
	if svc.reqSub != nil {
		svc.reqSub.Drain()
		svc.reqSub = nil
	}
	var keys []string
	for key, sub := range svc.internal {
		keys = append(keys, key)
		sub.Drain()
	}
	for _, key := range keys {
		delete(svc.internal, key)
	}
}

func (svc *service) ID() string {
	return svc.id
}

func (svc *service) Name() string {
	return svc.ServiceConfig.Name
}

func (svc *service) Description() string {
	return svc.ServiceConfig.Description
}

func (svc *service) Version() string {
	return svc.ServiceConfig.Version
}

func (svc *service) Stats() ServiceStats {
	svc.Lock()
	defer func() {
		svc.Unlock()
	}()
	if svc.ServiceConfig.StatusHandler != nil {
		stats := svc.statuses[""]
		stats.Data = svc.ServiceConfig.StatusHandler(svc.Endpoint)
	}
	idx := 0
	v := make([]Stats, len(svc.statuses))
	for _, se := range svc.statuses {
		v[idx] = *se
		idx++
	}
	svc.stats.Endpoints = v
	return *svc.stats
}

func (svc *service) Reset() {
	for _, se := range svc.statuses {
		se.NumRequests = 0
		se.TotalLatency = 0
		se.NumErrors = 0
		se.Data = nil
	}
}

// SvcControlSubject returns monitoring subjects used by the ServiceImpl
func SvcControlSubject(verb ServiceVerb, kind, id string) (string, error) {
	sverb := verb.String()
	if sverb == "" {
		return "", fmt.Errorf("unsupported service verb")
	}
	kind = strings.ToUpper(kind)
	if kind == "" && id == "" {
		return fmt.Sprintf("%s.%s", ServiceApiPrefix, sverb), nil
	}
	if id == "" {
		return fmt.Sprintf("%s.%s.%s", ServiceApiPrefix, sverb, kind), nil
	}
	return fmt.Sprintf("%s.%s.%s.%s", ServiceApiPrefix, sverb, kind, id), nil
}
