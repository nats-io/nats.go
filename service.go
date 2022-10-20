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
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"
)

type Service interface {
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
type ServiceHandler func(svc Service, req *Msg) error

// Clients can request as well.
type ServiceStats struct {
	Name      string    `json:"name"`
	ID        string    `json:"id"`
	Version   string    `json:"version"`
	Started   time.Time `json:"started"`
	Endpoints []Stats   `json:"stats"`
}
type Stats struct {
	Name         string        `json:"name"`
	NumRequests  int           `json:"numRequests"`
	NumErrors    int           `json:"numErrors"`
	TotalLatency time.Duration `json:"totalLatency"`
	Data         interface{}   `json:"data"`
}

// We can fix this, as versions will be on separate subjects and use account mapping to roll requests to new versions etc.
const QG = "svc"

// ServiceInfo is the basic information about a service type
type ServiceInfo struct {
	Name        string `json:"name"`
	Id          string `json:"id"`
	Description string `json:"description"`
	Version     string `json:"version"`
	Subject     string `json:"subject"`
}

func (s *ServiceConfig) Valid() error {
	if s.Name == "" {
		return errors.New("name is required")
	}
	return s.Endpoint.Valid()
}

type ServiceSchema struct {
	Request  string `json:"request"`
	Response string `json:"response"`
}

type Endpoint struct {
	Subject string `json:"subject"`
	Handler ServiceHandler
}

type InternalEndpoint struct {
	Name    string
	Handler MsgHandler
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

type ServiceConfig struct {
	Name          string        `json:"name"`
	Id            string        `json:"id"`
	Description   string        `json:"description"`
	Version       string        `json:"version"`
	Schema        ServiceSchema `json:"schema"`
	Endpoint      Endpoint      `json:"endpoint"`
	StatusHandler func(Endpoint) interface{}
}

// ServiceApiPrefix is the root of all control subjects
const ServiceApiPrefix = "$SRV"

type ServiceVerb int64

const (
	SrvPing ServiceVerb = iota
	SrvStatus
	SrvInfo
	SrvSchema
)

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

// ServiceImpl is the internal implementation of a Service
type ServiceImpl struct {
	sync.Mutex
	ServiceConfig
	// subs
	reqSub   *Subscription
	internal map[string]*Subscription
	statuses map[string]*Stats
	stats    *ServiceStats
}

// addInternalHandler generates control handlers for a specific verb
// each request generates 3 subscriptions, on for the general verb
// affecting all services written with the framework, one that handles
// all services of a particular kind, and finally a specific service.
func (svc *ServiceImpl) addInternalHandler(nc *Conn, verb ServiceVerb, handler MsgHandler) error {
	name := fmt.Sprintf("%s-all", verb.String())
	if err := svc._addInternalHandler(nc, verb, "", "", name, handler); err != nil {
		return err
	}
	name = fmt.Sprintf("%s-kind", verb.String())
	if err := svc._addInternalHandler(nc, verb, svc.Name(), "", name, handler); err != nil {
		return err
	}
	return svc._addInternalHandler(nc, verb, svc.Name(), svc.ID(), verb.String(), handler)
}

// _addInternalHandler registers a control subject handler
func (svc *ServiceImpl) _addInternalHandler(nc *Conn, verb ServiceVerb, kind string, id string, name string, handler MsgHandler) error {
	subj, err := SvcControlSubject(verb, kind, id)
	if err != nil {
		svc.Stop()
		return err
	}

	svc.internal[name], err = nc.Subscribe(subj, func(msg *Msg) {
		start := time.Now()
		defer func() {
			svc.Lock()
			stats := svc.statuses[name]
			stats.NumRequests++
			stats.TotalLatency += time.Since(start)
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

// AddService adds a microservice.
// NOTE we can do an OpenAPI version as well, but looking at it it was very involved. So I think keep simple version and
// also have a version that talkes full blown OpenAPI spec and we can pull these things out.
func (nc *Conn) AddService(config ServiceConfig) (Service, error) {
	if err := config.Valid(); err != nil {
		return nil, err
	}

	svc := &ServiceImpl{ServiceConfig: config}
	svc.internal = make(map[string]*Subscription)
	svc.statuses = make(map[string]*Stats)
	svc.statuses[""] = &Stats{
		Name: config.Name,
	}

	svc.stats = &ServiceStats{
		Name:    config.Name,
		ID:      config.Id,
		Version: config.Version,
		Started: time.Now(),
	}

	// Setup internal subscriptions.
	var err error

	svc.reqSub, err = nc.QueueSubscribe(config.Endpoint.Subject, QG, func(m *Msg) {
		svc.reqHandler(m)
	})
	if err != nil {
		return nil, err
	}

	info := &ServiceInfo{
		Name:        config.Name,
		Id:          config.Id,
		Description: config.Description,
		Version:     config.Version,
		Subject:     config.Endpoint.Subject,
	}

	infoHandler := func(m *Msg) {
		response, _ := json.MarshalIndent(info, "", "  ")
		m.Respond(response)
	}

	pingHandler := func(m *Msg) {
		infoHandler(m)
	}

	statusHandler := func(m *Msg) {
		response, _ := json.MarshalIndent(svc.Stats(), "", "  ")
		m.Respond(response)
	}

	schemaHandler := func(m *Msg) {
		response, _ := json.MarshalIndent(svc.ServiceConfig.Schema, "", "  ")
		m.Respond(response)
	}

	if err := svc.addInternalHandler(nc, SrvInfo, infoHandler); err != nil {
		return nil, err
	}
	if err := svc.addInternalHandler(nc, SrvPing, pingHandler); err != nil {
		return nil, err
	}
	if err := svc.addInternalHandler(nc, SrvStatus, statusHandler); err != nil {
		return nil, err
	}

	if svc.ServiceConfig.Schema.Request != "" || svc.ServiceConfig.Schema.Response != "" {
		if err := svc.addInternalHandler(nc, SrvSchema, schemaHandler); err != nil {
			return nil, err
		}
	}

	svc.stats.ID = svc.Id
	svc.stats.Started = time.Now()
	return svc, nil
}

// reqHandler itself
func (svc *ServiceImpl) reqHandler(req *Msg) {
	start := time.Now()
	defer func() {
		svc.Lock()
		stats := svc.statuses[""]
		stats.NumRequests++
		stats.TotalLatency += time.Since(start)
		svc.Unlock()
	}()

	if err := svc.ServiceConfig.Endpoint.Handler(svc, req); err != nil {
		svc.Lock()
		stats := svc.statuses[""]
		stats.NumErrors++
		svc.Unlock()
		req.Sub.mu.Lock()
		nc := req.Sub.conn
		req.Sub.mu.Unlock()

		hdr := []byte(fmt.Sprintf("NATS/1.0 500 %s\r\n\r\n", err.Error()))
		nc.publish(req.Reply, _EMPTY_, hdr, nil)
	}
}

func (svc *ServiceImpl) Stop() {
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

func (svc *ServiceImpl) ID() string {
	return svc.ServiceConfig.Id
}

func (svc *ServiceImpl) Name() string {
	return svc.ServiceConfig.Name
}

func (svc *ServiceImpl) Description() string {
	return svc.ServiceConfig.Description
}

func (svc *ServiceImpl) Version() string {
	return svc.ServiceConfig.Version
}

func (svc *ServiceImpl) Stats() ServiceStats {
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

func (svc *ServiceImpl) Reset() {
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
		return "", fmt.Errorf("unsupported ServiceImpl verb")
	}
	kind = strings.ToUpper(kind)
	id = strings.ToUpper(id)
	if kind == "" && id == "" {
		return fmt.Sprintf("%s.%s", ServiceApiPrefix, sverb), nil
	}
	if id == "" {
		return fmt.Sprintf("%s.%s.%s", ServiceApiPrefix, sverb, kind), nil
	}
	return fmt.Sprintf("%s.%s.%s.%s", ServiceApiPrefix, sverb, kind, id), nil
}
