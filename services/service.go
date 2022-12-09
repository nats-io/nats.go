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
	"regexp"
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

	// Service is an interface for service management.
	// It exposes methods to stop/reset a service, as well as get information on a service.
	Service interface {
		ID() string
		Name() string
		Description() string
		Version() string
		Stats() Stats
		Reset()
		Stop() error
	}

	// RequestHandler is a function used as a Handler for a service
	RequestHandler func(*nats.Msg)

	ErrHandler func(Service, *Error)

	DoneHandler func(Service)

	// Clients can request as well.
	Stats struct {
		Name      string          `json:"name"`
		ID        string          `json:"id"`
		Version   string          `json:"version"`
		Endpoints []EndpointStats `json:"stats"`
	}

	EndpointStats struct {
		Name                  string        `json:"name"`
		NumRequests           int           `json:"num_requests"`
		NumErrors             int           `json:"num_errors"`
		TotalProcessingTime   time.Duration `json:"total_processing_time"`
		AverageProcessingTime time.Duration `json:"average_processing_time"`
		Data                  interface{}   `json:"data"`
	}

	Ping struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	}

	// Info is the basic information about a service type
	Info struct {
		Name        string `json:"name"`
		ID          string `json:"id"`
		Description string `json:"description"`
		Version     string `json:"version"`
		Subject     string `json:"subject"`
	}

	Schema struct {
		Request  string `json:"request"`
		Response string `json:"response"`
	}

	Endpoint struct {
		Subject string `json:"subject"`
		Handler RequestHandler
	}

	Verb int64

	Config struct {
		Name         string   `json:"name"`
		Version      string   `json:"version"`
		Description  string   `json:"description"`
		Schema       Schema   `json:"schema"`
		Endpoint     Endpoint `json:"endpoint"`
		StatsHandler func(Endpoint) interface{}
		DoneHandler  DoneHandler
		ErrorHandler ErrHandler
	}

	Error struct {
		Subject     string
		Description string
	}

	// service is the internal implementation of a Service
	service struct {
		sync.Mutex
		Config
		id            string
		reqSub        *nats.Subscription
		verbSubs      map[string]*nats.Subscription
		endpointStats map[string]*EndpointStats
		conn          *nats.Conn
		natsHandlers  handlers
		stopped       bool
	}

	handlers struct {
		closed   nats.ConnHandler
		asyncErr nats.ErrHandler
	}
)

const (
	// Queue Group name used across all services
	QG = "q"

	// APIPrefix is the root of all control subjects
	APIPrefix = "$SRV"
)

// Service Error headers
const (
	ErrorHeader     = "Nats-Service-Error"
	ErrorCodeHeader = "Nats-Service-Error-Code"
)

const (
	PingVerb Verb = iota
	StatsVerb
	InfoVerb
	SchemaVerb
)

var (
	serviceNameRegexp = regexp.MustCompile(`^[A-Za-z0-9\-_]+$`)
)

var (
	ErrConfigValidation = errors.New("validation")
	ErrVerbNotSupported = errors.New("unsupported verb")
)

func (s *Config) Valid() error {
	if !serviceNameRegexp.MatchString(s.Name) {
		return fmt.Errorf("%w: service name: name should not be empty and should consist of alphanumerical charactest, dashes and underscores", ErrConfigValidation)
	}
	return s.Endpoint.Valid()
}

func (e *Endpoint) Valid() error {
	if e.Subject == "" {
		return fmt.Errorf("%w: endpoint: subject is required", ErrConfigValidation)
	}
	if e.Handler == nil {
		return fmt.Errorf("%w: endpoint: handler is required", ErrConfigValidation)
	}
	return nil
}

func (s Verb) String() string {
	switch s {
	case PingVerb:
		return "PING"
	case StatsVerb:
		return "STATS"
	case InfoVerb:
		return "INFO"
	case SchemaVerb:
		return "SCHEMA"
	default:
		return ""
	}
}

// Add adds a microservice.
// It will enable internal common services (PING, STATS, INFO and SCHEMA) as well as
// the actual service handler on the subject provided in config.Endpoint
// A service name and Endpoint configuration are required to add a service.
// Add returns a [Service] interface, allowing service menagement.
// Each service is assigned a unique ID.
func Add(nc *nats.Conn, config Config) (Service, error) {
	if err := config.Valid(); err != nil {
		return nil, err
	}

	id := nuid.Next()
	svc := &service{
		Config: config,
		conn:   nc,
		id:     id,
	}
	svc.verbSubs = make(map[string]*nats.Subscription)
	svc.endpointStats = make(map[string]*EndpointStats)
	svc.endpointStats[""] = &EndpointStats{
		Name: config.Name,
	}

	// Setup internal subscriptions.
	var err error

	svc.reqSub, err = nc.QueueSubscribe(config.Endpoint.Subject, QG, func(m *nats.Msg) {
		svc.reqHandler(m)
	})
	if err != nil {
		return nil, err
	}

	info := &Info{
		Name:        config.Name,
		ID:          id,
		Description: config.Description,
		Version:     config.Version,
		Subject:     config.Endpoint.Subject,
	}

	ping := &Ping{
		Name: config.Name,
		ID:   id,
	}

	infoHandler := func(m *nats.Msg) {
		response, _ := json.Marshal(info)
		m.Respond(response)
	}

	pingHandler := func(m *nats.Msg) {
		response, _ := json.Marshal(ping)
		m.Respond(response)
	}

	statusHandler := func(m *nats.Msg) {
		response, _ := json.Marshal(svc.Stats())
		m.Respond(response)
	}

	schemaHandler := func(m *nats.Msg) {
		response, _ := json.Marshal(svc.Config.Schema)
		m.Respond(response)
	}

	if err := svc.verbHandlers(nc, InfoVerb, infoHandler); err != nil {
		return nil, err
	}
	if err := svc.verbHandlers(nc, PingVerb, pingHandler); err != nil {
		return nil, err
	}
	if err := svc.verbHandlers(nc, StatsVerb, statusHandler); err != nil {
		return nil, err
	}

	if svc.Config.Schema.Request != "" || svc.Config.Schema.Response != "" {
		if err := svc.verbHandlers(nc, SchemaVerb, schemaHandler); err != nil {
			return nil, err
		}
	}

	svc.natsHandlers.closed = nc.Opts.ClosedCB
	if nc.Opts.ClosedCB != nil {
		nc.SetClosedHandler(func(c *nats.Conn) {
			svc.Stop()
			if config.DoneHandler != nil {
				config.DoneHandler(svc)
			}
			svc.natsHandlers.closed(c)
		})
	} else {
		nc.SetClosedHandler(func(c *nats.Conn) {
			if err := svc.Stop(); err != nil {
			}
			if config.DoneHandler != nil {
				config.DoneHandler(svc)
			}
		})
	}

	svc.natsHandlers.asyncErr = nc.Opts.AsyncErrorCB
	if nc.Opts.AsyncErrorCB != nil {
		nc.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if config.ErrorHandler != nil {
				config.ErrorHandler(svc, &Error{
					Description: err.Error(),
					Subject:     s.Subject,
				})
			}
			svc.Stop()
			svc.natsHandlers.asyncErr(c, s, err)
		})
	} else {
		nc.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if config.ErrorHandler != nil {
				config.ErrorHandler(svc, &Error{
					Description: err.Error(),
					Subject:     s.Subject,
				})
			}
			svc.Stop()
		})
	}

	return svc, nil
}

// verbHandlers generates control handlers for a specific verb
// each request generates 3 subscriptions, one for the general verb
// affecting all services written with the framework, one that handles
// all services of a particular kind, and finally a specific service.
func (svc *service) verbHandlers(nc *nats.Conn, verb Verb, handler nats.MsgHandler) error {
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
func (svc *service) addInternalHandler(nc *nats.Conn, verb Verb, kind, id, name string, handler nats.MsgHandler) error {
	subj, err := ControlSubject(verb, kind, id)
	if err != nil {
		svc.Stop()
		return err
	}

	svc.verbSubs[name], err = nc.Subscribe(subj, func(msg *nats.Msg) {
		start := time.Now()
		handler(msg)

		svc.Lock()
		stats := svc.endpointStats[name]
		stats.NumRequests++
		stats.TotalProcessingTime += time.Since(start)
		stats.AverageProcessingTime = stats.TotalProcessingTime / time.Duration(stats.NumRequests)
		svc.Unlock()
	})
	if err != nil {
		svc.Stop()
		return err
	}

	svc.endpointStats[name] = &EndpointStats{
		Name: name,
	}
	return nil
}

// reqHandler itself
func (svc *service) reqHandler(req *nats.Msg) {
	start := time.Now()
	svc.Config.Endpoint.Handler(req)
	svc.Lock()
	stats := svc.endpointStats[""]
	stats.NumRequests++
	stats.TotalProcessingTime += time.Since(start)
	stats.AverageProcessingTime = stats.TotalProcessingTime / time.Duration(stats.NumRequests)

	if req.Header.Get(ErrorHeader) != "" {
		stats := svc.endpointStats[""]
		stats.NumErrors++
	}
	svc.Unlock()
}

func (svc *service) Stop() error {
	if svc.stopped {
		return nil
	}
	if svc.reqSub != nil {
		if err := svc.reqSub.Drain(); err != nil {
			return fmt.Errorf("draining subsctioption for request handler: %w", err)
		}
		svc.reqSub = nil
	}
	var keys []string
	for key, sub := range svc.verbSubs {
		keys = append(keys, key)
		if err := sub.Drain(); err != nil {
			return fmt.Errorf("draining subsctioption for subject %q: %w", sub.Subject, err)
		}
	}
	for _, key := range keys {
		delete(svc.verbSubs, key)
	}
	restoreAsyncHandlers(svc.conn, svc.natsHandlers)
	svc.stopped = true
	return nil
}

func restoreAsyncHandlers(nc *nats.Conn, handlers handlers) {
	nc.SetClosedHandler(handlers.closed)
	nc.SetErrorHandler(handlers.asyncErr)
}

func (svc *service) ID() string {
	return svc.id
}

func (svc *service) Name() string {
	return svc.Config.Name
}

func (svc *service) Description() string {
	return svc.Config.Description
}

func (svc *service) Version() string {
	return svc.Config.Version
}

func (svc *service) Stats() Stats {
	svc.Lock()
	defer func() {
		svc.Unlock()
	}()
	if svc.Config.StatsHandler != nil {
		stats := svc.endpointStats[""]
		stats.Data = svc.Config.StatsHandler(svc.Endpoint)
	}
	idx := 0
	v := make([]EndpointStats, len(svc.endpointStats))
	for _, se := range svc.endpointStats {
		v[idx] = *se
		idx++
	}
	return Stats{
		Name:      svc.Name(),
		ID:        svc.ID(),
		Version:   svc.Version(),
		Endpoints: v,
	}
}

func (svc *service) Reset() {
	for _, se := range svc.endpointStats {
		se.NumRequests = 0
		se.TotalProcessingTime = 0
		se.NumErrors = 0
		se.Data = nil
	}
}

// ControlSubject returns monitoring subjects used by the Service
func ControlSubject(verb Verb, kind, id string) (string, error) {
	verbStr := verb.String()
	if verbStr == "" {
		return "", fmt.Errorf("%w: %q", ErrVerbNotSupported, verbStr)
	}
	kind = strings.ToUpper(kind)
	if kind == "" && id == "" {
		return fmt.Sprintf("%s.%s", APIPrefix, verbStr), nil
	}
	if id == "" {
		return fmt.Sprintf("%s.%s.%s", APIPrefix, verbStr, kind), nil
	}
	return fmt.Sprintf("%s.%s.%s.%s", APIPrefix, verbStr, kind, id), nil
}

func SendError(req *nats.Msg, err ResponseError) {
	if req.Header == nil {
		req.Header = nats.Header{}
	}
	req.Header.Add(ErrorHeader, err.Description)
	req.Header.Add(ErrorCodeHeader, err.ErrorCode)
	req.RespondMsg(req)
}
