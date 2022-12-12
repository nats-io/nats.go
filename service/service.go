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

package service

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
	// Service represents a configured NATS service.
	// It should be created using [Add] in order to configure the appropriate NATS subscriptions
	// for request handler and monitoring.
	Service struct {
		// Config contains a configuration of the service
		Config

		m             sync.Mutex
		id            string
		reqSub        *nats.Subscription
		verbSubs      map[string]*nats.Subscription
		endpointStats map[string]*EndpointStats
		conn          *nats.Conn
		natsHandlers  handlers
		stopped       bool
	}

	// ErrHandler is a function used to configure a custom error handler for a service,
	ErrHandler func(*Service, *NATSError)

	// DoneHandler is a function used to configure a custom done handler for a service.
	DoneHandler func(*Service)

	// StatsHandleris a function used to configure a custom STATS endpoint.
	// It should return a value which can be serialized to JSON.
	StatsHandler func(Endpoint) interface{}

	// Stats is the type returned by STATS monitoring endpoint.
	// It contains a slice of [EndpointStats], providing stats
	// for both the service handler as well as all monitoring subjects.
	Stats struct {
		Name      string          `json:"name"`
		ID        string          `json:"id"`
		Version   string          `json:"version"`
		Endpoints []EndpointStats `json:"stats"`
	}

	// EndpointStats are stats for a specific endpoint (either request handler or monitoring enpoints).
	// It contains general statisctics for an endpoint, as well as a custom value returned by optional [StatsHandler].
	EndpointStats struct {
		Name                  string        `json:"name"`
		NumRequests           int           `json:"num_requests"`
		NumErrors             int           `json:"num_errors"`
		TotalProcessingTime   time.Duration `json:"total_processing_time"`
		AverageProcessingTime time.Duration `json:"average_processing_time"`
		Data                  interface{}   `json:"data"`
	}

	// Ping is the response type for PING monitoring endpoint.
	Ping struct {
		Name string `json:"name"`
		ID   string `json:"id"`
	}

	// Info is the basic information about a service type.
	Info struct {
		Name        string `json:"name"`
		ID          string `json:"id"`
		Description string `json:"description"`
		Version     string `json:"version"`
		Subject     string `json:"subject"`
	}

	// Schema can be used to configure a schema for a service.
	// It is olso returned by the SCHEMA monitoring service (if set).
	Schema struct {
		Request  string `json:"request"`
		Response string `json:"response"`
	}

	// Endpoint is used to configure a subject and handler for a service.
	Endpoint struct {
		Subject string `json:"subject"`
		Handler RequestHandler
	}

	// Verb represents a name of the monitoring service.
	Verb int64

	// Config is a configuration of a service.
	Config struct {
		Name         string   `json:"name"`
		Version      string   `json:"version"`
		Description  string   `json:"description"`
		Schema       Schema   `json:"schema"`
		Endpoint     Endpoint `json:"endpoint"`
		StatsHandler StatsHandler
		DoneHandler  DoneHandler
		ErrorHandler ErrHandler
	}

	// NATSError represents an error returned by a NATS Subscription.
	// It contains a subject on which the subscription failed, so that
	// it can be linked with a specific service endpoint.
	NATSError struct {
		Subject     string
		Description string
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

// Verbs being used to set up a specific control subject.
const (
	PingVerb Verb = iota
	StatsVerb
	InfoVerb
	SchemaVerb
)

var (
	serviceNameRegexp = regexp.MustCompile(`^[A-Za-z0-9\-_]+$`)
)

// Common errors returned by the Service framework.
var (
	// ErrConfigValidation is returned when service configuration is invalid
	ErrConfigValidation = errors.New("validation")

	// ErrVerbNotSupported is returned when invalid [Verb] is used (PING, SCHEMA, INFO, STATS)
	ErrVerbNotSupported = errors.New("unsupported verb")
)

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
func Add(nc *nats.Conn, config Config) (*Service, error) {
	if err := config.valid(); err != nil {
		return nil, err
	}

	id := nuid.Next()
	svc := &Service{
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
		svc.reqHandler(&Request{Msg: m})
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
		response, _ := json.Marshal(svc.Schema)
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

	if svc.Schema.Request != "" || svc.Schema.Response != "" {
		if err := svc.verbHandlers(nc, SchemaVerb, schemaHandler); err != nil {
			return nil, err
		}
	}

	svc.natsHandlers.closed = nc.Opts.ClosedCB
	if nc.Opts.ClosedCB != nil {
		nc.SetClosedHandler(func(c *nats.Conn) {
			svc.Stop()
			svc.natsHandlers.closed(c)
		})
	} else {
		nc.SetClosedHandler(func(c *nats.Conn) {
			svc.Stop()
		})
	}

	svc.natsHandlers.asyncErr = nc.Opts.AsyncErrorCB
	if nc.Opts.AsyncErrorCB != nil {
		nc.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if config.ErrorHandler != nil {
				config.ErrorHandler(svc, &NATSError{
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
				config.ErrorHandler(svc, &NATSError{
					Description: err.Error(),
					Subject:     s.Subject,
				})
			}
			svc.Stop()
		})
	}

	return svc, nil
}

func (s *Config) valid() error {
	if !serviceNameRegexp.MatchString(s.Name) {
		return fmt.Errorf("%w: service name: name should not be empty and should consist of alphanumerical charactest, dashes and underscores", ErrConfigValidation)
	}
	return s.Endpoint.valid()
}

func (e *Endpoint) valid() error {
	if e.Subject == "" {
		return fmt.Errorf("%w: endpoint: subject is required", ErrConfigValidation)
	}
	if e.Handler == nil {
		return fmt.Errorf("%w: endpoint: handler is required", ErrConfigValidation)
	}
	return nil
}

// verbHandlers generates control handlers for a specific verb.
// Each request generates 3 subscriptions, one for the general verb
// affecting all services written with the framework, one that handles
// all services of a particular kind, and finally a specific service instance.
func (svc *Service) verbHandlers(nc *nats.Conn, verb Verb, handler nats.MsgHandler) error {
	name := fmt.Sprintf("%s-all", verb.String())
	if err := svc.addInternalHandler(nc, verb, "", "", name, handler); err != nil {
		return err
	}
	name = fmt.Sprintf("%s-kind", verb.String())
	if err := svc.addInternalHandler(nc, verb, svc.Name, "", name, handler); err != nil {
		return err
	}
	return svc.addInternalHandler(nc, verb, svc.Name, svc.ID(), verb.String(), handler)
}

// addInternalHandler registers a control subject handler.
func (s *Service) addInternalHandler(nc *nats.Conn, verb Verb, kind, id, name string, handler nats.MsgHandler) error {
	subj, err := ControlSubject(verb, kind, id)
	if err != nil {
		s.Stop()
		return err
	}

	s.verbSubs[name], err = nc.Subscribe(subj, func(msg *nats.Msg) {
		start := time.Now()
		handler(msg)

		s.m.Lock()
		stats := s.endpointStats[name]
		stats.NumRequests++
		stats.TotalProcessingTime += time.Since(start)
		stats.AverageProcessingTime = stats.TotalProcessingTime / time.Duration(stats.NumRequests)
		s.m.Unlock()
	})
	if err != nil {
		s.Stop()
		return err
	}

	s.endpointStats[name] = &EndpointStats{
		Name: name,
	}
	return nil
}

// reqHandler itself
func (s *Service) reqHandler(req *Request) {
	start := time.Now()
	s.Endpoint.Handler(req)
	s.m.Lock()
	stats := s.endpointStats[""]
	stats.NumRequests++
	stats.TotalProcessingTime += time.Since(start)
	stats.AverageProcessingTime = stats.TotalProcessingTime / time.Duration(stats.NumRequests)

	if req.errResponse {
		stats := s.endpointStats[""]
		stats.NumErrors++
	}
	s.m.Unlock()
}

// Stop drains the endpoint subscriptions and marks the service as stopped.
func (s *Service) Stop() error {
	s.m.Lock()
	if s.stopped {
		return nil
	}
	defer s.m.Unlock()
	if s.reqSub != nil {
		if err := s.reqSub.Drain(); err != nil {
			return fmt.Errorf("draining subscription for request handler: %w", err)
		}
		s.reqSub = nil
	}
	var keys []string
	for key, sub := range s.verbSubs {
		keys = append(keys, key)
		if err := sub.Drain(); err != nil {
			return fmt.Errorf("draining subscription for subject %q: %w", sub.Subject, err)
		}
	}
	for _, key := range keys {
		delete(s.verbSubs, key)
	}
	restoreAsyncHandlers(s.conn, s.natsHandlers)
	s.stopped = true
	if s.DoneHandler != nil {
		s.DoneHandler(s)
	}
	return nil
}

func restoreAsyncHandlers(nc *nats.Conn, handlers handlers) {
	nc.SetClosedHandler(handlers.closed)
	nc.SetErrorHandler(handlers.asyncErr)
}

// ID returns the service instance's unique ID.
func (s *Service) ID() string {
	return s.id
}

// Stats returns statisctics for the service endpoint and all monitoring endpoints.
func (s *Service) Stats() Stats {
	s.m.Lock()
	defer s.m.Unlock()
	if s.StatsHandler != nil {
		stats := s.endpointStats[""]
		stats.Data = s.StatsHandler(s.Endpoint)
	}
	idx := 0
	v := make([]EndpointStats, len(s.endpointStats))
	for _, se := range s.endpointStats {
		v[idx] = *se
		idx++
	}
	return Stats{
		Name:      s.Name,
		ID:        s.ID(),
		Version:   s.Version,
		Endpoints: v,
	}
}

// Reset resets all statistics on a service instance.
func (s *Service) Reset() {
	s.m.Lock()
	for _, se := range s.endpointStats {
		se.NumRequests = 0
		se.TotalProcessingTime = 0
		se.AverageProcessingTime = 0
		se.Data = nil
		se.NumErrors = 0
		se.Data = nil
	}
	s.m.Unlock()
}

// Stopped informs whether [Stop] was executed on the service.
func (s *Service) Stopped() bool {
	s.m.Lock()
	defer s.m.Unlock()
	return s.stopped
}

// ControlSubject returns monitoring subjects used by the Service.
// Providing a verb is mandatory (it should be one of Ping, Schema, Info or Stats).
// Depending on whether kind and id are provided, ControlSubject will return one of the following:
//   - verb only: subject used to monitor all available services
//   - verb and kind: subject used to monitor services with the provided name
//   - verb, name and id: subject used to monitor an instance of a service with the provided ID
func ControlSubject(verb Verb, name, id string) (string, error) {
	verbStr := verb.String()
	if verbStr == "" {
		return "", fmt.Errorf("%w: %q", ErrVerbNotSupported, verbStr)
	}
	name = strings.ToUpper(name)
	if name == "" && id == "" {
		return fmt.Sprintf("%s.%s", APIPrefix, verbStr), nil
	}
	if id == "" {
		return fmt.Sprintf("%s.%s.%s", APIPrefix, verbStr, name), nil
	}
	return fmt.Sprintf("%s.%s.%s.%s", APIPrefix, verbStr, name, id), nil
}
