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

package micro

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
	Service interface {
		// Info returns the service info.
		Info() Info

		// Stats returns statisctics for the service endpoint and all monitoring endpoints.
		Stats() Stats

		// Reset resets all statistics on a service instance.
		Reset()

		// Stop drains the endpoint subscriptions and marks the service as stopped.
		Stop() error

		// Stopped informs whether [Stop] was executed on the service.
		Stopped() bool
	}

	// ErrHandler is a function used to configure a custom error handler for a service,
	ErrHandler func(Service, *NATSError)

	// DoneHandler is a function used to configure a custom done handler for a service.
	DoneHandler func(Service)

	// StatsHandleris a function used to configure a custom STATS endpoint.
	// It should return a value which can be serialized to JSON.
	StatsHandler func(Endpoint) interface{}

	// ServiceIdentity contains fields helping to identidy a service instance.
	ServiceIdentity struct {
		Name    string `json:"name"`
		ID      string `json:"id"`
		Version string `json:"version"`
	}

	// Stats is the type returned by STATS monitoring endpoint.
	// It contains stats for a specific endpoint (either request handler or monitoring enpoints).
	Stats struct {
		ServiceIdentity
		NumRequests           int           `json:"num_requests"`
		NumErrors             int           `json:"num_errors"`
		TotalProcessingTime   time.Duration `json:"total_processing_time"`
		AverageProcessingTime time.Duration `json:"average_processing_time"`
		Started               string        `json:"started"`
		Data                  interface{}   `json:"data,omitempty"`
	}

	// Ping is the response type for PING monitoring endpoint.
	Ping ServiceIdentity

	// Info is the basic information about a service type.
	Info struct {
		ServiceIdentity
		Description string `json:"description"`
		Subject     string `json:"subject"`
	}

	// SchemaResp is the response value for SCHEMA requests.
	SchemaResp struct {
		ServiceIdentity
		Schema
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

	// service represents a configured NATS service.
	// It should be created using [Add] in order to configure the appropriate NATS subscriptions
	// for request handler and monitoring.
	service struct {
		// Config contains a configuration of the service
		Config

		m            sync.Mutex
		id           string
		reqSub       *nats.Subscription
		verbSubs     map[string]*nats.Subscription
		stats        *Stats
		conn         *nats.Conn
		natsHandlers handlers
		stopped      bool

		asyncDispatcher asyncCallbacksHandler
	}

	handlers struct {
		closed   nats.ConnHandler
		asyncErr nats.ErrHandler
	}

	asyncCallbacksHandler struct {
		cbQueue chan func()
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
	// this regular expression is suggested regexp for semver validation: https://semver.org/
	semVerRegexp      = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)
	serviceNameRegexp = regexp.MustCompile(`^[A-Za-z0-9\-_]+$`)
)

// Common errors returned by the Service framework.
var (
	// ErrConfigValidation is returned when service configuration is invalid
	ErrConfigValidation = errors.New("validation")

	// ErrVerbNotSupported is returned when invalid [Verb] is used (PING, SCHEMA, INFO, STATS)
	ErrVerbNotSupported = errors.New("unsupported verb")

	// ErrServiceNameRequired is returned when attempting to generate control subject with ID but empty name
	ErrServiceNameRequired = errors.New("service name is required to generate ID control subject")
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

// AddService adds a microservice.
// It will enable internal common services (PING, STATS, INFO and SCHEMA) as well as
// the actual service handler on the subject provided in config.Endpoint
// A service name and Endpoint configuration are required to add a service.
// AddService returns a [Service] interface, allowing service menagement.
// Each service is assigned a unique ID.
func AddService(nc *nats.Conn, config Config) (Service, error) {
	if err := config.valid(); err != nil {
		return nil, err
	}

	id := nuid.Next()
	svc := &service{
		Config: config,
		conn:   nc,
		id:     id,
		asyncDispatcher: asyncCallbacksHandler{
			cbQueue: make(chan func(), 100),
		},
	}
	svc.verbSubs = make(map[string]*nats.Subscription)
	svc.stats = &Stats{
		ServiceIdentity: ServiceIdentity{
			Name:    config.Name,
			ID:      id,
			Version: config.Version,
		},
	}

	svc.setupAsyncCallbacks()

	go svc.asyncDispatcher.asyncCBDispatcher()

	// Setup internal subscriptions.
	var err error

	svc.reqSub, err = nc.QueueSubscribe(config.Endpoint.Subject, QG, func(m *nats.Msg) {
		svc.reqHandler(&Request{Msg: m})
	})
	if err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}

	ping := &Ping{
		Name:    config.Name,
		ID:      id,
		Version: config.Version,
	}

	infoHandler := func(req *Request) {
		response, _ := json.Marshal(svc.Info())
		if err := req.Respond(response); err != nil {
			if err := req.Error("500", fmt.Sprintf("Error handling INFO request: %s", err), nil); err != nil && config.ErrorHandler != nil {
				svc.asyncDispatcher.push(func() { config.ErrorHandler(svc, &NATSError{req.Subject, err.Error()}) })
			}
		}
	}

	pingHandler := func(req *Request) {
		response, _ := json.Marshal(ping)
		if err := req.Respond(response); err != nil {
			if err := req.Error("500", fmt.Sprintf("Error handling PING request: %s", err), nil); err != nil && config.ErrorHandler != nil {
				svc.asyncDispatcher.push(func() { config.ErrorHandler(svc, &NATSError{req.Subject, err.Error()}) })
			}
		}
	}

	statsHandler := func(req *Request) {
		response, _ := json.Marshal(svc.Stats())
		if err := req.Respond(response); err != nil {
			if err := req.Error("500", fmt.Sprintf("Error handling STATS request: %s", err), nil); err != nil && config.ErrorHandler != nil {
				svc.asyncDispatcher.push(func() { config.ErrorHandler(svc, &NATSError{req.Subject, err.Error()}) })
			}
		}
	}

	schemaHandler := func(req *Request) {
		response, _ := json.Marshal(svc.Schema)
		if err := req.Respond(response); err != nil {
			if err := req.Error("500", fmt.Sprintf("Error handling SCHEMA request: %s", err), nil); err != nil && config.ErrorHandler != nil {
				svc.asyncDispatcher.push(func() { config.ErrorHandler(svc, &NATSError{req.Subject, err.Error()}) })
			}
		}
	}

	if err := svc.verbHandlers(nc, InfoVerb, infoHandler); err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}
	if err := svc.verbHandlers(nc, PingVerb, pingHandler); err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}
	if err := svc.verbHandlers(nc, StatsVerb, statsHandler); err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}

	if svc.Schema.Request != "" || svc.Schema.Response != "" {
		if err := svc.verbHandlers(nc, SchemaVerb, schemaHandler); err != nil {
			svc.asyncDispatcher.close()
			return nil, err
		}
	}
	svc.stats.Started = time.Now().Format(time.RFC3339)

	return svc, nil
}

// dispatch is responsible for calling any async callbacks
func (ac *asyncCallbacksHandler) asyncCBDispatcher() {
	for {
		f := <-ac.cbQueue
		if f == nil {
			return
		}
		f()
	}
}

// dispatch is responsible for calling any async callbacks
func (ac *asyncCallbacksHandler) push(f func()) {
	ac.cbQueue <- f
}

func (ac *asyncCallbacksHandler) close() {
	close(ac.cbQueue)
}

func (s *Config) valid() error {
	if !serviceNameRegexp.MatchString(s.Name) {
		return fmt.Errorf("%w: service name: name should not be empty and should consist of alphanumerical charactest, dashes and underscores", ErrConfigValidation)
	}
	if !semVerRegexp.MatchString(s.Version) {
		return fmt.Errorf("%w: version: version should not be empty should match the SemVer format", ErrConfigValidation)
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

func (svc *service) setupAsyncCallbacks() {
	svc.natsHandlers.closed = svc.conn.ClosedHandler()
	if svc.natsHandlers.closed != nil {
		svc.conn.SetClosedHandler(func(c *nats.Conn) {
			svc.Stop()
			svc.natsHandlers.closed(c)
		})
	} else {
		svc.conn.SetClosedHandler(func(c *nats.Conn) {
			svc.Stop()
		})
	}

	svc.natsHandlers.asyncErr = svc.conn.ErrorHandler()
	if svc.natsHandlers.asyncErr != nil {
		svc.conn.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if !svc.matchSubscriptionSubject(s.Subject) {
				svc.natsHandlers.asyncErr(c, s, err)
			}
			if svc.Config.ErrorHandler != nil {
				svc.Config.ErrorHandler(svc, &NATSError{
					Subject:     s.Subject,
					Description: err.Error(),
				})
			}
			svc.Stop()
			svc.natsHandlers.asyncErr(c, s, err)
		})
	} else {
		svc.conn.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if !svc.matchSubscriptionSubject(s.Subject) {
				return
			}
			if svc.Config.ErrorHandler != nil {
				svc.Config.ErrorHandler(svc, &NATSError{
					Subject:     s.Subject,
					Description: err.Error(),
				})
			}
			svc.Stop()
		})
	}
}

func (svc *service) matchSubscriptionSubject(subj string) bool {
	if svc.reqSub.Subject == subj {
		return true
	}
	for _, verbSub := range svc.verbSubs {
		if verbSub.Subject == subj {
			return true
		}
	}
	return false
}

// verbHandlers generates control handlers for a specific verb.
// Each request generates 3 subscriptions, one for the general verb
// affecting all services written with the framework, one that handles
// all services of a particular kind, and finally a specific service instance.
func (svc *service) verbHandlers(nc *nats.Conn, verb Verb, handler RequestHandler) error {
	name := fmt.Sprintf("%s-all", verb.String())
	if err := svc.addInternalHandler(nc, verb, "", "", name, handler); err != nil {
		return err
	}
	name = fmt.Sprintf("%s-kind", verb.String())
	if err := svc.addInternalHandler(nc, verb, svc.Config.Name, "", name, handler); err != nil {
		return err
	}
	return svc.addInternalHandler(nc, verb, svc.Config.Name, svc.id, verb.String(), handler)
}

// addInternalHandler registers a control subject handler.
func (s *service) addInternalHandler(nc *nats.Conn, verb Verb, kind, id, name string, handler RequestHandler) error {
	subj, err := ControlSubject(verb, kind, id)
	if err != nil {
		s.Stop()
		return err
	}

	s.verbSubs[name], err = nc.Subscribe(subj, func(msg *nats.Msg) {
		handler(&Request{Msg: msg})
	})
	if err != nil {
		s.Stop()
		return err
	}
	return nil
}

// reqHandler itself
func (s *service) reqHandler(req *Request) {
	start := time.Now()
	s.Endpoint.Handler(req)
	s.m.Lock()
	s.stats.NumRequests++
	s.stats.TotalProcessingTime += time.Since(start)
	avgProcessingTime := s.stats.TotalProcessingTime.Nanoseconds() / int64(s.stats.NumRequests)
	s.stats.AverageProcessingTime = time.Duration(avgProcessingTime)

	if req.errResponse {
		s.stats.NumErrors++
	}
	s.m.Unlock()
}

// Stop drains the endpoint subscriptions and marks the service as stopped.
func (s *service) Stop() error {
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
		s.asyncDispatcher.push(func() { s.DoneHandler(s) })
		s.asyncDispatcher.close()
	}
	return nil
}

func restoreAsyncHandlers(nc *nats.Conn, handlers handlers) {
	nc.SetClosedHandler(handlers.closed)
	nc.SetErrorHandler(handlers.asyncErr)
}

// ID returns the service instance's unique ID.
func (s *service) Info() Info {
	return Info{
		ServiceIdentity: ServiceIdentity{
			Name:    s.Config.Name,
			ID:      s.id,
			Version: s.Config.Version,
		},
		Description: s.Config.Description,
		Subject:     s.Config.Endpoint.Subject,
	}
}

// Stats returns statisctics for the service endpoint and all monitoring endpoints.
func (s *service) Stats() Stats {
	s.m.Lock()
	defer s.m.Unlock()
	if s.StatsHandler != nil {
		s.stats.Data = s.StatsHandler(s.Endpoint)
	}
	info := s.Info()
	return Stats{
		ServiceIdentity: ServiceIdentity{
			Name:    info.Name,
			ID:      info.ID,
			Version: info.Version,
		},
		NumRequests:           s.stats.NumRequests,
		NumErrors:             s.stats.NumErrors,
		TotalProcessingTime:   s.stats.TotalProcessingTime,
		AverageProcessingTime: s.stats.AverageProcessingTime,
		Started:               s.stats.Started,
		Data:                  s.stats.Data,
	}
}

// Reset resets all statistics on a service instance.
func (s *service) Reset() {
	s.m.Lock()
	s.stats = &Stats{
		ServiceIdentity: s.Info().ServiceIdentity,
	}
	s.m.Unlock()
}

// Stopped informs whether [Stop] was executed on the service.
func (s *service) Stopped() bool {
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
	if name == "" && id != "" {
		return "", ErrServiceNameRequired
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

func (e *NATSError) Error() string {
	return fmt.Sprintf("%q: %s", e.Subject, e.Description)
}
