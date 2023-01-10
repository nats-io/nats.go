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
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
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
		Info(ctx context.Context) Info

		// Stats returns statistics for the service endpoint and all monitoring endpoints.
		Stats(ctx context.Context) Stats

		// Reset resets all statistics on a service instance.
		Reset(ctx context.Context)

		// Stop drains the endpoint subscriptions and marks the service as stopped.
		Stop(ctx context.Context) error

		// Stopped informs whether [Stop] was executed on the service.
		Stopped(ctx context.Context) bool
	}

	// ErrHandler is a function used to configure a custom error handler for a service,
	ErrHandler func(context.Context, Service, *NATSError)

	// DoneHandler is a function used to configure a custom done handler for a service.
	DoneHandler func(context.Context, Service)

	// StatsHandler is a function used to configure a custom STATS endpoint.
	// It should return a value which can be serialized to JSON.
	StatsHandler func(context.Context, Endpoint) interface{}

	// ServiceIdentity contains fields helping to identity a service instance.
	ServiceIdentity struct {
		Name    string `json:"name"`
		ID      string `json:"id"`
		Version string `json:"version"`
	}

	// Stats is the type returned by STATS monitoring endpoint.
	// It contains stats for a specific endpoint (either request handler or monitoring enpoints).
	Stats struct {
		ServiceIdentity
		Type                  string          `json:"type"`
		NumRequests           int             `json:"num_requests"`
		NumErrors             int             `json:"num_errors"`
		LastError             string          `json:"last_error"`
		ProcessingTime        time.Duration   `json:"processing_time"`
		AverageProcessingTime time.Duration   `json:"average_processing_time"`
		Started               time.Time       `json:"started"`
		Data                  json.RawMessage `json:"data,omitempty"`
	}

	// Ping is the response type for PING monitoring endpoint.
	Ping struct {
		ServiceIdentity
		Type string `json:"type"`
	}

	// Info is the basic information about a service type.
	Info struct {
		ServiceIdentity
		Type        string `json:"type"`
		Description string `json:"description"`
		Subject     string `json:"subject"`
	}

	// SchemaResp is the response value for SCHEMA requests.
	SchemaResp struct {
		ServiceIdentity
		Type   string `json:"type"`
		Schema Schema `json:"schema"`
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
		Handler Handler
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

const (
	InfoResponseType   = "io.nats.micro.v1.info_response"
	PingResponseType   = "io.nats.micro.v1.ping_response"
	StatsResponseType  = "io.nats.micro.v1.stats_response"
	SchemaResponseType = "io.nats.micro.v1.schema_response"
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
// A service name, version and Endpoint configuration are required to add a service.
// AddService returns a [Service] interface, allowing service management.
// Each service is assigned a unique ID.
func AddService(ctx context.Context, nc *nats.Conn, config Config) (Service, error) {
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
	svcIdentity := ServiceIdentity{
		Name:    config.Name,
		ID:      id,
		Version: config.Version,
	}
	svc.verbSubs = make(map[string]*nats.Subscription)
	svc.stats = &Stats{
		ServiceIdentity: svcIdentity,
	}

	svc.setupAsyncCallbacks()

	go svc.asyncDispatcher.asyncCBDispatcher()

	// Setup internal subscriptions.
	var err error

	svc.reqSub, err = nc.QueueSubscribe(config.Endpoint.Subject, QG, func(m *nats.Msg) {
		svc.reqHandler(ctx, &request{msg: m})
	})
	if err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}

	ping := Ping{
		ServiceIdentity: svcIdentity,
		Type:            PingResponseType,
	}

	infoHandler := func(ctx context.Context, req Request) {
		response, _ := json.Marshal(svc.Info(ctx))
		if err := req.Respond(response); err != nil {
			if err := req.Error("500", fmt.Sprintf("Error handling INFO request: %s", err), nil); err != nil && config.ErrorHandler != nil {
				svc.asyncDispatcher.push(func() { config.ErrorHandler(ctx, svc, &NATSError{req.Subject(), err.Error()}) })
			}
		}
	}

	pingHandler := func(ctx context.Context, req Request) {
		response, _ := json.Marshal(ping)
		if err := req.Respond(response); err != nil {
			if err := req.Error("500", fmt.Sprintf("Error handling PING request: %s", err), nil); err != nil && config.ErrorHandler != nil {
				svc.asyncDispatcher.push(func() { config.ErrorHandler(ctx, svc, &NATSError{req.Subject(), err.Error()}) })
			}
		}
	}

	statsHandler := func(ctx context.Context, req Request) {
		response, _ := json.Marshal(svc.Stats(ctx))
		if err := req.Respond(response); err != nil {
			if err := req.Error("500", fmt.Sprintf("Error handling STATS request: %s", err), nil); err != nil && config.ErrorHandler != nil {
				svc.asyncDispatcher.push(func() { config.ErrorHandler(ctx, svc, &NATSError{req.Subject(), err.Error()}) })
			}
		}
	}

	schema := SchemaResp{
		ServiceIdentity: svcIdentity,
		Schema:          config.Schema,
		Type:            SchemaResponseType,
	}
	schemaHandler := func(ctx context.Context, req Request) {
		response, _ := json.Marshal(schema)
		if err := req.Respond(response); err != nil {
			if err := req.Error("500", fmt.Sprintf("Error handling SCHEMA request: %s", err), nil); err != nil && config.ErrorHandler != nil {
				svc.asyncDispatcher.push(func() { config.ErrorHandler(ctx, svc, &NATSError{req.Subject(), err.Error()}) })
			}
		}
	}

	if err := svc.verbHandlers(ctx, nc, InfoVerb, infoHandler); err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}
	if err := svc.verbHandlers(ctx, nc, PingVerb, pingHandler); err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}
	if err := svc.verbHandlers(ctx, nc, StatsVerb, statsHandler); err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}

	if err := svc.verbHandlers(ctx, nc, SchemaVerb, schemaHandler); err != nil {
		svc.asyncDispatcher.close()
		return nil, err
	}
	svc.stats.Started = time.Now().UTC()

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
	ctx := context.Background()
	svc.m.Lock()
	defer svc.m.Unlock()
	svc.natsHandlers.closed = svc.conn.ClosedHandler()
	if svc.natsHandlers.closed != nil {
		svc.conn.SetClosedHandler(func(c *nats.Conn) {
			svc.Stop(ctx)
			svc.natsHandlers.closed(c)
		})
	} else {
		svc.conn.SetClosedHandler(func(c *nats.Conn) {
			svc.Stop(ctx)
		})
	}

	svc.natsHandlers.asyncErr = svc.conn.ErrorHandler()
	if svc.natsHandlers.asyncErr != nil {
		svc.conn.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if !svc.matchSubscriptionSubject(s.Subject) {
				svc.natsHandlers.asyncErr(c, s, err)
			}
			if svc.Config.ErrorHandler != nil {
				svc.Config.ErrorHandler(ctx, svc, &NATSError{
					Subject:     s.Subject,
					Description: err.Error(),
				})
			}
			svc.m.Lock()
			svc.stats.NumErrors++
			svc.stats.LastError = err.Error()
			svc.m.Unlock()
			svc.Stop(ctx)
			svc.natsHandlers.asyncErr(c, s, err)
		})
	} else {
		svc.conn.SetErrorHandler(func(c *nats.Conn, s *nats.Subscription, err error) {
			if !svc.matchSubscriptionSubject(s.Subject) {
				return
			}
			if svc.Config.ErrorHandler != nil {
				svc.Config.ErrorHandler(ctx, svc, &NATSError{
					Subject:     s.Subject,
					Description: err.Error(),
				})
			}
			svc.m.Lock()
			svc.stats.NumErrors++
			svc.stats.LastError = err.Error()
			svc.m.Unlock()
			svc.Stop(ctx)
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
func (svc *service) verbHandlers(ctx context.Context, nc *nats.Conn, verb Verb, handler HandlerFunc) error {
	name := fmt.Sprintf("%s-all", verb.String())
	if err := svc.addInternalHandler(ctx, nc, verb, "", "", name, handler); err != nil {
		return err
	}
	name = fmt.Sprintf("%s-kind", verb.String())
	if err := svc.addInternalHandler(ctx, nc, verb, svc.Config.Name, "", name, handler); err != nil {
		return err
	}
	return svc.addInternalHandler(ctx, nc, verb, svc.Config.Name, svc.id, verb.String(), handler)
}

// addInternalHandler registers a control subject handler.
func (s *service) addInternalHandler(ctx context.Context, nc *nats.Conn, verb Verb, kind, id, name string, handler HandlerFunc) error {
	subj, err := ControlSubject(verb, kind, id)
	if err != nil {
		s.Stop(ctx)
		return err
	}

	s.verbSubs[name], err = nc.Subscribe(subj, func(msg *nats.Msg) {
		handler(ctx, &request{msg: msg})
	})
	if err != nil {
		s.Stop(ctx)
		return err
	}
	return nil
}

// reqHandler invokes the service request handler and modifies service stats
func (s *service) reqHandler(ctx context.Context, req *request) {
	start := time.Now()
	s.Endpoint.Handler.Handle(ctx, req)
	s.m.Lock()
	s.stats.NumRequests++
	s.stats.ProcessingTime += time.Since(start)
	avgProcessingTime := s.stats.ProcessingTime.Nanoseconds() / int64(s.stats.NumRequests)
	s.stats.AverageProcessingTime = time.Duration(avgProcessingTime)

	if req.respondError != nil {
		s.stats.NumErrors++
		s.stats.LastError = req.respondError.Error()
	}
	s.m.Unlock()
}

// Stop drains the endpoint subscriptions and marks the service as stopped.
func (s *service) Stop(ctx context.Context) error {
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
		s.asyncDispatcher.push(func() { s.DoneHandler(ctx, s) })
		s.asyncDispatcher.close()
	}
	return nil
}

func restoreAsyncHandlers(nc *nats.Conn, handlers handlers) {
	nc.SetClosedHandler(handlers.closed)
	nc.SetErrorHandler(handlers.asyncErr)
}

// Info returns information about the service
func (s *service) Info(ctx context.Context) Info {
	return Info{
		ServiceIdentity: ServiceIdentity{
			Name:    s.Config.Name,
			ID:      s.id,
			Version: s.Config.Version,
		},
		Type:        InfoResponseType,
		Description: s.Config.Description,
		Subject:     s.Config.Endpoint.Subject,
	}
}

// Stats returns statistics for the service endpoint and all monitoring endpoints.
func (s *service) Stats(ctx context.Context) Stats {
	s.m.Lock()
	defer s.m.Unlock()
	if s.StatsHandler != nil {
		s.stats.Data, _ = json.Marshal(s.StatsHandler(ctx, s.Endpoint))
	}
	info := s.Info(ctx)
	return Stats{
		ServiceIdentity: ServiceIdentity{
			Name:    info.Name,
			ID:      info.ID,
			Version: info.Version,
		},
		Type:                  StatsResponseType,
		NumRequests:           s.stats.NumRequests,
		NumErrors:             s.stats.NumErrors,
		ProcessingTime:        s.stats.ProcessingTime,
		AverageProcessingTime: s.stats.AverageProcessingTime,
		Started:               s.stats.Started,
		Data:                  s.stats.Data,
	}
}

// Reset resets all statistics on a service instance.
func (s *service) Reset(ctx context.Context) {
	s.m.Lock()
	s.stats = &Stats{
		ServiceIdentity: s.Info(ctx).ServiceIdentity,
	}
	s.m.Unlock()
}

// Stopped informs whether [Stop] was executed on the service.
func (s *service) Stopped(context.Context) bool {
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
