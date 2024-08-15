// Copyright 2022-2023 The NATS Authors
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

	// Service exposes methods to operate on a service instance.
	Service interface {
		// AddEndpoint registers endpoint with given name on a specific subject.
		AddEndpoint(string, Handler, ...EndpointOpt) error

		// AddGroup returns a Group interface, allowing for more complex endpoint topologies.
		// A group can be used to register endpoints with given prefix.
		AddGroup(string, ...GroupOpt) Group

		// Info returns the service info.
		Info() Info

		// Stats returns statistics for the service endpoint and all monitoring endpoints.
		Stats() Stats

		// Reset resets all statistics (for all endpoints) on a service instance.
		Reset()

		// Stop drains the endpoint subscriptions and marks the service as stopped.
		Stop() error

		// Stopped informs whether [Stop] was executed on the service.
		Stopped() bool
	}

	// Group allows for grouping endpoints on a service.
	//
	// Endpoints created using AddEndpoint will be grouped under common prefix (group name)
	// New groups can also be derived from a group using AddGroup.
	Group interface {
		// AddGroup creates a new group, prefixed by this group's prefix.
		AddGroup(string, ...GroupOpt) Group

		// AddEndpoint registers new endpoints on a service.
		// The endpoint's subject will be prefixed with the group prefix.
		AddEndpoint(string, Handler, ...EndpointOpt) error
	}

	EndpointOpt func(*endpointOpts) error
	GroupOpt    func(*groupOpts)

	endpointOpts struct {
		subject    string
		metadata   map[string]string
		queueGroup string
	}

	groupOpts struct {
		queueGroup string
	}

	// ErrHandler is a function used to configure a custom error handler for a service,
	ErrHandler func(Service, *NATSError)

	// DoneHandler is a function used to configure a custom done handler for a service.
	DoneHandler func(Service)

	// StatsHandler is a function used to configure a custom STATS endpoint.
	// It should return a value which can be serialized to JSON.
	StatsHandler func(*Endpoint) any

	// ServiceIdentity contains fields helping to identity a service instance.
	ServiceIdentity struct {
		Name     string            `json:"name"`
		ID       string            `json:"id"`
		Version  string            `json:"version"`
		Metadata map[string]string `json:"metadata"`
	}

	// Stats is the type returned by STATS monitoring endpoint.
	// It contains stats of all registered endpoints.
	Stats struct {
		ServiceIdentity
		Type      string           `json:"type"`
		Started   time.Time        `json:"started"`
		Endpoints []*EndpointStats `json:"endpoints"`
	}

	// EndpointStats contains stats for a specific endpoint.
	EndpointStats struct {
		Name                  string          `json:"name"`
		Subject               string          `json:"subject"`
		QueueGroup            string          `json:"queue_group"`
		NumRequests           int             `json:"num_requests"`
		NumErrors             int             `json:"num_errors"`
		LastError             string          `json:"last_error"`
		ProcessingTime        time.Duration   `json:"processing_time"`
		AverageProcessingTime time.Duration   `json:"average_processing_time"`
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
		Type        string         `json:"type"`
		Description string         `json:"description"`
		Endpoints   []EndpointInfo `json:"endpoints"`
	}

	EndpointInfo struct {
		Name       string            `json:"name"`
		Subject    string            `json:"subject"`
		QueueGroup string            `json:"queue_group"`
		Metadata   map[string]string `json:"metadata"`
	}

	// Endpoint manages a service endpoint.
	Endpoint struct {
		EndpointConfig
		Name string

		service *service

		stats        EndpointStats
		subscription *nats.Subscription
	}

	group struct {
		service    *service
		prefix     string
		queueGroup string
	}

	// Verb represents a name of the monitoring service.
	Verb int64

	// Config is a configuration of a service.
	Config struct {
		// Name represents the name of the service.
		Name string `json:"name"`

		// Endpoint is an optional endpoint configuration.
		// More complex, multi-endpoint services can be configured using
		// Service.AddGroup and Service.AddEndpoint methods.
		Endpoint *EndpointConfig `json:"endpoint"`

		// Version is a SemVer compatible version string.
		Version string `json:"version"`

		// Description of the service.
		Description string `json:"description"`

		// Metadata annotates the service
		Metadata map[string]string `json:"metadata,omitempty"`

		// QueueGroup can be used to override the default queue group name.
		QueueGroup string `json:"queue_group"`

		// StatsHandler is a user-defined custom function.
		// used to calculate additional service stats.
		StatsHandler StatsHandler

		// DoneHandler is invoked when all service subscription are stopped.
		DoneHandler DoneHandler

		// ErrorHandler is invoked on any nats-related service error.
		ErrorHandler ErrHandler
	}

	EndpointConfig struct {
		// Subject on which the endpoint is registered.
		Subject string

		// Handler used by the endpoint.
		Handler Handler

		// Metadata annotates the service
		Metadata map[string]string `json:"metadata,omitempty"`

		// QueueGroup can be used to override the default queue group name.
		QueueGroup string `json:"queue_group"`
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
		endpoints    []*Endpoint
		verbSubs     map[string]*nats.Subscription
		started      time.Time
		nc           *nats.Conn
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
	DefaultQueueGroup = "q"

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
)

const (
	InfoResponseType  = "io.nats.micro.v1.info_response"
	PingResponseType  = "io.nats.micro.v1.ping_response"
	StatsResponseType = "io.nats.micro.v1.stats_response"
)

var (
	// this regular expression is suggested regexp for semver validation: https://semver.org/
	semVerRegexp  = regexp.MustCompile(`^(0|[1-9]\d*)\.(0|[1-9]\d*)\.(0|[1-9]\d*)(?:-((?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*)(?:\.(?:0|[1-9]\d*|\d*[a-zA-Z-][0-9a-zA-Z-]*))*))?(?:\+([0-9a-zA-Z-]+(?:\.[0-9a-zA-Z-]+)*))?$`)
	nameRegexp    = regexp.MustCompile(`^[A-Za-z0-9\-_]+$`)
	subjectRegexp = regexp.MustCompile(`^[^ >]*[>]?$`)
)

// Common errors returned by the Service framework.
var (
	// ErrConfigValidation is returned when service configuration is invalid
	ErrConfigValidation = errors.New("validation")

	// ErrVerbNotSupported is returned when invalid [Verb] is used (PING, INFO, STATS)
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
	default:
		return ""
	}
}

// AddService adds a microservice.
// It will enable internal common services (PING, STATS and INFO).
// Request handlers have to be registered separately using Service.AddEndpoint.
// A service name, version and Endpoint configuration are required to add a service.
// AddService returns a [Service] interface, allowing service management.
// Each service is assigned a unique ID.
func AddService(nc *nats.Conn, config Config) (Service, error) {
	if err := config.valid(); err != nil {
		return nil, err
	}

	if config.Metadata == nil {
		config.Metadata = map[string]string{}
	}

	id := nuid.Next()
	svc := &service{
		Config: config,
		nc:     nc,
		id:     id,
		asyncDispatcher: asyncCallbacksHandler{
			cbQueue: make(chan func(), 100),
		},
		verbSubs:  make(map[string]*nats.Subscription),
		endpoints: make([]*Endpoint, 0),
	}

	// Add connection event (closed, error) wrapper handlers. If the service has
	// custom callbacks, the events are queued and invoked by the same
	// goroutine, starting now.
	go svc.asyncDispatcher.run()
	svc.wrapConnectionEventCallbacks()

	if config.Endpoint != nil {
		opts := []EndpointOpt{WithEndpointSubject(config.Endpoint.Subject)}
		if config.Endpoint.Metadata != nil {
			opts = append(opts, WithEndpointMetadata(config.Endpoint.Metadata))
		}
		if config.Endpoint.QueueGroup != "" {
			opts = append(opts, WithEndpointQueueGroup(config.Endpoint.QueueGroup))
		} else if config.QueueGroup != "" {
			opts = append(opts, WithEndpointQueueGroup(config.QueueGroup))
		}
		if err := svc.AddEndpoint("default", config.Endpoint.Handler, opts...); err != nil {
			return nil, err
		}
	}

	// Setup internal subscriptions.
	pingResponse := Ping{
		ServiceIdentity: svc.serviceIdentity(),
		Type:            PingResponseType,
	}

	handleVerb := func(verb Verb, valuef func() any) func(req Request) {
		return func(req Request) {
			response, _ := json.Marshal(valuef())
			if err := req.Respond(response); err != nil {
				if err := req.Error("500", fmt.Sprintf("Error handling %s request: %s", verb, err), nil); err != nil && config.ErrorHandler != nil {
					svc.asyncDispatcher.push(func() { config.ErrorHandler(svc, &NATSError{req.Subject(), err.Error()}) })
				}
			}
		}
	}

	for verb, source := range map[Verb]func() any{
		InfoVerb:  func() any { return svc.Info() },
		PingVerb:  func() any { return pingResponse },
		StatsVerb: func() any { return svc.Stats() },
	} {
		handler := handleVerb(verb, source)
		if err := svc.addVerbHandlers(nc, verb, handler); err != nil {
			svc.asyncDispatcher.close()
			return nil, err
		}
	}

	svc.started = time.Now().UTC()
	return svc, nil
}

func (s *service) AddEndpoint(name string, handler Handler, opts ...EndpointOpt) error {
	var options endpointOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return err
		}
	}
	subject := name
	if options.subject != "" {
		subject = options.subject
	}
	queueGroup := queueGroupName(options.queueGroup, s.Config.QueueGroup)
	return addEndpoint(s, name, subject, handler, options.metadata, queueGroup)
}

func addEndpoint(s *service, name, subject string, handler Handler, metadata map[string]string, queueGroup string) error {
	if !nameRegexp.MatchString(name) {
		return fmt.Errorf("%w: invalid endpoint name", ErrConfigValidation)
	}
	if !subjectRegexp.MatchString(subject) {
		return fmt.Errorf("%w: invalid endpoint subject", ErrConfigValidation)
	}
	if !subjectRegexp.MatchString(queueGroup) {
		return fmt.Errorf("%w: invalid endpoint queue group", ErrConfigValidation)
	}
	endpoint := &Endpoint{
		service: s,
		EndpointConfig: EndpointConfig{
			Subject:    subject,
			Handler:    handler,
			Metadata:   metadata,
			QueueGroup: queueGroup,
		},
		Name: name,
	}

	sub, err := s.nc.QueueSubscribe(
		subject,
		queueGroup,
		func(m *nats.Msg) {
			s.reqHandler(endpoint, &request{msg: m})
		},
	)
	if err != nil {
		return err
	}
	s.m.Lock()
	endpoint.subscription = sub
	s.endpoints = append(s.endpoints, endpoint)
	endpoint.stats = EndpointStats{
		Name:       name,
		Subject:    subject,
		QueueGroup: queueGroup,
	}
	s.m.Unlock()
	return nil
}

func (s *service) AddGroup(name string, opts ...GroupOpt) Group {
	var o groupOpts
	for _, opt := range opts {
		opt(&o)
	}
	queueGroup := queueGroupName(o.queueGroup, s.Config.QueueGroup)
	return &group{
		service:    s,
		prefix:     name,
		queueGroup: queueGroup,
	}
}

// dispatch is responsible for calling any async callbacks
func (ac *asyncCallbacksHandler) run() {
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

func (c *Config) valid() error {
	if !nameRegexp.MatchString(c.Name) {
		return fmt.Errorf("%w: service name: name should not be empty and should consist of alphanumerical characters, dashes and underscores", ErrConfigValidation)
	}
	if !semVerRegexp.MatchString(c.Version) {
		return fmt.Errorf("%w: version: version should not be empty should match the SemVer format", ErrConfigValidation)
	}
	if c.QueueGroup != "" && !subjectRegexp.MatchString(c.QueueGroup) {
		return fmt.Errorf("%w: queue group: invalid queue group name", ErrConfigValidation)
	}

	return nil
}

func (s *service) wrapConnectionEventCallbacks() {
	s.m.Lock()
	defer s.m.Unlock()
	s.natsHandlers.closed = s.nc.ClosedHandler()
	if s.natsHandlers.closed != nil {
		s.nc.SetClosedHandler(func(c *nats.Conn) {
			s.Stop()
			s.natsHandlers.closed(c)
		})
	} else {
		s.nc.SetClosedHandler(func(c *nats.Conn) {
			s.Stop()
		})
	}

	s.natsHandlers.asyncErr = s.nc.ErrorHandler()
	if s.natsHandlers.asyncErr != nil {
		s.nc.SetErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
			if sub == nil {
				s.natsHandlers.asyncErr(c, sub, err)
				return
			}
			endpoint, match := s.matchSubscriptionSubject(sub.Subject)
			if !match {
				s.natsHandlers.asyncErr(c, sub, err)
				return
			}
			if s.Config.ErrorHandler != nil {
				s.Config.ErrorHandler(s, &NATSError{
					Subject:     sub.Subject,
					Description: err.Error(),
				})
			}
			s.m.Lock()
			if endpoint != nil {
				endpoint.stats.NumErrors++
				endpoint.stats.LastError = err.Error()
			}
			s.m.Unlock()
			if stopErr := s.Stop(); stopErr != nil {
				s.natsHandlers.asyncErr(c, sub, errors.Join(err, fmt.Errorf("stopping service: %w", stopErr)))
			} else {
				s.natsHandlers.asyncErr(c, sub, err)
			}
		})
	} else {
		s.nc.SetErrorHandler(func(c *nats.Conn, sub *nats.Subscription, err error) {
			if sub == nil {
				return
			}
			endpoint, match := s.matchSubscriptionSubject(sub.Subject)
			if !match {
				return
			}
			if s.Config.ErrorHandler != nil {
				s.Config.ErrorHandler(s, &NATSError{
					Subject:     sub.Subject,
					Description: err.Error(),
				})
			}
			s.m.Lock()
			if endpoint != nil {
				endpoint.stats.NumErrors++
				endpoint.stats.LastError = err.Error()
			}
			s.m.Unlock()
			s.Stop()
		})
	}
}

func unwrapConnectionEventCallbacks(nc *nats.Conn, handlers handlers) {
	nc.SetClosedHandler(handlers.closed)
	nc.SetErrorHandler(handlers.asyncErr)
}

func (s *service) matchSubscriptionSubject(subj string) (*Endpoint, bool) {
	s.m.Lock()
	defer s.m.Unlock()
	for _, verbSub := range s.verbSubs {
		if verbSub.Subject == subj {
			return nil, true
		}
	}
	for _, e := range s.endpoints {
		if matchEndpointSubject(e.Subject, subj) {
			return e, true
		}
	}
	return nil, false
}

func matchEndpointSubject(endpointSubject, literalSubject string) bool {
	subjectTokens := strings.Split(literalSubject, ".")
	endpointTokens := strings.Split(endpointSubject, ".")
	if len(endpointTokens) > len(subjectTokens) {
		return false
	}
	for i, et := range endpointTokens {
		if i == len(endpointTokens)-1 && et == ">" {
			return true
		}
		if et != subjectTokens[i] && et != "*" {
			return false
		}
	}
	return true
}

// addVerbHandlers generates control handlers for a specific verb.
// Each request generates 3 subscriptions, one for the general verb
// affecting all services written with the framework, one that handles
// all services of a particular kind, and finally a specific service instance.
func (svc *service) addVerbHandlers(nc *nats.Conn, verb Verb, handler HandlerFunc) error {
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
func (s *service) addInternalHandler(nc *nats.Conn, verb Verb, kind, id, name string, handler HandlerFunc) error {
	subj, err := ControlSubject(verb, kind, id)
	if err != nil {
		if stopErr := s.Stop(); stopErr != nil {
			return errors.Join(err, fmt.Errorf("stopping service: %w", stopErr))
		}
		return err
	}

	s.verbSubs[name], err = nc.Subscribe(subj, func(msg *nats.Msg) {
		handler(&request{msg: msg})
	})
	if err != nil {
		if stopErr := s.Stop(); stopErr != nil {
			return errors.Join(err, fmt.Errorf("stopping service: %w", stopErr))
		}
		return err
	}
	return nil
}

// reqHandler invokes the service request handler and modifies service stats
func (s *service) reqHandler(endpoint *Endpoint, req *request) {
	start := time.Now()
	endpoint.Handler.Handle(req)
	s.m.Lock()
	endpoint.stats.NumRequests++
	endpoint.stats.ProcessingTime += time.Since(start)
	avgProcessingTime := endpoint.stats.ProcessingTime.Nanoseconds() / int64(endpoint.stats.NumRequests)
	endpoint.stats.AverageProcessingTime = time.Duration(avgProcessingTime)

	if req.respondError != nil {
		endpoint.stats.NumErrors++
		endpoint.stats.LastError = req.respondError.Error()
	}
	s.m.Unlock()
}

// Stop drains the endpoint subscriptions and marks the service as stopped.
func (s *service) Stop() error {
	s.m.Lock()
	defer s.m.Unlock()
	if s.stopped {
		return nil
	}
	for _, e := range s.endpoints {
		if err := e.stop(); err != nil {
			return err
		}
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
	unwrapConnectionEventCallbacks(s.nc, s.natsHandlers)
	s.stopped = true
	if s.DoneHandler != nil {
		s.asyncDispatcher.push(func() { s.DoneHandler(s) })
	}
	s.asyncDispatcher.close()
	return nil
}

func (s *service) serviceIdentity() ServiceIdentity {
	return ServiceIdentity{
		Name:     s.Config.Name,
		ID:       s.id,
		Version:  s.Config.Version,
		Metadata: s.Config.Metadata,
	}
}

// Info returns information about the service
func (s *service) Info() Info {
	s.m.Lock()
	defer s.m.Unlock()

	endpoints := make([]EndpointInfo, 0, len(s.endpoints))
	for _, e := range s.endpoints {
		endpoints = append(endpoints, EndpointInfo{
			Name:       e.Name,
			Subject:    e.Subject,
			QueueGroup: e.QueueGroup,
			Metadata:   e.Metadata,
		})
	}

	return Info{
		ServiceIdentity: s.serviceIdentity(),
		Type:            InfoResponseType,
		Description:     s.Config.Description,
		Endpoints:       endpoints,
	}
}

// Stats returns statistics for the service endpoint and all monitoring endpoints.
func (s *service) Stats() Stats {
	s.m.Lock()
	defer s.m.Unlock()

	stats := Stats{
		ServiceIdentity: s.serviceIdentity(),
		Endpoints:       make([]*EndpointStats, 0),
		Type:            StatsResponseType,
		Started:         s.started,
	}
	for _, endpoint := range s.endpoints {
		endpointStats := &EndpointStats{
			Name:                  endpoint.stats.Name,
			Subject:               endpoint.stats.Subject,
			QueueGroup:            endpoint.stats.QueueGroup,
			NumRequests:           endpoint.stats.NumRequests,
			NumErrors:             endpoint.stats.NumErrors,
			LastError:             endpoint.stats.LastError,
			ProcessingTime:        endpoint.stats.ProcessingTime,
			AverageProcessingTime: endpoint.stats.AverageProcessingTime,
		}
		if s.StatsHandler != nil {
			data, _ := json.Marshal(s.StatsHandler(endpoint))
			endpointStats.Data = data
		}
		stats.Endpoints = append(stats.Endpoints, endpointStats)
	}
	return stats
}

// Reset resets all statistics on a service instance.
func (s *service) Reset() {
	s.m.Lock()
	for _, endpoint := range s.endpoints {
		endpoint.reset()
	}
	s.started = time.Now().UTC()
	s.m.Unlock()
}

// Stopped informs whether [Stop] was executed on the service.
func (s *service) Stopped() bool {
	s.m.Lock()
	defer s.m.Unlock()
	return s.stopped
}

func (e *NATSError) Error() string {
	return fmt.Sprintf("%q: %s", e.Subject, e.Description)
}

func (g *group) AddEndpoint(name string, handler Handler, opts ...EndpointOpt) error {
	var options endpointOpts
	for _, opt := range opts {
		if err := opt(&options); err != nil {
			return err
		}
	}
	subject := name
	if options.subject != "" {
		subject = options.subject
	}
	endpointSubject := fmt.Sprintf("%s.%s", g.prefix, subject)
	if g.prefix == "" {
		endpointSubject = subject
	}
	queueGroup := queueGroupName(options.queueGroup, g.queueGroup)

	return addEndpoint(g.service, name, endpointSubject, handler, options.metadata, queueGroup)
}

func queueGroupName(customQG, parentQG string) string {
	queueGroup := customQG
	if queueGroup == "" {
		if parentQG != "" {
			queueGroup = parentQG
		} else {
			queueGroup = DefaultQueueGroup
		}
	}
	return queueGroup
}

func (g *group) AddGroup(name string, opts ...GroupOpt) Group {
	var o groupOpts
	for _, opt := range opts {
		opt(&o)
	}
	queueGroup := queueGroupName(o.queueGroup, g.queueGroup)

	parts := make([]string, 0, 2)
	if g.prefix != "" {
		parts = append(parts, g.prefix)
	}
	if name != "" {
		parts = append(parts, name)
	}
	prefix := strings.Join(parts, ".")

	return &group{
		service:    g.service,
		prefix:     prefix,
		queueGroup: queueGroup,
	}
}

func (e *Endpoint) stop() error {
	if err := e.subscription.Drain(); err != nil {
		return fmt.Errorf("draining subscription for request handler: %w", err)
	}
	for i := 0; i < len(e.service.endpoints); i++ {
		if e.service.endpoints[i].Subject == e.Subject {
			if i != len(e.service.endpoints)-1 {
				e.service.endpoints = append(e.service.endpoints[:i], e.service.endpoints[i+1:]...)
			} else {
				e.service.endpoints = e.service.endpoints[:i]
			}
			i++
		}
	}
	return nil
}

func (e *Endpoint) reset() {
	e.stats = EndpointStats{
		Name:    e.stats.Name,
		Subject: e.stats.Subject,
	}
}

// ControlSubject returns monitoring subjects used by the Service.
// Providing a verb is mandatory (it should be one of Ping, Info or Stats).
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

func WithEndpointSubject(subject string) EndpointOpt {
	return func(e *endpointOpts) error {
		e.subject = subject
		return nil
	}
}

func WithEndpointMetadata(metadata map[string]string) EndpointOpt {
	return func(e *endpointOpts) error {
		e.metadata = metadata
		return nil
	}
}

func WithEndpointQueueGroup(queueGroup string) EndpointOpt {
	return func(e *endpointOpts) error {
		e.queueGroup = queueGroup
		return nil
	}
}

func WithGroupQueueGroup(queueGroup string) GroupOpt {
	return func(g *groupOpts) {
		g.queueGroup = queueGroup
	}
}
