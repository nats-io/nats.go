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

// The "micro" package is deprecated. Please use the "service" package instead.
package micro

import (
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/service"
)

type (
	Service         = service.Service
	Group           = service.Group
	EndpointOpt     = service.EndpointOpt
	GroupOpt        = service.GroupOpt
	ErrHandler      = service.ErrHandler
	DoneHandler     = service.DoneHandler
	StatsHandler    = service.StatsHandler
	ServiceIdentity = service.ServiceIdentity
	Stats           = service.Stats
	EndpointStats   = service.EndpointStats
	Ping            = service.Ping
	Info            = service.Info
	EndpointInfo    = service.EndpointInfo
	Endpoint        = service.Endpoint
	Verb            = service.Verb
	Config          = service.Config
	EndpointConfig  = service.EndpointConfig
	NATSError       = service.NATSError
)

const (
	DefaultQueueGroup = service.DefaultQueueGroup
	APIPrefix         = service.APIPrefix
)

const (
	ErrorHeader     = service.ErrorHeader
	ErrorCodeHeader = service.ErrorCodeHeader
)

const (
	PingVerb  = service.PingVerb
	StatsVerb = service.StatsVerb
	InfoVerb  = service.InfoVerb
)

const (
	InfoResponseType  = service.InfoResponseType
	PingResponseType  = service.PingResponseType
	StatsResponseType = service.StatsResponseType
)

var (
	ErrConfigValidation    = service.ErrConfigValidation
	ErrVerbNotSupported    = service.ErrVerbNotSupported
	ErrServiceNameRequired = service.ErrServiceNameRequired
)

func AddService(nc *nats.Conn, config Config) (Service, error) {
	return service.AddService(nc, config)
}

func ControlSubject(verb Verb, name, id string) (string, error) {
	return service.ControlSubject(verb, name, id)
}

func WithEndpointSubject(subject string) EndpointOpt {
	return service.WithEndpointSubject(subject)
}

func WithEndpointMetadata(metadata map[string]string) EndpointOpt {
	return service.WithEndpointMetadata(metadata)
}

func WithEndpointQueueGroup(queueGroup string) EndpointOpt {
	return service.WithEndpointQueueGroup(queueGroup)
}

func WithGroupQueueGroup(queueGroup string) GroupOpt {
	return service.WithGroupQueueGroup(queueGroup)
}
