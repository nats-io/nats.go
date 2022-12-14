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

package svc

import "github.com/nats-io/nuid"

type (
	// Service represents the behavior from a NATS service.
	Service interface {
		ID() string
		Stop()
		// ... rest of interface
	}

	// Conn represents the behavior that NATS Services use
	// from a NATS connection to be able to create services.
	Conn interface {
		QueueSubscribe(subj, queue string, cb MsgHandler) (Subscription, error)
	}

	Endpoint struct {
		Subject string `json:"subject"`
		Handler RequestHandler
	}

	// Config is the configuration of a NATS service.
	Config struct {
		Name        string   `json:"name"`
		Description string   `json:"description"`
		Version     string   `json:"version"`
		Endpoint    Endpoint `json:"endpoint"`
		// ...rest of the service config
	}

	// service is the internal implementation of a Service
	service struct {
		Config Config
		conn   Conn
		reqSub Subscription
		id     string
		// ...rest of state from service
	}

	Request struct {
		Msg
		errResponse bool
	}

	// RequestHandler is a function used as a Handler for a service.
	RequestHandler func(*Request)
)

const (
	QG = "svc"
)

func Add(nc Conn, config Config) (Service, error) {
	id := nuid.Next()
	svc := &service{
		Config: config,
		conn:   nc,
		id:     id,
	}
	var err error
	svc.reqSub, err = nc.QueueSubscribe(config.Endpoint.Subject, QG, func(m Msg) {
		svc.reqHandler(&Request{Msg: m})
	})
	if err != nil {
		return nil, err
	}
	return svc, nil
}

func (s *service) ID() string {
	return s.id
}

func (s *service) Stop() {
	s.reqSub.Drain()
	// .. stop ..
}

func (s *service) reqHandler(req *Request) {
	s.Config.Endpoint.Handler(req)
}
