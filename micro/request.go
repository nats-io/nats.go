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
	"context"

	"github.com/nats-io/nats.go/service"
)

type (
	Handler     = service.Handler
	HandlerFunc = service.HandlerFunc
	Request     = service.Request
	Headers     = service.Headers
	RespondOpt  = service.RespondOpt
)

var (
	ErrRespond         = service.ErrRespond
	ErrMarshalResponse = service.ErrMarshalResponse
	ErrArgRequired     = service.ErrArgRequired
)

func ContextHandler(ctx context.Context, handler func(context.Context, Request)) Handler {
	return service.ContextHandler(ctx, handler)
}

// WithHeaders can be used to configure response with custom headers.
func WithHeaders(headers Headers) RespondOpt {
	return service.WithHeaders(headers)
}
