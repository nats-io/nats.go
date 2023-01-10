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

package micro_test

import (
	"context"
	"os"
	"reflect"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
	"golang.org/x/exp/slog"
)

func ExampleAddService() {
	ctx := exampleCtx()
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		logger := slog.FromContext(ctx)
		logger.Error("failed to connect to NATS", err)
		os.Exit(1)
	}
	defer nc.Close()

	echoHandler := func(ctx context.Context, req micro.Request) {
		req.Respond(req.Data())
	}

	config := micro.Config{
		Name:        "EchoService",
		Version:     "1.0.0",
		Description: "Send back what you receive",
		Endpoint: micro.Endpoint{
			Subject: "echo",
			Handler: micro.HandlerFunc(echoHandler),
		},

		// DoneHandler can be set to customize behavior on stopping a service.
		DoneHandler: func(ctx context.Context, srv micro.Service) {
			info := srv.Info(ctx)
			logger := slog.FromContext(ctx)
			logger.Info(
				"stopped service",
				slog.String("name", info.Name),
				slog.String("id", info.ID),
			)
		},

		// ErrorHandler can be used to customize behavior on service execution error.
		ErrorHandler: func(ctx context.Context, srv micro.Service, err *micro.NATSError) {
			info := srv.Info(ctx)
			logger := slog.FromContext(ctx)
			logger.Error(
				"service error",
				err,
				slog.String("name", info.Name),
				slog.String("subject", err.Subject),
				slog.String("description", err.Description),
			)
		},
	}

	logger := slog.FromContext(ctx)
	srv, err := micro.AddService(ctx, nc, config)
	if err != nil {
		logger.Error("failed to add service", err)
		os.Exit(1)
	}
	defer srv.Stop(ctx)
}

func ExampleService_Info() {
	ctx := exampleCtx()
	logger := slog.FromContext(ctx)

	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		logger.Error("failed to connect to NATS", err)
		os.Exit(1)
	}
	defer nc.Close()

	config := micro.Config{
		Name: "EchoService",
		Endpoint: micro.Endpoint{
			Subject: "echo",
			Handler: micro.HandlerFunc(func(context.Context, micro.Request) {}),
		},
	}

	srv, _ := micro.AddService(ctx, nc, config)

	// service info
	info := srv.Info(ctx)

	logger.Info(
		"service",
		slog.String("name", info.Name),
		slog.String("id", info.ID),
		slog.String("version", info.Version),
		slog.String("description", info.Description),
		slog.String("subject", info.Subject),
	)
}

func ExampleService_Stats() {
	ctx := exampleCtx()
	logger := slog.FromContext(ctx)

	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		logger.Error("failed to connect to NATS", err)
		os.Exit(1)
	}
	defer nc.Close()

	config := micro.Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: micro.Endpoint{
			Subject: "echo",
			Handler: micro.HandlerFunc(func(context.Context, micro.Request) {}),
		},
	}

	srv, _ := micro.AddService(ctx, nc, config)

	// stats of a service instance
	stats := srv.Stats(ctx)

	logger.Info(
		"stats",
		slog.Duration("averageProcessingTime", stats.AverageProcessingTime),
		slog.Duration("processingTime", stats.ProcessingTime),
	)
}

func ExampleService_Stop() {
	ctx := exampleCtx()
	logger := slog.FromContext(ctx)

	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		logger.Error("failed to connect to NATS", err)
		os.Exit(1)
	}
	defer nc.Close()

	config := micro.Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: micro.Endpoint{
			Subject: "echo",
			Handler: micro.HandlerFunc(func(context.Context, micro.Request) {}),
		},
	}

	srv, _ := micro.AddService(ctx, nc, config)

	// stop a service
	err = srv.Stop(ctx)
	if err != nil {
		logger.Error("failed to stop service", err)
		os.Exit(1)
	}

	// stop is idempotent so multiple executions will not return an error
	err = srv.Stop(ctx)
	if err != nil {
		logger.Error("failed to stop service", err)
		os.Exit(1)
	}
}

func ExampleService_Stopped() {
	ctx := exampleCtx()
	logger := slog.FromContext(ctx)

	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		logger.Error("failed to connect to NATS", err)
		os.Exit(1)
	}
	defer nc.Close()

	config := micro.Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: micro.Endpoint{
			Subject: "echo",
			Handler: micro.HandlerFunc(func(context.Context, micro.Request) {}),
		},
	}

	srv, _ := micro.AddService(ctx, nc, config)

	// stop a service
	err = srv.Stop(ctx)
	if err != nil {
		logger.Error("failed to stop service", err)
		os.Exit(1)
	}

	if srv.Stopped(ctx) {
		logger.Info("service stopped")
	}
}

func ExampleService_Reset() {
	ctx := exampleCtx()
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		logger := slog.FromContext(ctx)
		logger.Error("failed to connect to NATS", err)
		os.Exit(1)
	}
	defer nc.Close()

	config := micro.Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: micro.Endpoint{
			Subject: "echo",
			Handler: micro.HandlerFunc(func(context.Context, micro.Request) {}),
		},
	}

	srv, _ := micro.AddService(ctx, nc, config)

	// reset endpoint stats on this service
	srv.Reset(ctx)

	empty := micro.Stats{
		ServiceIdentity: srv.Info(ctx).ServiceIdentity,
	}
	if !reflect.DeepEqual(srv.Stats(ctx), empty) {
		logger := slog.FromContext(ctx)
		logger.Error("Expected endpoint stats to be empty", err)
		os.Exit(1)
	}
}

func ExampleControlSubject() {
	ctx := exampleCtx()

	// subject used to get PING from all services
	subjectPINGAll, _ := micro.ControlSubject(micro.PingVerb, "", "")

	// subject used to get PING from services with provided name
	subjectPINGName, _ := micro.ControlSubject(micro.PingVerb, "CoolService", "")

	// subject used to get PING from a service with provided name and ID
	subjectPINGInstance, _ := micro.ControlSubject(micro.PingVerb, "CoolService", "123")

	logger := slog.FromContext(ctx)
	logger.Info(
		"subjects",
		slog.String("PINGAll", subjectPINGAll),
		slog.String("PINGName", subjectPINGName),
		slog.String("PINGInstance", subjectPINGInstance),
	)

	// Output:
	// $SRV.PING
	// $SRV.PING.CoolService
	// $SRV.PING.CoolService.123
}

func ExampleRequest_Respond() {
	ctx := exampleCtx()

	handler := func(ctx context.Context, req micro.Request) {
		// respond to the request
		if err := req.Respond(req.Data()); err != nil {
			logger := slog.FromContext(ctx)
			logger.Error("failed to respond to request", err)
			os.Exit(1)
		}
	}

	logger := slog.FromContext(ctx)
	logger.Info("handler",
		slog.String("type", reflect.TypeOf(handler).String()),
	)
}

func ExampleRequest_RespondJSON() {
	ctx := exampleCtx()

	type Point struct {
		X int `json:"x"`
		Y int `json:"y"`
	}

	handler := func(ctx context.Context, req micro.Request) {
		resp := Point{5, 10}
		// respond to the request
		// response will be serialized to {"x":5,"y":10}
		if err := req.RespondJSON(resp); err != nil {
			logger := slog.FromContext(ctx)
			logger.Error("failed to respond to request", err)
			os.Exit(1)
		}
	}

	logger := slog.FromContext(ctx)
	logger.Info("handler",
		slog.String("type", reflect.TypeOf(handler).String()),
	)
}

func ExampleRequest_Error() {
	ctx := exampleCtx()

	handler := func(ctx context.Context, req micro.Request) {
		// respond with an error
		// Error sets Nats-Service-Error and Nats-Service-Error-Code headers in the response
		if err := req.Error("400", "bad request", []byte(`{"error": "value should be a number"}`)); err != nil {
			logger := slog.FromContext(ctx)
			logger.Error("failed to respond to request", err)
			os.Exit(1)
		}
	}

	logger := slog.FromContext(ctx)
	logger.Info("handler",
		slog.String("type", reflect.TypeOf(handler).String()),
	)
}
