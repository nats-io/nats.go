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

package service_test

import (
	"context"
	"fmt"
	"log"
	"reflect"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/service"
)

func ExampleNew() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	echoHandler := func(req service.Request) {
		req.Respond(req.Data())
	}

	config := service.Config{
		Name:        "EchoService",
		Version:     "1.0.0",
		Description: "Send back what you receive",
		// DoneHandler can be set to customize behavior on stopping a service.
		DoneHandler: func(srv service.Service) {
			info := srv.Info()
			fmt.Printf("stopped service %q with ID %q\n", info.Name, info.ID)
		},

		// ErrorHandler can be used to customize behavior on service execution error.
		ErrorHandler: func(srv service.Service, err *service.NATSError) {
			info := srv.Info()
			fmt.Printf("Service %q returned an error on subject %q: %s", info.Name, err.Subject, err.Description)
		},

		// optional base handler
		Endpoint: &service.EndpointConfig{
			Subject: "echo",
			Handler: service.HandlerFunc(echoHandler),
		},
	}

	srv, err := service.New(nc, config)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Stop()
}

func ExampleService_AddEndpoint() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	echoHandler := func(req service.Request) {
		req.Respond(req.Data())
	}

	config := service.Config{
		Name:    "EchoService",
		Version: "1.0.0",
	}

	srv, err := service.New(nc, config)
	if err != nil {
		log.Fatal(err)
	}

	// endpoint will be registered under "Echo" subject
	err = srv.AddEndpoint("Echo", service.HandlerFunc(echoHandler))
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleWithEndpointSubject() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	echoHandler := func(req service.Request) {
		req.Respond(req.Data())
	}

	config := service.Config{
		Name:    "EchoService",
		Version: "1.0.0",
	}

	srv, err := service.New(nc, config)
	if err != nil {
		log.Fatal(err)
	}

	// endpoint will be registered under "service.echo" subject
	err = srv.AddEndpoint("Echo", service.HandlerFunc(echoHandler), service.WithEndpointSubject("service.echo"))
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleService_AddGroup() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	echoHandler := func(req service.Request) {
		req.Respond(req.Data())
	}

	config := service.Config{
		Name:    "EchoService",
		Version: "1.0.0",
	}

	srv, err := service.New(nc, config)
	if err != nil {
		log.Fatal(err)
	}

	v1 := srv.AddGroup("v1")

	// endpoint will be registered under "v1.Echo" subject
	err = v1.AddEndpoint("Echo", service.HandlerFunc(echoHandler))
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleService_Info() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	config := service.Config{
		Name: "EchoService",
	}

	srv, _ := service.New(nc, config)

	// service info
	info := srv.Info()

	fmt.Println(info.ID)
	fmt.Println(info.Name)
	fmt.Println(info.Description)
	fmt.Println(info.Version)
	fmt.Println(info.Endpoints)
}

func ExampleService_Stats() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	config := service.Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: &service.EndpointConfig{
			Subject: "echo",
			Handler: service.HandlerFunc(func(r service.Request) {}),
		},
	}

	srv, _ := service.New(nc, config)
	// stats of a service instance
	stats := srv.Stats()

	fmt.Println(stats.Endpoints[0].AverageProcessingTime)
	fmt.Println(stats.Endpoints[0].ProcessingTime)

}

func ExampleService_Stop() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	config := service.Config{
		Name:    "EchoService",
		Version: "0.1.0",
	}

	srv, _ := service.New(nc, config)

	// stop a service
	err = srv.Stop()
	if err != nil {
		log.Fatal(err)
	}

	// stop is idempotent so multiple executions will not return an error
	err = srv.Stop()
	if err != nil {
		log.Fatal(err)
	}
}

func ExampleService_Stopped() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	config := service.Config{
		Name:    "EchoService",
		Version: "0.1.0",
	}

	srv, _ := service.New(nc, config)

	// stop a service
	err = srv.Stop()
	if err != nil {
		log.Fatal(err)
	}

	if srv.Stopped() {
		fmt.Println("service stopped")
	}
}

func ExampleService_Reset() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	config := service.Config{
		Name:    "EchoService",
		Version: "0.1.0",
	}

	srv, _ := service.New(nc, config)

	// reset endpoint stats on this service
	srv.Reset()

	empty := service.Stats{
		ServiceIdentity: srv.Info().ServiceIdentity,
	}
	if !reflect.DeepEqual(srv.Stats(), empty) {
		log.Fatal("Expected endpoint stats to be empty")
	}
}

func ExampleContextHandler() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	handler := func(ctx context.Context, req service.Request) {
		select {
		case <-ctx.Done():
			req.Error("400", "context canceled", nil)
		default:
			req.Respond([]byte("ok"))
		}
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	config := service.Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: &service.EndpointConfig{
			Subject: "echo",
			Handler: service.ContextHandler(ctx, handler),
		},
	}

	srv, _ := service.New(nc, config)
	defer srv.Stop()
}

func ExampleControlSubject() {

	// subject used to get PING from all services
	subjectPINGAll, _ := service.ControlSubject(service.PingVerb, "", "")
	fmt.Println(subjectPINGAll)

	// subject used to get PING from services with provided name
	subjectPINGName, _ := service.ControlSubject(service.PingVerb, "CoolService", "")
	fmt.Println(subjectPINGName)

	// subject used to get PING from a service with provided name and ID
	subjectPINGInstance, _ := service.ControlSubject(service.PingVerb, "CoolService", "123")
	fmt.Println(subjectPINGInstance)

	// Output:
	// $SRV.PING
	// $SRV.PING.CoolService
	// $SRV.PING.CoolService.123
}

func ExampleRequest_Respond() {
	handler := func(req service.Request) {
		// respond to the request
		if err := req.Respond(req.Data()); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("%T", handler)
}

func ExampleRequest_RespondJSON() {
	type Point struct {
		X int `json:"x"`
		Y int `json:"y"`
	}

	handler := func(req service.Request) {
		resp := Point{5, 10}
		// respond to the request
		// response will be serialized to {"x":5,"y":10}
		if err := req.RespondJSON(resp); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("%T", handler)
}

func ExampleRequest_Error() {
	handler := func(req service.Request) {
		// respond with an error
		// Error sets Nats-Service-Error and Nats-Service-Error-Code headers in the response
		if err := req.Error("400", "bad request", []byte(`{"error": "value should be a number"}`)); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("%T", handler)
}
