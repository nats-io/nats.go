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
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

func ExampleAddService() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	echoHandler := func(req *Request) {
		req.Respond(req.Data)
	}

	config := Config{
		Name:        "EchoService",
		Version:     "v1.0.0",
		Description: "Send back what you receive",
		Endpoint: Endpoint{
			Subject: "echo",
			Handler: echoHandler,
		},

		// DoneHandler can be set to customize behavior on stopping a service.
		DoneHandler: func(srv Service) {
			info := srv.Info()
			fmt.Printf("stopped service %q with ID %q\n", info.Name, info.ID)
		},

		// ErrorHandler can be used to customize behavior on service execution error.
		ErrorHandler: func(srv Service, err *NATSError) {
			info := srv.Info()
			fmt.Printf("Service %q returned an error on subject %q: %s", info.Name, err.Subject, err.Description)
		},
	}

	srv, err := AddService(nc, config)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Stop()
}

func ExampleService_Info() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	config := Config{
		Name: "EchoService",
		Endpoint: Endpoint{
			Subject: "echo",
			Handler: func(*Request) {},
		},
	}

	srv, _ := AddService(nc, config)

	// service info
	info := srv.Info()

	fmt.Println(info.ID)
	fmt.Println(info.Name)
	fmt.Println(info.Description)
	fmt.Println(info.Version)
	fmt.Println(info.Subject)
}

func ExampleService_Stats() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	config := Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: Endpoint{
			Subject: "echo",
			Handler: func(*Request) {},
		},
	}

	srv, _ := AddService(nc, config)

	// stats of a service instance
	stats := srv.Stats()

	fmt.Println(stats.AverageProcessingTime)
	fmt.Println(stats.TotalProcessingTime)

}

func ExampleService_Stop() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	config := Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: Endpoint{
			Subject: "echo",
			Handler: func(*Request) {},
		},
	}

	srv, _ := AddService(nc, config)

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

	config := Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: Endpoint{
			Subject: "echo",
			Handler: func(*Request) {},
		},
	}

	srv, _ := AddService(nc, config)

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

	config := Config{
		Name:    "EchoService",
		Version: "0.1.0",
		Endpoint: Endpoint{
			Subject: "echo",
			Handler: func(*Request) {},
		},
	}

	srv, _ := AddService(nc, config)

	// reset endpoint stats on this service
	srv.Reset()

	empty := Stats{
		ServiceIdentity: srv.Info().ServiceIdentity,
	}
	if srv.Stats() != empty {
		log.Fatal("Expected endpoint stats to be empty")
	}
}

func ExampleControlSubject() {

	// subject used to get PING from all services
	subjectPINGAll, _ := ControlSubject(PingVerb, "", "")
	fmt.Println(subjectPINGAll)

	// subject used to get PING from services with provided name
	subjectPINGName, _ := ControlSubject(PingVerb, "CoolService", "")
	fmt.Println(subjectPINGName)

	// subject used to get PING from a service with provided name and ID
	subjectPINGInstance, _ := ControlSubject(PingVerb, "CoolService", "123")
	fmt.Println(subjectPINGInstance)

	// Output:
	// $SRV.PING
	// $SRV.PING.COOLSERVICE
	// $SRV.PING.COOLSERVICE.123
}

func ExampleRequest_Respond() {
	handler := func(req *Request) {
		// respond to the request
		if err := req.Respond(req.Data); err != nil {
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

	handler := func(req *Request) {
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
	handler := func(req *Request) {
		// respond with an error
		// Error sets Nats-Service-Error and Nats-Service-Error-Code headers in the response
		if err := req.Error("400", "bad request"); err != nil {
			log.Fatal(err)
		}
	}

	fmt.Printf("%T", handler)
}
