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
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/micro"
)

func Example() {
	nc, err := nats.Connect("127.0.0.1:4222")
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// endpoint handler - in this case, HandlerFunc is used,
	// which is a built-in implementation of Handler interface
	echoHandler := func(req micro.Request) {
		req.Respond(req.Data())
	}

	// second endpoint
	incrementHandler := func(req micro.Request) {
		val, err := strconv.Atoi(string(req.Data()))
		if err != nil {
			req.Error("400", "request data should be a number", nil)
			return
		}

		responseData := val + 1
		req.Respond([]byte(strconv.Itoa(responseData)))
	}

	// third endpoint
	multiplyHandler := func(req micro.Request) {
		val, err := strconv.Atoi(string(req.Data()))
		if err != nil {
			req.Error("400", "request data should be a number", nil)
			return
		}

		responseData := val * 2
		req.Respond([]byte(strconv.Itoa(responseData)))
	}

	config := micro.Config{
		Name:        "IncrementService",
		Version:     "0.1.0",
		Description: "Increment numbers",

		// base handler - for simple services with single endpoints this is sufficient
		Endpoint: &micro.EndpointConfig{
			Subject: "echo",
			Handler: micro.HandlerFunc(echoHandler),
		},
	}
	svc, err := micro.AddService(nc, config)
	if err != nil {
		log.Fatal(err)
	}
	defer svc.Stop()

	// add a group to aggregate endpoints under common prefix
	numbers := svc.AddGroup("numbers")

	// register endpoints in a group
	err = numbers.AddEndpoint("Increment", micro.HandlerFunc(incrementHandler))
	if err != nil {
		log.Fatal(err)
	}
	err = numbers.AddEndpoint("Multiply", micro.HandlerFunc(multiplyHandler))
	if err != nil {
		log.Fatal(err)
	}

	// send a request to a service
	resp, err := nc.Request("numbers.Increment", []byte("3"), 1*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	responseVal, err := strconv.Atoi(string(resp.Data))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(responseVal)
}
