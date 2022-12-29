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

	// Service handler is a function which takes Service.Request as argument.
	// req.Respond or req.Error should be used to respond to the request.
	incrementHandler := func(req micro.Request) {
		val, err := strconv.Atoi(string(req.Data()))
		if err != nil {
			req.Error("400", "request data should be a number", nil)
			return
		}

		responseData := val + 1
		req.Respond([]byte(strconv.Itoa(responseData)))
	}

	config := micro.Config{
		Name:        "IncrementService",
		Version:     "0.1.0",
		Description: "Increment numbers",
		Endpoint: micro.Endpoint{
			// service handler
			Handler: incrementHandler,
			// a unique subject serving as a service endpoint
			Subject: "numbers.increment",
		},
	}
	// Multiple instances of the servcice with the same name can be created.
	// Requests to a service with the same name will be load-balanced.
	for i := 0; i < 5; i++ {
		svc, err := micro.AddService(nc, config)
		if err != nil {
			log.Fatal(err)
		}
		defer svc.Stop()
	}

	// send a request to a service
	resp, err := nc.Request("numbers.increment", []byte("3"), 1*time.Second)
	if err != nil {
		log.Fatal(err)
	}
	responseVal, err := strconv.Atoi(string(resp.Data))
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(responseVal)
}
