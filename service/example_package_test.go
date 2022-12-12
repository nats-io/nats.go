package service

import (
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
)

func Example() {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := nats.Connect(s.ClientURL())
	if err != nil {
		log.Fatal(err)
	}
	defer nc.Close()

	// Service handler is a function which takes *service.Request as argument.
	// req.Respond or req.Error should be used to respond to the request.
	incrementHandler := func(req *Request) {
		val, err := strconv.Atoi(string(req.Data))
		if err != nil {
			req.Error("400", "request data should be a number")
			return
		}

		responseData := val + 1
		req.Respond([]byte(strconv.Itoa(responseData)))
	}

	config := Config{
		Name:        "IncrementService",
		Version:     "v0.1.0",
		Description: "Increment numbers",
		Endpoint: Endpoint{
			// service handler
			Handler: incrementHandler,
			// a unique subject serving as a service endpoint
			Subject: "numbers.increment",
		},
	}
	// Multiple instances of the servcice with the same name can be created.
	// Requests to a service with the same name will be load-balanced.
	for i := 0; i < 5; i++ {
		svc, err := Add(nc, config)
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

	//
	// Output: 4
	//
}
