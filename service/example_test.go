package service

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

func ExampleAdd() {
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
		DoneHandler: func(srv *Service) {
			fmt.Printf("stopped service %q with ID %q\n", srv.Name, srv.ID())
		},

		// ErrorHandler can be used to customize behavior on service execution error.
		ErrorHandler: func(srv *Service, err *NATSError) {
			fmt.Printf("Service %q returned an error on subject %q: %s", srv.Name, err.Subject, err.Description)
		},
	}

	srv, err := Add(nc, config)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Stop()
}

func ExampleService_ID() {
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

	srv, _ := Add(nc, config)

	// unique service ID
	id := srv.ID()
	fmt.Println(id)
}

func ExampleService_Stats() {
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

	srv, _ := Add(nc, config)

	// stats of a service instance
	stats := srv.Stats()

	for _, e := range stats.Endpoints {
		fmt.Println(e.AverageProcessingTime)
	}

}

func ExampleService_Stop() {
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

	srv, _ := Add(nc, config)

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
		Name: "EchoService",
		Endpoint: Endpoint{
			Subject: "echo",
			Handler: func(*Request) {},
		},
	}

	srv, _ := Add(nc, config)

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
		Name: "EchoService",
		Endpoint: Endpoint{
			Subject: "echo",
			Handler: func(*Request) {},
		},
	}

	srv, _ := Add(nc, config)

	// reset endpoint stats on this service
	srv.Reset()

	empty := EndpointStats{Name: "EchoService"}
	for _, e := range srv.Stats().Endpoints {
		if e != empty {
			log.Fatal("Expected endpoint stats to be empty")
		}
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
