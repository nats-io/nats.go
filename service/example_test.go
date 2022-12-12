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
		DoneHandler: func(srv Service) {
			fmt.Printf("stopped service %q with ID %q\n", srv.Name(), srv.ID())
		},

		// ErrorHandler can be used to customize behavior on service execution error.
		ErrorHandler: func(srv Service, err *NATSError) {
			fmt.Printf("Service %q returned an error on subject %q: %s", srv.Name(), err.Subject, err.Description)
		},
	}

	srv, err := Add(nc, config)
	if err != nil {
		log.Fatal(err)
	}
	defer srv.Stop()
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
