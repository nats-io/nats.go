package nats

import (
	"log"
	"testing"
	"time"

	"github.com/nats-io/nats.go/svc"
)

func TestAddService_Interfaces(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	nc, err := Connect(s.ClientURL())
	if err != nil {
		t.Fatalf("Expected to connect to server, got %v", err)
	}
	defer nc.Close()

	doAdd := func(req *svc.Request) {
		req.Respond([]byte("hello world"))
	}
	svc, err := nc.AddService(svc.Config{
		Endpoint: svc.Endpoint{
			Subject: "svc.add",
			Handler: doAdd,
		},
	})
	if err != nil {
		log.Fatal(err)
	}
	resp, err := nc.Request("svc.add", []byte("Hi!"), 1*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if string(resp.Data) != "hello world" {
		t.Fatal("Unexpected response")
	}
	svc.Stop()
}
