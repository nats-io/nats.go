package nats

import (
	"testing"
)

var as *server

func TestAuthServerStart(t *testing.T) {
	as = startServer(t, 8222, "--user derek --pass foo")
}

func TestAuthConnectionFail(t *testing.T) {
	_, err := Connect("nats://localhost:8222")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
}

func TestAuthConnectionSuccess(t *testing.T) {
	nc, err := Connect("nats://derek:foo@localhost:8222")
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()
}

func TestAuthServerStop(t *testing.T) {
	as.stopServer()
}
