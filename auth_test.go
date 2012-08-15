package nats

import (
	"testing"
)

var as *server

var uri  = "nats://localhost:8222"
var auri = "nats://derek:foo@localhost:8222"

func TestAuthConnectionFail(t *testing.T) {
	as = startNatsServer(t, 8222, "--user derek --pass foo -l /tmp/foo.log")
	_, err := Connect(uri)
	if err == nil {
		t.Fatal("Should have gotten error trying to connect")
	}
}

func TestAuthConnectionSuccess(t *testing.T) {
	as = startNatsServer(t, 8222, "--user derek --pass foo -l /tmp/foo.log")
	_, err := Connect(auri)
	if err != nil {
		t.Fatal("Should have connected succesfully")
	}
}

func TestAuthServerStop(t *testing.T) {
	as.stopServer()
}