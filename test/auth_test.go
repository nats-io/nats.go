// Copyright 2012-2023 The NATS Authors
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

package test

import (
	"errors"
	"fmt"
	"io/fs"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats-server/v2/test"
	"github.com/nats-io/nats.go"
)

func TestAuth(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	opts.Username = "derek"
	opts.Password = "foo"
	s := RunServerWithOptions(&opts)
	defer s.Shutdown()

	_, err := nats.Connect("nats://127.0.0.1:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	// This test may be a bit too strict for the future, but for now makes
	// sure that we correctly process the -ERR content on connect.
	if strings.ToLower(err.Error()) != nats.ErrAuthorization.Error() {
		t.Fatalf("Expected error '%v', got '%v'", nats.ErrAuthorization, err)
	}

	if !errors.Is(err, nats.ErrAuthorization) {
		t.Fatalf("Expected error '%v', got '%v'", nats.ErrAuthorization, err)
	}

	nc, err := nats.Connect("nats://derek:foo@127.0.0.1:8232")
	if err != nil {
		t.Fatal("Should have connected successfully with a token")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect("nats://127.0.0.1:8232", nats.UserInfo("derek", "foo"))
	if err != nil {
		t.Fatalf("Should have connected successfully with a token: %v", err)
	}
	nc.Close()
	// Verify that credentials in URL take precedence.
	nc, err = nats.Connect("nats://derek:foo@127.0.0.1:8232", nats.UserInfo("foo", "bar"))
	if err != nil {
		t.Fatalf("Should have connected successfully with a token: %v", err)
	}
	nc.Close()
}

func TestAuthFailNoDisconnectErrCB(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	opts.Username = "derek"
	opts.Password = "foo"
	s := RunServerWithOptions(&opts)
	defer s.Shutdown()

	copts := nats.GetDefaultOptions()
	copts.Url = "nats://127.0.0.1:8232"
	receivedDisconnectErrCB := int32(0)
	copts.DisconnectedErrCB = func(nc *nats.Conn, _ error) {
		atomic.AddInt32(&receivedDisconnectErrCB, 1)
	}

	_, err := copts.Connect()
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	if atomic.LoadInt32(&receivedDisconnectErrCB) > 0 {
		t.Fatal("Should not have received a disconnect callback on auth failure")
	}
}

func TestAuthFailAllowReconnect(t *testing.T) {
	ts := RunServerOnPort(23232)
	defer ts.Shutdown()

	var servers = []string{
		"nats://127.0.0.1:23232",
		"nats://127.0.0.1:23233",
		"nats://127.0.0.1:23234",
	}

	opts2 := test.DefaultTestOptions
	opts2.Port = 23233
	opts2.Username = "ivan"
	opts2.Password = "foo"
	ts2 := RunServerWithOptions(&opts2)
	defer ts2.Shutdown()

	ts3 := RunServerOnPort(23234)
	defer ts3.Shutdown()

	reconnectch := make(chan bool)
	defer close(reconnectch)

	copts := nats.GetDefaultOptions()
	copts.Servers = servers
	copts.AllowReconnect = true
	copts.NoRandomize = true
	copts.MaxReconnect = 10
	copts.ReconnectWait = 100 * time.Millisecond
	nats.ReconnectJitter(0, 0)(&copts)

	copts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect
	nc, err := copts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	// Override default handler for test.
	nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {})

	// Stop the server
	ts.Shutdown()

	// The client will try to connect to the second server, and that
	// should fail. It should then try to connect to the third and succeed.

	// Wait for the reconnect CB.
	if e := Wait(reconnectch); e != nil {
		t.Fatal("Reconnect callback should have been triggered")
	}

	if nc.IsClosed() {
		t.Fatal("Should have reconnected")
	}

	if nc.ConnectedUrl() != servers[2] {
		t.Fatalf("Should have reconnected to %s, reconnected to %s instead", servers[2], nc.ConnectedUrl())
	}
}

func TestTokenHandlerReconnect(t *testing.T) {
	var servers = []string{
		"nats://127.0.0.1:8232",
		"nats://127.0.0.1:8233",
	}

	ts := RunServerOnPort(8232)
	defer ts.Shutdown()

	opts2 := test.DefaultTestOptions
	opts2.Port = 8233
	secret := "S3Cr3T0k3n!"
	opts2.Authorization = secret
	ts2 := RunServerWithOptions(&opts2)
	defer ts2.Shutdown()

	reconnectch := make(chan bool)
	defer close(reconnectch)

	copts := nats.GetDefaultOptions()
	copts.Servers = servers
	copts.AllowReconnect = true
	copts.NoRandomize = true
	copts.MaxReconnect = 10
	copts.ReconnectWait = 100 * time.Millisecond
	nats.ReconnectJitter(0, 0)(&copts)

	copts.TokenHandler = func() string {
		return secret
	}

	copts.ReconnectedCB = func(_ *nats.Conn) {
		reconnectch <- true
	}

	// Connect
	nc, err := copts.Connect()
	if err != nil {
		t.Fatalf("Should have connected ok: %v", err)
	}
	defer nc.Close()

	// Stop the server
	ts.Shutdown()

	// The client will try to connect to the second server and succeed.

	// Wait for the reconnect CB.
	if e := Wait(reconnectch); e != nil {
		t.Fatal("Reconnect callback should have been triggered")
	}

	if nc.IsClosed() {
		t.Fatal("Should have reconnected")
	}

	if nc.ConnectedUrl() != servers[1] {
		t.Fatalf("Should have reconnected to %s, reconnected to %s instead", servers[1], nc.ConnectedUrl())
	}
}

func TestTokenAuth(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	secret := "S3Cr3T0k3n!"
	opts.Authorization = secret
	s := RunServerWithOptions(&opts)
	defer s.Shutdown()

	_, err := nats.Connect("nats://127.0.0.1:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	tokenURL := fmt.Sprintf("nats://%s@127.0.0.1:8232", secret)
	nc, err := nats.Connect(tokenURL)
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect("nats://127.0.0.1:8232", nats.Token(secret))
	if err != nil {
		t.Fatalf("Should have connected successfully: %v", err)
	}
	nc.Close()
	// Verify that token cannot be set when token handler is provided.
	_, err = nats.Connect("nats://127.0.0.1:8232", nats.TokenHandler(func() string { return secret }), nats.Token(secret))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token handler cannot be provided when token is set.
	_, err = nats.Connect("nats://127.0.0.1:8232", nats.Token(secret), nats.TokenHandler(func() string { return secret }))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token in the URL takes precedence.
	nc, err = nats.Connect(tokenURL, nats.Token("badtoken"))
	if err != nil {
		t.Fatalf("Should have connected successfully: %v", err)
	}
	nc.Close()
}

func TestTokenHandlerAuth(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	secret := "S3Cr3T0k3n!"
	opts.Authorization = secret
	s := RunServerWithOptions(&opts)
	defer s.Shutdown()

	_, err := nats.Connect("nats://127.0.0.1:8232")
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	tokenURL := fmt.Sprintf("nats://%s@127.0.0.1:8232", secret)
	nc, err := nats.Connect(tokenURL)
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect("nats://127.0.0.1:8232", nats.TokenHandler(func() string { return secret }))
	if err != nil {
		t.Fatalf("Should have connected successfully: %v", err)
	}
	nc.Close()
	// Verify that token cannot be set when token handler is provided.
	_, err = nats.Connect("nats://127.0.0.1:8232", nats.TokenHandler(func() string { return secret }), nats.Token(secret))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token handler cannot be provided when token is set.
	_, err = nats.Connect("nats://127.0.0.1:8232", nats.Token(secret), nats.TokenHandler(func() string { return secret }))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token handler cannot be provided when token is in URL.
	_, err = nats.Connect(tokenURL, nats.TokenHandler(func() string { return secret }))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
}

func TestPermViolation(t *testing.T) {
	opts := test.DefaultTestOptions
	opts.Port = 8232
	opts.Users = []*server.User{
		{
			Username: "ivan",
			Password: "pwd",
			Permissions: &server.Permissions{
				Publish:   &server.SubjectPermission{Allow: []string{"Foo"}},
				Subscribe: &server.SubjectPermission{Allow: []string{"Bar"}},
			},
		},
	}
	s := RunServerWithOptions(&opts)
	defer s.Shutdown()

	errCh := make(chan error, 2)
	errCB := func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errCh <- err
	}
	nc, err := nats.Connect(
		fmt.Sprintf("nats://ivan:pwd@127.0.0.1:%d", opts.Port),
		nats.ErrorHandler(errCB))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// Cause a publish error
	nc.Publish("Bar", []byte("fail"))
	// Cause a subscribe error
	nc.Subscribe("Foo", func(_ *nats.Msg) {})

	expectedErrorTypes := []string{"publish", "subscription"}
	for _, expectedErr := range expectedErrorTypes {
		select {
		case e := <-errCh:
			if !strings.Contains(strings.ToLower(e.Error()), nats.PERMISSIONS_ERR) {
				t.Fatalf("Did not receive error about permissions")
			}
			if !strings.Contains(strings.ToLower(e.Error()), expectedErr) {
				t.Fatalf("Did not receive error about %q, got %v", expectedErr, e.Error())
			}
			// Make sure subject is not converted to lower case
			if expectedErr == "publish" && !strings.Contains(e.Error(), "Bar") {
				t.Fatalf("Subject Bar not found in error: %v", e)
			} else if expectedErr == "subscribe" && !strings.Contains(e.Error(), "Foo") {
				t.Fatalf("Subject Foo not found in error: %v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get the permission error")
		}
	}
	// Make sure connection has not been closed
	if nc.IsClosed() {
		t.Fatal("Connection should be not be closed")
	}
}

func TestConnectMissingCreds(t *testing.T) {
	s := RunServerOnPort(-1)
	defer s.Shutdown()

	// Using TEST-NET sample address from RFC 5737.
	testnetaddr := "nats://192.0.2.2:4222"
	_, err := nats.Connect(fmt.Sprintf("%s,%s", s.ClientURL(), testnetaddr), nats.UserCredentials("missing"), nats.DontRandomize())
	if !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("Expected not exists error, got: %v", err)
	}
}

func TestUserInfoHandler(t *testing.T) {
	conf := createConfFile(t, []byte(`
	listen: 127.0.0.1:-1
	accounts: {
	  A {
		users: [{ user: "pp", password: "foo" }]
	  }
	}
`))
	defer os.Remove(conf)

	s, _ := RunServerWithConfig(conf)
	defer s.Shutdown()

	user, pass := "pp", "foo"
	userInfoCB := func() (string, string) {
		return user, pass
	}

	// check that we cannot set the user info twice
	_, err := nats.Connect(s.ClientURL(), nats.UserInfo("pp", "foo"), nats.UserInfoHandler(userInfoCB))
	if !errors.Is(err, nats.ErrUserInfoAlreadySet) {
		t.Fatalf("Expected ErrUserInfoAlreadySet, got: %v", err)
	}

	addr, ok := s.Addr().(*net.TCPAddr)
	if !ok {
		t.Fatalf("Expected a TCP address, got %T", addr)
	}

	// check that user/pass from url takes precedence
	_, err = nats.Connect(fmt.Sprintf("nats://bad:bad@localhost:%d", addr.Port),
		nats.UserInfoHandler(userInfoCB))
	if !errors.Is(err, nats.ErrAuthorization) {
		t.Fatalf("Expected ErrAuthorization, got: %v", err)
	}

	// connect using the handler
	nc, err := nats.Connect(s.ClientURL(),
		nats.ReconnectWait(100*time.Millisecond),
		nats.UserInfoHandler(userInfoCB))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// now change the password and reload the server
	newConfig := []byte(`
	listen: 127.0.0.1:-1
	accounts: {
	  A {
		users: [{ user: "dd", password: "bar" }]
	  }
	}
`)
	if err := os.WriteFile(conf, newConfig, 0666); err != nil {
		t.Fatalf("Error writing conf file: %v", err)
	}

	// update the user info used by the callback
	user, pass = "dd", "bar"

	status := nc.StatusChanged(nats.CONNECTED)

	if err := s.Reload(); err != nil {
		t.Fatalf("Error on reload: %v", err)
	}

	// we should get a reconnected event meaning the new credentials were used
	WaitOnChannel(t, status, nats.CONNECTED)
}
