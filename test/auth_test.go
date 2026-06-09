// Copyright 2012-2026 The NATS Authors
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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

func TestAuth(t *testing.T) {
	c := newTester(t)
	authBody := `authorization {
  user:     derek
  password: foo
}`
	inst := c.CreateServer(t, false, singleUserPassOpts(authBody)...)
	t.Cleanup(func() { inst.Destroy(t) })

	url := inst.Servers[0].URL
	hostPort := hostPortFromURL(url)

	_, err := nats.Connect(url)
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

	nc, err := nats.Connect(fmt.Sprintf("nats://derek:foo@%s", hostPort))
	if err != nil {
		t.Fatal("Should have connected successfully with a token")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect(url, nats.UserInfo("derek", "foo"))
	if err != nil {
		t.Fatalf("Should have connected successfully with a token: %v", err)
	}
	nc.Close()
	// Verify that credentials in URL take precedence.
	nc, err = nats.Connect(fmt.Sprintf("nats://derek:foo@%s", hostPort), nats.UserInfo("foo", "bar"))
	if err != nil {
		t.Fatalf("Should have connected successfully with a token: %v", err)
	}
	nc.Close()
}

func TestAuthFailNoDisconnectErrCB(t *testing.T) {
	c := newTester(t)
	authBody := `authorization {
  user:     derek
  password: foo
}`
	inst := c.CreateServer(t, false, singleUserPassOpts(authBody)...)
	t.Cleanup(func() { inst.Destroy(t) })

	copts := nats.GetDefaultOptions()
	copts.Url = inst.Servers[0].URL
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
	c := newTester(t)

	ts := c.CreateServer(t, false)
	t.Cleanup(func() { ts.Destroy(t) })

	authBody := `authorization {
  user:     ivan
  password: foo
}`
	ts2 := c.CreateServer(t, false, singleUserPassOpts(authBody)...)
	t.Cleanup(func() { ts2.Destroy(t) })

	ts3 := c.CreateServer(t, false)
	t.Cleanup(func() { ts3.Destroy(t) })

	servers := []string{
		ts.Servers[0].URL,
		ts2.Servers[0].URL,
		ts3.Servers[0].URL,
	}

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
	ts.StopServer(t, ts.Servers[0])

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
	c := newTester(t)

	// Server 1 has no authorization at all — neutralize the default
	// accounts/system_account/no_auth_user so any client (including ones
	// sending a token) is allowed in.
	ts := c.CreateServer(t, false, singleUserPassOpts(`# no authorization required`)...)
	t.Cleanup(func() { ts.Destroy(t) })

	secret := "S3Cr3T0k3n!"
	authBody := fmt.Sprintf(`authorization {
  token: %q
}`, secret)
	ts2 := c.CreateServer(t, false, singleUserPassOpts(authBody)...)
	t.Cleanup(func() { ts2.Destroy(t) })

	servers := []string{
		ts.Servers[0].URL,
		ts2.Servers[0].URL,
	}

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
	ts.StopServer(t, ts.Servers[0])

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
	c := newTester(t)
	secret := "S3Cr3T0k3n!"
	authBody := fmt.Sprintf(`authorization {
  token: %q
}`, secret)
	inst := c.CreateServer(t, false, singleUserPassOpts(authBody)...)
	t.Cleanup(func() { inst.Destroy(t) })

	url := inst.Servers[0].URL
	hostPort := hostPortFromURL(url)

	_, err := nats.Connect(url)
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	tokenURL := fmt.Sprintf("nats://%s@%s", secret, hostPort)
	nc, err := nats.Connect(tokenURL)
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect(url, nats.Token(secret))
	if err != nil {
		t.Fatalf("Should have connected successfully: %v", err)
	}
	nc.Close()
	// Verify that token cannot be set when token handler is provided.
	_, err = nats.Connect(url, nats.TokenHandler(func() string { return secret }), nats.Token(secret))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token handler cannot be provided when token is set.
	_, err = nats.Connect(url, nats.Token(secret), nats.TokenHandler(func() string { return secret }))
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
	c := newTester(t)
	secret := "S3Cr3T0k3n!"
	authBody := fmt.Sprintf(`authorization {
  token: %q
}`, secret)
	inst := c.CreateServer(t, false, singleUserPassOpts(authBody)...)
	t.Cleanup(func() { inst.Destroy(t) })

	url := inst.Servers[0].URL
	hostPort := hostPortFromURL(url)

	_, err := nats.Connect(url)
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}

	tokenURL := fmt.Sprintf("nats://%s@%s", secret, hostPort)
	nc, err := nats.Connect(tokenURL)
	if err != nil {
		t.Fatal("Should have connected successfully")
	}
	nc.Close()

	// Use Options
	nc, err = nats.Connect(url, nats.TokenHandler(func() string { return secret }))
	if err != nil {
		t.Fatalf("Should have connected successfully: %v", err)
	}
	nc.Close()
	// Verify that token cannot be set when token handler is provided.
	_, err = nats.Connect(url, nats.TokenHandler(func() string { return secret }), nats.Token(secret))
	if err == nil {
		t.Fatal("Should have received an error while trying to connect")
	}
	// Verify that token handler cannot be provided when token is set.
	_, err = nats.Connect(url, nats.Token(secret), nats.TokenHandler(func() string { return secret }))
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
	c := newTester(t)
	authBody := `authorization {
  users: [
    {
      user: "ivan"
      password: "pwd"
      permissions: {
        publish:   { allow: ["Foo"] }
        subscribe: { allow: ["Bar"] }
      }
    }
  ]
}`
	inst := c.CreateServer(t, false, singleUserPassOpts(authBody)...)
	t.Cleanup(func() { inst.Destroy(t) })

	hostPort := hostPortFromURL(inst.Servers[0].URL)

	errCh := make(chan error, 2)
	errCB := func(_ *nats.Conn, _ *nats.Subscription, err error) {
		errCh <- err
	}
	nc, err := nats.Connect(
		fmt.Sprintf("nats://ivan:pwd@%s", hostPort),
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
	withServerInstance(t, func(t *testing.T, _ *nats.Conn, inst *testservice.Instance) {
		// Using TEST-NET sample address from RFC 5737.
		testnetaddr := "nats://192.0.2.2:4222"
		_, err := nats.Connect(fmt.Sprintf("%s,%s", inst.Servers[0].URL, testnetaddr), nats.UserCredentials("missing"), nats.DontRandomize())
		if !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("Expected not exists error, got: %v", err)
		}
	})
}

func TestUserInfoHandler(t *testing.T) {
	c := newTester(t)
	initialAccounts := `accounts: {
  A {
    users: [{ user: "pp", password: "foo" }]
  }
}`
	inst := c.CreateServer(t, false,
		testservice.WithAccounts(initialAccounts),
		testservice.WithAuthorization("# no_auth_user intentionally unset"),
		testservice.WithSystemAccount(noSystemAccountBody()),
	)
	t.Cleanup(func() { inst.Destroy(t) })

	user, pass := "pp", "foo"
	userInfoCB := func() (string, string) {
		return user, pass
	}

	// cannot set the user info twice
	_, err := nats.Connect(inst.Servers[0].URL, nats.UserInfo("pp", "foo"), nats.UserInfoHandler(userInfoCB))
	if !errors.Is(err, nats.ErrUserInfoAlreadySet) {
		t.Fatalf("Expected ErrUserInfoAlreadySet, got: %v", err)
	}

	// user/pass from url takes precedence
	badURL := fmt.Sprintf("nats://bad:bad@%s:%d", testerHost(t), inst.Servers[0].Port)
	_, err = nats.Connect(badURL, nats.UserInfoHandler(userInfoCB))
	if !errors.Is(err, nats.ErrAuthorization) {
		t.Fatalf("Expected ErrAuthorization, got: %v", err)
	}

	nc, err := nats.Connect(inst.Servers[0].URL,
		nats.ReconnectWait(100*time.Millisecond),
		nats.UserInfoHandler(userInfoCB))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc.Close()

	// swap the user via UpdateServer + ReloadServer (SIGHUP)
	newAccounts := `accounts: {
  A {
    users: [{ user: "dd", password: "bar" }]
  }
}`
	inst.UpdateServer(t, inst.Servers[0],
		testservice.WithAccounts(newAccounts),
		testservice.WithAuthorization("# no_auth_user intentionally unset"),
		testservice.WithSystemAccount(noSystemAccountBody()),
	)

	user, pass = "dd", "bar"
	status := nc.StatusChanged(nats.CONNECTED)
	inst.ReloadServer(t, inst.Servers[0])
	WaitOnChannel(t, status, nats.CONNECTED)
}
