//go:build testservice

package test

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/internal/testclient/testservice"
)

func TestCloseLeakingGoRoutinesTestServer(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		base := getStableNumGoroutine(t)

		nc.Flush()
		nc.Close()

		checkNoGoroutineLeak(t, base, "Close()")

		// Make sure we can call Close() multiple times
		nc.Close()
	})
}

func TestLeakingGoRoutinesOnFailedConnectTestServer(t *testing.T) {
	base := getStableNumGoroutine(t)

	nc, err := nats.Connect("localhost:1")
	if err == nil {
		nc.Close()
		t.Fatalf("Expected failure to connect")
	}

	checkNoGoroutineLeak(t, base, "failed connect")
}

func TestTLSConnectionStateNonTLSTestServer(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		_, err := nc.TLSConnectionState()
		if err != nats.ErrConnectionNotTLS {
			t.Fatalf("Expected a not tls error, got: %v", err)
		}
	})
}

func TestConnectedServerTestServer(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		u := nc.ConnectedUrl()
		if u == "" || u != inst.Servers[0].URL {
			t.Fatalf("Unexpected connected URL of %s\n", u)
		}
		id := nc.ConnectedServerId()
		if id == "" {
			t.Fatalf("Expected a connected server id, got %s", id)
		}
		name := nc.ConnectedServerName()
		if name == "" {
			t.Fatalf("Expected a connected server name, got %s", name)
		}
		jsEnabled, _ := nc.ConnectedServerJetStream()
		if jsEnabled {
			t.Fatalf("Expected JetStream to be disabled")
		}
		if nc.IsSystemAccount() {
			t.Fatalf("Expected non-system account")
		}

		nc.Close()
		u = nc.ConnectedUrl()
		if u != "" {
			t.Fatalf("Expected a nil connected URL, got %s\n", u)
		}
		id = nc.ConnectedServerId()
		if id != "" {
			t.Fatalf("Expected a nil connect server, got %s", id)
		}
		name = nc.ConnectedServerName()
		if name != "" {
			t.Fatalf("Expected a nil connect server name, got %s", name)
		}
		jsEnabled, _ = nc.ConnectedServerJetStream()
		if jsEnabled {
			t.Fatalf("Expected JetStream to be disabled after close")
		}
		if nc.IsSystemAccount() {
			t.Fatalf("Expected non-system account after close")
		}
	})
}

func TestConnectedServerJetStreamTestServer(t *testing.T) {
	withTesterJSServer(t, func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext) {
		jsEnabled, jsApiLevel := nc.ConnectedServerJetStream()
		if !jsEnabled {
			t.Fatalf("Expected JetStream to be enabled")
		}
		if jsApiLevel == 0 {
			t.Fatalf("Expected non-zero JetStream API level")
		}
	})
}

func TestIsSystemAccountTestServer(t *testing.T) {

	const accountsSnippet = `
	accounts: {
		SYS: {
			users: [ {user: "sys", password: "pass"} ]
		}
		APP: {
			users: [ {user: "app", password: "pass"} ]
		}
	}`

	const sysAccountSnippet = `system_account: SYS`
	const authSnippet = `# auth required, no no_auth_user`

	c := newTester(t)
	inst := c.CreateServer(t, false,
		testservice.WithAccounts(accountsSnippet),
		testservice.WithAuthorization(authSnippet),
		testservice.WithSystemAccount(sysAccountSnippet))
	defer inst.Destroy(t)
	nc, err := nats.Connect(inst.Servers[0].URL, nats.UserInfo("sys", "pass"))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer nc.Close()

	// IsSystemAccount is sent by the server in an async INFO after connect.
	checkFor(t, time.Second, 15*time.Millisecond, func() error {
		if !nc.IsSystemAccount() {
			return fmt.Errorf("expected system account")
		}
		return nil
	})

	nc2, err := nats.Connect(inst.Servers[0].URL, nats.UserInfo("app", "pass"))
	if err != nil {
		t.Fatalf("Error connecting: %v", err)
	}
	defer nc2.Close()

	if nc2.IsSystemAccount() {
		t.Fatalf("Expected non-system account")
	}
}

func TestMultipleCloseTestServer(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		var wg sync.WaitGroup
		for i := 0; i < 10; i++ {
			wg.Add(1)
			go func() {
				nc.Close()
				wg.Done()
			}()
		}
		wg.Wait()
	})
}

func TestBadOptionTimeoutConnectTestServer(t *testing.T) {
	c := newTester(t)
	inst := c.CreateServer(t, false)
	defer inst.Destroy(t)

	opts := nats.GetDefaultOptions()
	opts.Timeout = -1
	opts.Url = inst.Servers[0].URL

	_, err := opts.Connect()
	if err == nil {
		t.Fatal("Expected an error")
	}
	if !strings.Contains(err.Error(), "invalid") {
		t.Fatalf("Expected a ErrNoServers error: Got %v\n", err)
	}
}

func TestSimplePublishTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		if err := nc.Publish("foo", []byte("Hello World")); err != nil {
			t.Fatal("Failed to publish string message: ", err)
		}
	})

}

func TestSimplePublishNoDataTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		if err := nc.Publish("foo", nil); err != nil {
			t.Fatal("Failed to publish empty message: ", err)
		}
	})
}

func TestPublishDoesNotFailOnSlowConsumerTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		// Override default handler for test.
		nc.SetErrorHandler(func(_ *nats.Conn, _ *nats.Subscription, _ error) {})

		sub, err := nc.SubscribeSync("foo")
		if err != nil {
			t.Fatalf("Unable to create subscription: %v", err)
		}

		if err := sub.SetPendingLimits(1, 1000); err != nil {
			t.Fatalf("Unable to set pending limits: %v", err)
		}
		var pubErr error

		msg := []byte("Hello")
		for i := 0; i < 10; i++ {
			pubErr = nc.Publish("foo", msg)
			if pubErr != nil {
				break
			}
			nc.Flush()
		}

		if pubErr != nil {
			t.Fatalf("Publish() should not fail because of slow consumer. Got '%v'", pubErr)
		}
	})
}

func TestAsyncSubscribeTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		omsg := []byte("Hello World")
		ch := make(chan bool)

		// Callback is mandatory
		if _, err := nc.Subscribe("foo", nil); err == nil {
			t.Fatal("Creating subscription without callback should have failed")
		}

		_, err := nc.Subscribe("foo", func(m *nats.Msg) {
			if !bytes.Equal(m.Data, omsg) {
				t.Fatal("Message received does not match")
			}
			if m.Sub == nil {
				t.Fatal("Callback does not have a valid Subscription")
			}
			ch <- true
		})
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}
		nc.Publish("foo", omsg)
		if e := Wait(ch); e != nil {
			t.Fatal("Message not received for subscription")
		}
	})
}

func TestAsyncSubscribeRoutineLeakOnUnsubscribeTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		ch := make(chan bool)

		// Take the base once the connection is established, but before
		// the subscriber is created.
		base := getStableNumGoroutine(t)

		sub, err := nc.Subscribe("foo", func(m *nats.Msg) { ch <- true })
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}

		// Send to ourself
		nc.Publish("foo", []byte("hello"))

		// This ensures that the async delivery routine is up and running.
		if err := Wait(ch); err != nil {
			t.Fatal("Failed to receive message")
		}

		// Make sure to give it time to go back into wait
		time.Sleep(200 * time.Millisecond)

		// Explicit unsubscribe
		sub.Unsubscribe()

		checkNoGoroutineLeak(t, base, "Unsubscribe()")
	})
}

func TestAsyncSubscribeRoutineLeakOnCloseTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		ch := make(chan bool)

		// Take the base before creating the connection, since we are going
		// to close it before taking the delta.
		base := getStableNumGoroutine(t)

		_, err := nc.Subscribe("foo", func(m *nats.Msg) { ch <- true })
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}

		// Send to ourself
		nc.Publish("foo", []byte("hello"))

		// This ensures that the async delivery routine is up and running.
		if err := Wait(ch); err != nil {
			t.Fatal("Failed to receive message")
		}

		// Make sure to give it time to go back into wait
		time.Sleep(200 * time.Millisecond)

		// Close connection without explicit unsubscribe
		nc.Close()

		checkNoGoroutineLeak(t, base, "Close()")
	})
}

func TestSyncSubscribeTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		sub, err := nc.SubscribeSync("foo")
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}
		omsg := []byte("Hello World")
		nc.Publish("foo", omsg)
		msg, err := sub.NextMsg(1 * time.Second)
		if err != nil || !bytes.Equal(msg.Data, omsg) {
			t.Fatal("Message received does not match")
		}
	})
}

func TestPubSubWithReplyTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		sub, err := nc.SubscribeSync("foo")
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}
		omsg := []byte("Hello World")
		nc.PublishMsg(&nats.Msg{Subject: "foo", Reply: "bar", Data: omsg})
		msg, err := sub.NextMsg(10 * time.Second)
		if err != nil || !bytes.Equal(msg.Data, omsg) {
			t.Fatal("Message received does not match")
		}
	})
}

func TestMsgRespondTester(t *testing.T) {
	withTesterServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		m := &nats.Msg{}
		if err := m.Respond(nil); err != nats.ErrMsgNotBound {
			t.Fatal("Expected ErrMsgNotBound error")
		}

		sub, err := nc.Subscribe("req", func(msg *nats.Msg) {
			msg.Respond([]byte("42"))
		})
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}

		// Fake the bound notion by assigning Sub directly to test no reply.
		m.Sub = sub
		if err := m.Respond(nil); err != nats.ErrMsgNoReply {
			t.Fatal("Expected ErrMsgNoReply error")
		}

		response, err := nc.Request("req", []byte("help"), 50*time.Millisecond)
		if err != nil {
			t.Fatal("Request Failed: ", err)
		}

		if string(response.Data) != "42" {
			t.Fatalf("Expected '42', got %q", response.Data)
		}
	})

}

// by TESTER_NATS_URL. Tests skip when the env var is unset so a `go test
// -tags=testservice ./...` run does not fail on developer machines without
// docker.
func newTester(t *testing.T) *testservice.Client {
	t.Helper()
	url := os.Getenv("TESTER_NATS_URL")
	if url == "" {
		t.Skip("TESTER_NATS_URL not set; skipping testservice PoC")
	}
	c := testservice.New(t, url)
	t.Cleanup(func() { c.Close(t) })
	return c
}

// withTesterJSServer creates a single JetStream server via the tester, opens
// a connection and a jetstream.JetStream handle, runs the callback, and
// destroys the instance on return.
func withTesterServer(t *testing.T, fn func(t *testing.T, nc *nats.Conn, inst *testservice.Instance)) {
	t.Helper()
	c := newTester(t)
	c.WithServer(t, func(t *testing.T, nc *nats.Conn, inst *testservice.Instance) {
		fn(t, nc, inst)
	})
}

func withTesterJSServer(t *testing.T, fn func(t *testing.T, nc *nats.Conn, js nats.JetStreamContext)) {
	t.Helper()
	c := newTester(t)
	c.WithJetStreamServer(t, func(t *testing.T, nc *nats.Conn, _ *testservice.Instance) {
		js, err := nc.JetStream()
		if err != nil {
			t.Fatalf("jetstream.New: %v", err)
		}
		fn(t, nc, js)
	})
}
