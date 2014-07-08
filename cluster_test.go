package nats

import (
	"math"
	"reflect"
	"regexp"
	"sync"
	"testing"
	"time"
)

var testServers = []string{
	"nats://localhost:1222",
	"nats://localhost:1223",
	"nats://localhost:1224",
	"nats://localhost:1225",
	"nats://localhost:1226",
	"nats://localhost:1227",
	"nats://localhost:1228",
}

func TestServersRandomize(t *testing.T) {
	opts := DefaultOptions
	opts.Servers = testServers
	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	// Build []string from srvPool
	clientServers := []string{}
	for _, s := range nc.srvPool {
		clientServers = append(clientServers, s.url.String())
	}
	// In theory this could happen..
	if reflect.DeepEqual(testServers, clientServers) {
		t.Fatalf("ServerPool list not randomized\n")
	}

	// Now test that we do not randomize if proper flag is set.
	opts = DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true
	nc = &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	// Build []string from srvPool
	clientServers = []string{}
	for _, s := range nc.srvPool {
		clientServers = append(clientServers, s.url.String())
	}
	if !reflect.DeepEqual(testServers, clientServers) {
		t.Fatalf("ServerPool list should not be randomized\n")
	}
}

func TestServersOption(t *testing.T) {
	opts := DefaultOptions
	opts.NoRandomize = true

	_, err := opts.Connect()
	if err != ErrNoServers {
		t.Fatalf("Wrong error: '%s'\n", err)
	}
	opts.Servers = testServers
	_, err = opts.Connect()
	if err == nil || err != ErrNoServers {
		t.Fatalf("Did not receive proper error: %v\n", err)
	}

	// Make sure we can connect to first server if running
	s1 := startServer(t, 1222, "")
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Could not connect: %v\n", err)
	}
	if nc.ConnectedUrl() != "nats://localhost:1222" {
		t.Fatalf("Does not report correct connection: %s\n",
			nc.ConnectedUrl())
	}
	nc.Close()
	s1.stopServer()

	// Make sure we can connect to a non first server if running
	s2 := startServer(t, 1223, "")
	nc, err = opts.Connect()
	if err != nil {
		t.Fatalf("Could not connect: %v\n", err)
	}
	if nc.ConnectedUrl() != "nats://localhost:1223" {
		t.Fatalf("Does not report correct connection: %s\n",
			nc.ConnectedUrl())
	}
	nc.Close()
	s2.stopServer()
}

func TestAuthServers(t *testing.T) {

	var plainServers = []string{
		"nats://localhost:1222",
		"nats://localhost:1224",
	}

	as1 := startServer(t, 1222, "--user derek --pass foo")
	defer as1.stopServer()
	as2 := startServer(t, 1224, "--user derek --pass foo")
	defer as2.stopServer()

	opts := DefaultOptions
	opts.NoRandomize = true
	opts.Servers = plainServers
	_, err := opts.Connect()

	if err == nil {
		t.Fatalf("Expect Auth failure, got no error\n")
	}

	if matched, _ := regexp.Match(`Authorization`, []byte(err.Error())); !matched {
		t.Fatalf("Wrong error, wanted Auth failure, got '%s'\n", err)
	}

	// Test that we can connect to a subsequent correct server.
	var authServers = []string{
		"nats://localhost:1222",
		"nats://derek:foo@localhost:1224",
	}

	opts.Servers = authServers
	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Expected to connect properly: %v\n", err)
	}
	if nc.ConnectedUrl() != authServers[1] {
		t.Fatalf("Does not report correct connection: %s\n",
			nc.ConnectedUrl())
	}
}

func TestSelectNextServer(t *testing.T) {
	opts := DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true
	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	if nc.url != nc.srvPool[0].url {
		t.Fatalf("Wrong default selection: %v\n", nc.url)
	}

	sel, err := nc.selectNextServer()
	if err != nil {
		t.Fatalf("Got an err: %v\n", err)
	}
	// Check that we are now looking at #2, and current is now last.
	if len(nc.srvPool) != len(testServers) {
		t.Fatalf("List is incorrect size: %d vs %d\n", len(nc.srvPool), len(testServers))
	}
	if nc.url.String() != testServers[1] {
		t.Fatalf("Selection incorrect: %v vs %v\n", nc.url, testServers[1])
	}
	if nc.srvPool[len(nc.srvPool)-1].url.String() != testServers[0] {
		t.Fatalf("Did not push old to last position\n")
	}
	if sel != nc.srvPool[0] {
		t.Fatalf("Did not return correct server: %v vs %v\n", sel.url, nc.srvPool[0].url)
	}

	// Test that we do not keep servers where we have tried to reconnect past our limit.
	nc.srvPool[0].reconnects = int(opts.MaxReconnect)
	if _, err := nc.selectNextServer(); err != nil {
		t.Fatalf("Got an err: %v\n", err)
	}
	// Check that we are now looking at #3, and current is not in the list.
	if len(nc.srvPool) != len(testServers)-1 {
		t.Fatalf("List is incorrect size: %d vs %d\n", len(nc.srvPool), len(testServers)-1)
	}
	if nc.url.String() != testServers[2] {
		t.Fatalf("Selection incorrect: %v vs %v\n", nc.url, testServers[2])
	}
	if nc.srvPool[len(nc.srvPool)-1].url.String() == testServers[1] {
		t.Fatalf("Did not throw away the last server correctly\n")
	}
}

func TestBasicClusterReconnect(t *testing.T) {
	s1 := startServer(t, 1222, "")
	s2 := startServer(t, 1224, "")
	defer s2.stopServer()

	opts := DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true

	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *Conn) {
		dch <- true
	}

	rch := make(chan bool)
	opts.ReconnectedCB = func(_ *Conn) {
		rch <- true
	}

	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Expected to connect, got err: %v\n", err)
	}

	s1.stopServer()

	// wait for disconnect
	if e := waitTime(dch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	reconnectTimeStart := time.Now()

	// wait for reconnect
	if e := waitTime(rch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a reconnect callback message")
	}

	if nc.ConnectedUrl() != testServers[2] {
		t.Fatalf("Does not report correct connection: %s\n",
			nc.ConnectedUrl())
	}

	// Make sure we did not wait on reconnect for default time.
	reconnectTime := time.Since(reconnectTimeStart)
	if reconnectTime > (100 * time.Millisecond) {
		t.Fatalf("Took longer than expected to reconnect: %v\n", reconnectTime)
	}
}

func TestHotSpotReconnect(t *testing.T) {
	s1 := startServer(t, 1222, "")

	numClients := 100
	clients := []*Conn{}

	wg := &sync.WaitGroup{}
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		opts := DefaultOptions
		opts.Servers = testServers
		opts.ReconnectedCB = func(_ *Conn) {
			wg.Done()
		}
		nc, err := opts.Connect()
		if err != nil {
			t.Fatalf("Expected to connect, got err: %v\n", err)
		}
		if nc.ConnectedUrl() != testServers[0] {
			t.Fatalf("Connected to incorrect server: %v\n", nc.ConnectedUrl())
		}
		clients = append(clients, nc)
	}

	s2 := startServer(t, 1224, "")
	defer s2.stopServer()
	s3 := startServer(t, 1226, "")
	defer s3.stopServer()

	s1.stopServer()

	numServers := 2

	// Wait on all reconnects
	wg.Wait()

	// Walk the clients and calculate how many of each..
	cs := make(map[string]int)
	for _, nc := range clients {
		cs[nc.ConnectedUrl()] += 1
		nc.Close()
	}
	if len(cs) != numServers {
		t.Fatalf("Wrong number or reported servers: %d vs %d\n", len(cs), numServers)
	}
	expected := numClients / numServers
	v := uint(float32(expected) * 0.30)

	// Check that each item is within acceptable range
	for s, total := range cs {
		delta := uint(math.Abs(float64(expected - total)))
		if delta > v {
			t.Fatalf("Connected clients to server: %s out of range: %d\n", s, total)
		}
	}
}

func TestProperReconnectDelay(t *testing.T) {
	s1 := startServer(t, 1222, "")

	opts := DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true

	dcbCalled := false
	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *Conn) {
		dcbCalled = true
		dch <- true
	}

	closedCbCalled := false
	opts.ClosedCB = func(_ *Conn) {
		closedCbCalled = true
	}

	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Expected to connect, got err: %v\n", err)
	}

	s1.stopServer()

	// wait for disconnect
	if e := waitTime(dch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	// Wait, want to make sure we don't spin on reconnect to non-existant servers.
	time.Sleep(1 * time.Second)

	// Make sure we are still reconnecting..
	if closedCbCalled {
		t.Fatal("Closed CB was triggered, should not have been.")
	}
	if nc.status != RECONNECTING {
		t.Fatalf("Wrong status: %d\n", nc.status)
	}
}

func TestProperFalloutAfterMaxAttempts(t *testing.T) {
	s1 := startServer(t, 1222, "")

	opts := DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true
	opts.MaxReconnect = 5
	opts.ReconnectWait = (10 * time.Millisecond)

	dcbCalled := false
	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *Conn) {
		dcbCalled = true
		dch <- true
	}

	closedCbCalled := false
	cch := make(chan bool)

	opts.ClosedCB = func(_ *Conn) {
		closedCbCalled = true
		cch <- true
	}

	nc, err := opts.Connect()
	if err != nil {
		t.Fatalf("Expected to connect, got err: %v\n", err)
	}

	s1.stopServer()

	// wait for disconnect
	if e := waitTime(dch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	// Wait for ClosedCB
	if e := waitTime(cch, 1*time.Second); e != nil {
		t.Fatal("Did not receive a closed callback message")
	}

	// Make sure we are still reconnecting..
	if !closedCbCalled {
		t.Logf("%+v\n", nc)
		t.Fatal("Closed CB was not triggered, should have been.")
	}

	if nc.IsClosed() != true {
		t.Fatalf("Wrong status: %d\n", nc.status)
	}
}

func TestTimeoutOnNoServers(t *testing.T) {
	s1 := startServer(t, 1222, "")

	opts := DefaultOptions
	opts.Servers = testServers
	opts.NoRandomize = true

	// 100 milliseconds total time wait
	opts.MaxReconnect = 10
	opts.ReconnectWait = (10 * time.Millisecond)

	dcbCalled := false
	dch := make(chan bool)
	opts.DisconnectedCB = func(_ *Conn) {
		dcbCalled = true
		dch <- true
	}

	cch := make(chan bool)
	opts.ClosedCB = func(_ *Conn) {
		cch <- true
	}

	if _, err := opts.Connect(); err != nil {
		t.Fatalf("Expected to connect, got err: %v\n", err)
	}

	s1.stopServer()

	// wait for disconnect
	if e := waitTime(dch, 2*time.Second); e != nil {
		t.Fatal("Did not receive a disconnect callback message")
	}

	startWait := time.Now()

	// Wait for ClosedCB
	if e := waitTime(cch, 1*time.Second); e != nil {
		t.Fatal("Did not receive a closed callback message")
	}

	timeWait := time.Since(startWait)

	// Use 50ms as variable time delta
	variable := (50 * time.Millisecond)
	expected := (time.Duration(opts.MaxReconnect) * opts.ReconnectWait)

	if timeWait > (expected + variable) {
		t.Fatalf("Waited too long for Closed state: %d\n", timeWait/time.Millisecond)
	}
}
