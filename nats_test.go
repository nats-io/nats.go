// Copyright 2012-2020 The NATS Authors
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

package nats

////////////////////////////////////////////////////////////////////////////////
// Package scoped specific tests here..
////////////////////////////////////////////////////////////////////////////////

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"os"
	"reflect"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestVersion(t *testing.T) {
	// Semantic versioning
	verRe := regexp.MustCompile(`\d+.\d+.\d+(-\S+)?`)
	if !verRe.MatchString(Version) {
		t.Fatalf("Version not compatible with semantic versioning: %q", Version)
	}
}

// Dumb wait program to sync on callbacks, etc... Will timeout
func Wait(ch chan bool) error {
	return WaitTime(ch, 5*time.Second)
}

func WaitTime(ch chan bool, timeout time.Duration) error {
	select {
	case <-ch:
		return nil
	case <-time.After(timeout):
	}
	return errors.New("timeout")
}

func stackFatalf(t *testing.T, f string, args ...interface{}) {
	lines := make([]string, 0, 32)
	msg := fmt.Sprintf(f, args...)
	lines = append(lines, msg)

	// Generate the Stack of callers: Skip us and verify* frames.
	for i := 1; true; i++ {
		_, file, line, ok := runtime.Caller(i)
		if !ok {
			break
		}
		msg := fmt.Sprintf("%d - %s:%d", i, file, line)
		lines = append(lines, msg)
	}
	t.Fatalf("%s", strings.Join(lines, "\n"))
}

// Check the error channel for an error and if one is present,
// calls t.Fatal(e.Error()). Note that this supports tests that
// send nil to the error channel and so report error only if
// e is != nil.
func checkErrChannel(t *testing.T, errCh chan error) {
	t.Helper()
	select {
	case e := <-errCh:
		if e != nil {
			t.Fatal(e.Error())
		}
	default:
	}
}

func TestVersionMatchesTag(t *testing.T) {
	tag := os.Getenv("TRAVIS_TAG")
	if tag == "" {
		t.SkipNow()
	}
	// We expect a tag of the form vX.Y.Z. If that's not the case,
	// we need someone to have a look. So fail if first letter is not
	// a `v`
	if tag[0] != 'v' {
		t.Fatalf("Expect tag to start with `v`, tag is: %s", tag)
	}
	// Strip the `v` from the tag for the version comparison.
	if Version != tag[1:] {
		t.Fatalf("Version (%s) does not match tag (%s)", Version, tag[1:])
	}
}

func TestExpandPath(t *testing.T) {
	if runtime.GOOS == "windows" {
		origUserProfile := os.Getenv("USERPROFILE")
		origHomeDrive, origHomePath := os.Getenv("HOMEDRIVE"), os.Getenv("HOMEPATH")
		defer func() {
			os.Setenv("USERPROFILE", origUserProfile)
			os.Setenv("HOMEDRIVE", origHomeDrive)
			os.Setenv("HOMEPATH", origHomePath)
		}()

		cases := []struct {
			path        string
			userProfile string
			homeDrive   string
			homePath    string

			wantPath string
			wantErr  bool
		}{
			// Missing HOMEDRIVE and HOMEPATH.
			{path: "/Foo/Bar", userProfile: `C:\Foo\Bar`, wantPath: "/Foo/Bar"},
			{path: "Foo/Bar", userProfile: `C:\Foo\Bar`, wantPath: "Foo/Bar"},
			{path: "~/Fizz", userProfile: `C:\Foo\Bar`, wantPath: `C:\Foo\Bar\Fizz`},
			{path: `${HOMEDRIVE}${HOMEPATH}\Fizz`, userProfile: `C:\Foo\Bar`, wantPath: `C:\Foo\Bar\Fizz`},

			// Missing USERPROFILE.
			{path: "~/Fizz", homeDrive: "X:", homePath: `\Foo\Bar`, wantPath: `X:\Foo\Bar\Fizz`},

			// Set all environment variables. HOMEDRIVE and HOMEPATH take
			// precedence.
			{path: "~/Fizz", userProfile: `C:\Foo\Bar`,
				homeDrive: "X:", homePath: `\Foo\Bar`, wantPath: `X:\Foo\Bar\Fizz`},

			// Missing all environment variables.
			{path: "~/Fizz", wantErr: true},
		}
		for i, c := range cases {
			t.Run(fmt.Sprintf("windows case %d", i), func(t *testing.T) {
				os.Setenv("USERPROFILE", c.userProfile)
				os.Setenv("HOMEDRIVE", c.homeDrive)
				os.Setenv("HOMEPATH", c.homePath)

				gotPath, err := expandPath(c.path)
				if !c.wantErr && err != nil {
					t.Fatalf("unexpected error: got=%v; want=%v", err, nil)
				} else if c.wantErr && err == nil {
					t.Fatalf("unexpected success: got=%v; want=%v", nil, "err")
				}

				if gotPath != c.wantPath {
					t.Fatalf("unexpected path: got=%v; want=%v", gotPath, c.wantPath)
				}
			})
		}

		return
	}

	// Unix tests

	origHome := os.Getenv("HOME")
	defer os.Setenv("HOME", origHome)

	cases := []struct {
		path    string
		home    string
		testEnv string

		wantPath string
		wantErr  bool
	}{
		{path: "/foo/bar", home: "/fizz/buzz", wantPath: "/foo/bar"},
		{path: "foo/bar", home: "/fizz/buzz", wantPath: "foo/bar"},
		{path: "~/fizz", home: "/foo/bar", wantPath: "/foo/bar/fizz"},
		{path: "$HOME/fizz", home: "/foo/bar", wantPath: "/foo/bar/fizz"},

		// missing HOME env var
		{path: "~/fizz", wantErr: true},
	}
	for i, c := range cases {
		t.Run(fmt.Sprintf("unix case %d", i), func(t *testing.T) {
			os.Setenv("HOME", c.home)

			gotPath, err := expandPath(c.path)
			if !c.wantErr && err != nil {
				t.Fatalf("unexpected error: got=%v; want=%v", err, nil)
			} else if c.wantErr && err == nil {
				t.Fatalf("unexpected success: got=%v; want=%v", nil, "err")
			}

			if gotPath != c.wantPath {
				t.Fatalf("unexpected path: got=%v; want=%v", gotPath, c.wantPath)
			}
		})
	}
}

////////////////////////////////////////////////////////////////////////////////
// ServerPool tests
////////////////////////////////////////////////////////////////////////////////

var testServers = []string{
	"nats://localhost:1222",
	"nats://localhost:1223",
	"nats://localhost:1224",
	"nats://localhost:1225",
	"nats://localhost:1226",
	"nats://localhost:1227",
	"nats://localhost:1228",
}

func TestSimplifiedURLs(t *testing.T) {
	opts := GetDefaultOptions()
	opts.NoRandomize = true
	opts.Servers = []string{
		"nats://host1:1234",
		"nats://host2:",
		"nats://host3",
		"host4:1234",
		"host5:",
		"host6",
		"nats://[1:2:3:4]:1234",
		"nats://[5:6:7:8]:",
		"nats://[9:10:11:12]",
		"[13:14:15:16]:",
		"[17:18:19:20]:1234",
	}

	// We expect the result in the server pool to be:
	expected := []string{
		"nats://host1:1234",
		"nats://host2:4222",
		"nats://host3:4222",
		"nats://host4:1234",
		"nats://host5:4222",
		"nats://host6:4222",
		"nats://[1:2:3:4]:1234",
		"nats://[5:6:7:8]:4222",
		"nats://[9:10:11:12]:4222",
		"nats://[13:14:15:16]:4222",
		"nats://[17:18:19:20]:1234",
	}

	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	// Check server pool directly
	for i, u := range nc.srvPool {
		if u.url.String() != expected[i] {
			t.Fatalf("Expected url %q, got %q", expected[i], u.url.String())
		}
	}
}

func TestServersRandomize(t *testing.T) {
	opts := GetDefaultOptions()
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
	opts = GetDefaultOptions()
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

	// Although the original intent was that if Opts.Url is
	// set, Opts.Servers is not (and vice versa), the behavior
	// is that Opts.Url is always first, even when randomization
	// is enabled. So make sure that this is still the case.
	opts = GetDefaultOptions()
	opts.Url = DefaultURL
	opts.Servers = testServers
	nc = &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	// Build []string from srvPool
	clientServers = []string{}
	for _, s := range nc.srvPool {
		clientServers = append(clientServers, s.url.String())
	}
	// In theory this could happen..
	if reflect.DeepEqual(testServers, clientServers) {
		t.Fatalf("ServerPool list not randomized\n")
	}
	if clientServers[0] != DefaultURL {
		t.Fatalf("Options.Url should be first in the array, got %v", clientServers[0])
	}
}

func TestSelectNextServer(t *testing.T) {
	opts := GetDefaultOptions()
	opts.Servers = testServers
	opts.NoRandomize = true
	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}
	if nc.current != nc.srvPool[0] {
		t.Fatalf("Wrong default selection: %v\n", nc.current.url)
	}

	sel, err := nc.selectNextServer()
	if err != nil {
		t.Fatalf("Got an err: %v\n", err)
	}
	// Check that we are now looking at #2, and current is now last.
	if len(nc.srvPool) != len(testServers) {
		t.Fatalf("List is incorrect size: %d vs %d\n", len(nc.srvPool), len(testServers))
	}
	if nc.current.url.String() != testServers[1] {
		t.Fatalf("Selection incorrect: %v vs %v\n", nc.current.url, testServers[1])
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
	if nc.current.url.String() != testServers[2] {
		t.Fatalf("Selection incorrect: %v vs %v\n", nc.current.url, testServers[2])
	}
	if nc.srvPool[len(nc.srvPool)-1].url.String() == testServers[1] {
		t.Fatalf("Did not throw away the last server correctly\n")
	}
}

// This will test that comma separated url strings work properly for
// the Connect() command.
func TestUrlArgument(t *testing.T) {
	check := func(url string, expected []string) {
		if !reflect.DeepEqual(processUrlString(url), expected) {
			t.Fatalf("Got wrong response processing URL: %q, RES: %#v\n", url, processUrlString(url))
		}
	}
	// This is normal case
	oneExpected := []string{"nats://localhost:1222"}

	check("nats://localhost:1222", oneExpected)
	check("nats://localhost:1222 ", oneExpected)
	check(" nats://localhost:1222", oneExpected)
	check(" nats://localhost:1222 ", oneExpected)

	var multiExpected = []string{
		"nats://localhost:1222",
		"nats://localhost:1223",
		"nats://localhost:1224",
	}

	check("nats://localhost:1222,nats://localhost:1223,nats://localhost:1224", multiExpected)
	check("nats://localhost:1222, nats://localhost:1223, nats://localhost:1224", multiExpected)
	check(" nats://localhost:1222, nats://localhost:1223, nats://localhost:1224 ", multiExpected)
	check("nats://localhost:1222,   nats://localhost:1223  ,nats://localhost:1224", multiExpected)
}

func TestParserPing(t *testing.T) {
	c := &Conn{}
	fake := &bytes.Buffer{}
	c.bw = bufio.NewWriterSize(fake, c.Opts.ReconnectBufSize)

	c.ps = &parseState{}

	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}
	ping := []byte("PING\r\n")
	err := c.parse(ping[:1])
	if err != nil || c.ps.state != OP_P {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[1:2])
	if err != nil || c.ps.state != OP_PI {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[2:3])
	if err != nil || c.ps.state != OP_PIN {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[3:4])
	if err != nil || c.ps.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[4:5])
	if err != nil || c.ps.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping[5:6])
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(ping)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	// Should tolerate spaces
	ping = []byte("PING  \r")
	err = c.parse(ping)
	if err != nil || c.ps.state != OP_PING {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	c.ps.state = OP_START
	ping = []byte("PING  \r  \n")
	err = c.parse(ping)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
}

func TestParserErr(t *testing.T) {
	c := &Conn{}
	c.status = CLOSED
	fake := &bytes.Buffer{}
	c.bw = bufio.NewWriterSize(fake, c.Opts.ReconnectBufSize)

	c.ps = &parseState{}

	// This test focuses on the parser only, not how the error is
	// actually processed by the upper layer.

	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}

	expectedError := "'Any kind of error'"
	errProto := []byte("-ERR  " + expectedError + "\r\n")
	err := c.parse(errProto[:1])
	if err != nil || c.ps.state != OP_MINUS {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[1:2])
	if err != nil || c.ps.state != OP_MINUS_E {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[2:3])
	if err != nil || c.ps.state != OP_MINUS_ER {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[3:4])
	if err != nil || c.ps.state != OP_MINUS_ERR {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[4:5])
	if err != nil || c.ps.state != OP_MINUS_ERR_SPC {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[5:6])
	if err != nil || c.ps.state != OP_MINUS_ERR_SPC {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}

	// Check with split arg buffer
	err = c.parse(errProto[6:7])
	if err != nil || c.ps.state != MINUS_ERR_ARG {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[7:10])
	if err != nil || c.ps.state != MINUS_ERR_ARG {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[10 : len(errProto)-2])
	if err != nil || c.ps.state != MINUS_ERR_ARG {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	if c.ps.argBuf == nil {
		t.Fatal("ArgBuf should not be nil")
	}
	s := string(c.ps.argBuf)
	if s != expectedError {
		t.Fatalf("Expected %v, got %v", expectedError, s)
	}
	err = c.parse(errProto[len(errProto)-2:])
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}

	// Check without split arg buffer
	errProto = []byte("-ERR 'Any error'\r\n")
	err = c.parse(errProto)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
}

func TestParserOK(t *testing.T) {
	c := &Conn{}
	c.ps = &parseState{}

	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}
	errProto := []byte("+OKay\r\n")
	err := c.parse(errProto[:1])
	if err != nil || c.ps.state != OP_PLUS {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[1:2])
	if err != nil || c.ps.state != OP_PLUS_O {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[2:3])
	if err != nil || c.ps.state != OP_PLUS_OK {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(errProto[3:])
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
}

func TestParserShouldFail(t *testing.T) {
	c := &Conn{}
	c.ps = &parseState{}

	if err := c.parse([]byte(" PING")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("POO")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("Px")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("PIx")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("PINx")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	// Stop here because 'PING' protos are tolerant for anything between PING and \n

	c.ps.state = OP_START
	if err := c.parse([]byte("POx")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("PONx")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	// Stop here because 'PONG' protos are tolerant for anything between PONG and \n

	c.ps.state = OP_START
	if err := c.parse([]byte("ZOO")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("Mx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSGx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG  foo\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG \r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG foo 1\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG foo bar 1\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG foo bar 1 baz\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("MSG foo 1 bar baz\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("+x\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("+Ox\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("-x\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("-Ex\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("-ERx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
	c.ps.state = OP_START
	if err := c.parse([]byte("-ERRx\r\n")); err == nil {
		t.Fatal("Should have received a parse error")
	}
}

func TestParserSplitMsg(t *testing.T) {
	nc := &Conn{}
	nc.ps = &parseState{}

	buf := []byte("MSG a\r\n")
	err := nc.parse(buf)
	if err == nil {
		t.Fatal("Expected an error")
	}
	nc.ps = &parseState{}

	buf = []byte("MSG a b c\r\n")
	err = nc.parse(buf)
	if err == nil {
		t.Fatal("Expected an error")
	}
	nc.ps = &parseState{}

	expectedCount := uint64(1)
	expectedSize := uint64(3)

	buf = []byte("MSG a")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.argBuf == nil {
		t.Fatal("Arg buffer should have been created")
	}

	buf = []byte(" 1 3\r\nf")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.ma.size != 3 {
		t.Fatalf("Wrong msg size: %d instead of 3", nc.ps.ma.size)
	}
	if nc.ps.ma.sid != 1 {
		t.Fatalf("Wrong sid: %d instead of 1", nc.ps.ma.sid)
	}
	if string(nc.ps.ma.subject) != "a" {
		t.Fatalf("Wrong subject: '%s' instead of 'a'", string(nc.ps.ma.subject))
	}
	if nc.ps.msgBuf == nil {
		t.Fatal("Msg buffer should have been created")
	}

	buf = []byte("oo\r\n")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if (nc.Statistics.InMsgs != expectedCount) || (nc.Statistics.InBytes != expectedSize) {
		t.Fatalf("Wrong stats: %d - %d instead of %d - %d", nc.Statistics.InMsgs, nc.Statistics.InBytes, expectedCount, expectedSize)
	}
	if (nc.ps.argBuf != nil) || (nc.ps.msgBuf != nil) {
		t.Fatal("Buffers should be nil now")
	}

	buf = []byte("MSG a 1 3\r\nfo")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.ma.size != 3 {
		t.Fatalf("Wrong msg size: %d instead of 3", nc.ps.ma.size)
	}
	if nc.ps.ma.sid != 1 {
		t.Fatalf("Wrong sid: %d instead of 1", nc.ps.ma.sid)
	}
	if string(nc.ps.ma.subject) != "a" {
		t.Fatalf("Wrong subject: '%s' instead of 'a'", string(nc.ps.ma.subject))
	}
	if nc.ps.argBuf == nil {
		t.Fatal("Arg buffer should have been created")
	}
	if nc.ps.msgBuf == nil {
		t.Fatal("Msg buffer should have been created")
	}

	expectedCount++
	expectedSize += 3

	buf = []byte("o\r\n")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if (nc.Statistics.InMsgs != expectedCount) || (nc.Statistics.InBytes != expectedSize) {
		t.Fatalf("Wrong stats: %d - %d instead of %d - %d", nc.Statistics.InMsgs, nc.Statistics.InBytes, expectedCount, expectedSize)
	}
	if (nc.ps.argBuf != nil) || (nc.ps.msgBuf != nil) {
		t.Fatal("Buffers should be nil now")
	}

	buf = []byte("MSG a 1 6\r\nfo")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.ma.size != 6 {
		t.Fatalf("Wrong msg size: %d instead of 3", nc.ps.ma.size)
	}
	if nc.ps.ma.sid != 1 {
		t.Fatalf("Wrong sid: %d instead of 1", nc.ps.ma.sid)
	}
	if string(nc.ps.ma.subject) != "a" {
		t.Fatalf("Wrong subject: '%s' instead of 'a'", string(nc.ps.ma.subject))
	}
	if nc.ps.argBuf == nil {
		t.Fatal("Arg buffer should have been created")
	}
	if nc.ps.msgBuf == nil {
		t.Fatal("Msg buffer should have been created")
	}

	buf = []byte("ob")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}

	expectedCount++
	expectedSize += 6

	buf = []byte("ar\r\n")
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if (nc.Statistics.InMsgs != expectedCount) || (nc.Statistics.InBytes != expectedSize) {
		t.Fatalf("Wrong stats: %d - %d instead of %d - %d", nc.Statistics.InMsgs, nc.Statistics.InBytes, expectedCount, expectedSize)
	}
	if (nc.ps.argBuf != nil) || (nc.ps.msgBuf != nil) {
		t.Fatal("Buffers should be nil now")
	}

	// Let's have a msg that is bigger than the parser's scratch size.
	// Since we prepopulate the msg with 'foo', adding 3 to the size.
	msgSize := cap(nc.ps.scratch) + 100 + 3
	buf = []byte(fmt.Sprintf("MSG a 1 b %d\r\nfoo", msgSize))
	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.ma.size != msgSize {
		t.Fatalf("Wrong msg size: %d instead of %d", nc.ps.ma.size, msgSize)
	}
	if nc.ps.ma.sid != 1 {
		t.Fatalf("Wrong sid: %d instead of 1", nc.ps.ma.sid)
	}
	if string(nc.ps.ma.subject) != "a" {
		t.Fatalf("Wrong subject: '%s' instead of 'a'", string(nc.ps.ma.subject))
	}
	if string(nc.ps.ma.reply) != "b" {
		t.Fatalf("Wrong reply: '%s' instead of 'b'", string(nc.ps.ma.reply))
	}
	if nc.ps.argBuf == nil {
		t.Fatal("Arg buffer should have been created")
	}
	if nc.ps.msgBuf == nil {
		t.Fatal("Msg buffer should have been created")
	}

	expectedCount++
	expectedSize += uint64(msgSize)

	bufSize := msgSize - 3

	buf = make([]byte, bufSize)
	for i := 0; i < bufSize; i++ {
		buf[i] = byte('a' + (i % 26))
	}

	err = nc.parse(buf)
	if err != nil {
		t.Fatalf("Parser error: %v", err)
	}
	if nc.ps.state != MSG_PAYLOAD {
		t.Fatalf("Wrong state: %v instead of %v", nc.ps.state, MSG_PAYLOAD)
	}
	if nc.ps.ma.size != msgSize {
		t.Fatalf("Wrong (ma) msg size: %d instead of %d", nc.ps.ma.size, msgSize)
	}
	if len(nc.ps.msgBuf) != msgSize {
		t.Fatalf("Wrong msg size: %d instead of %d", len(nc.ps.msgBuf), msgSize)
	}
	// Check content:
	if string(nc.ps.msgBuf[0:3]) != "foo" {
		t.Fatalf("Wrong msg content: %s", string(nc.ps.msgBuf))
	}
	for k := 3; k < nc.ps.ma.size; k++ {
		if nc.ps.msgBuf[k] != byte('a'+((k-3)%26)) {
			t.Fatalf("Wrong msg content: %s", string(nc.ps.msgBuf))
		}
	}

	buf = []byte("\r\n")
	if err := nc.parse(buf); err != nil {
		t.Fatalf("Unexpected error during parsing: %v", err)
	}
	if (nc.Statistics.InMsgs != expectedCount) || (nc.Statistics.InBytes != expectedSize) {
		t.Fatalf("Wrong stats: %d - %d instead of %d - %d", nc.Statistics.InMsgs, nc.Statistics.InBytes, expectedCount, expectedSize)
	}
	if (nc.ps.argBuf != nil) || (nc.ps.msgBuf != nil) {
		t.Fatal("Buffers should be nil now")
	}
	if nc.ps.state != OP_START {
		t.Fatalf("Wrong state: %v", nc.ps.state)
	}
}

func TestNormalizeError(t *testing.T) {
	expected := "Typical Error"
	if s := normalizeErr("-ERR '" + expected + "'"); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	expected = "Trim Surrounding Spaces"
	if s := normalizeErr("-ERR    '" + expected + "'   "); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	expected = "Trim Surrounding Spaces Without Quotes"
	if s := normalizeErr("-ERR    " + expected + "   "); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	expected = "Error Without Quotes"
	if s := normalizeErr("-ERR " + expected); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	expected = "Error With Quote Only On Left"
	if s := normalizeErr("-ERR '" + expected); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}

	expected = "Error With Quote Only On Right"
	if s := normalizeErr("-ERR " + expected + "'"); s != expected {
		t.Fatalf("Expected '%s', got '%s'", expected, s)
	}
}

func TestAsyncINFO(t *testing.T) {
	opts := GetDefaultOptions()
	c := &Conn{Opts: opts}

	c.ps = &parseState{}

	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}

	info := []byte("INFO {}\r\n")
	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}
	err := c.parse(info[:1])
	if err != nil || c.ps.state != OP_I {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(info[1:2])
	if err != nil || c.ps.state != OP_IN {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(info[2:3])
	if err != nil || c.ps.state != OP_INF {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(info[3:4])
	if err != nil || c.ps.state != OP_INFO {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(info[4:5])
	if err != nil || c.ps.state != OP_INFO_SPC {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	err = c.parse(info[5:])
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}

	// All at once
	err = c.parse(info)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}

	// Server pool needs to be setup
	c.setupServerPool()

	// Partials requiring argBuf
	expectedServer := serverInfo{
		ID:           "test",
		Host:         "localhost",
		Port:         4222,
		AuthRequired: true,
		TLSRequired:  true,
		MaxPayload:   2 * 1024 * 1024,
		ConnectURLs:  []string{"localhost:5222", "localhost:6222"},
	}
	// Set NoRandomize so that the check with expectedServer info
	// matches.
	c.Opts.NoRandomize = true

	b, _ := json.Marshal(expectedServer)
	info = []byte(fmt.Sprintf("INFO %s\r\n", b))
	if c.ps.state != OP_START {
		t.Fatalf("Expected OP_START vs %d\n", c.ps.state)
	}
	err = c.parse(info[:9])
	if err != nil || c.ps.state != INFO_ARG || c.ps.argBuf == nil {
		t.Fatalf("Unexpected: %d err: %v argBuf: %v\n", c.ps.state, err, c.ps.argBuf)
	}
	err = c.parse(info[9:11])
	if err != nil || c.ps.state != INFO_ARG || c.ps.argBuf == nil {
		t.Fatalf("Unexpected: %d err: %v argBuf: %v\n", c.ps.state, err, c.ps.argBuf)
	}
	err = c.parse(info[11:])
	if err != nil || c.ps.state != OP_START || c.ps.argBuf != nil {
		t.Fatalf("Unexpected: %d err: %v argBuf: %v\n", c.ps.state, err, c.ps.argBuf)
	}
	if !reflect.DeepEqual(c.info, expectedServer) {
		t.Fatalf("Expected server info to be: %v, got: %v", expectedServer, c.info)
	}

	// Good INFOs
	good := []string{"INFO {}\r\n", "INFO  {}\r\n", "INFO {} \r\n", "INFO { \"server_id\": \"test\"  }   \r\n", "INFO {\"connect_urls\":[]}\r\n"}
	for _, gi := range good {
		c.ps = &parseState{}
		err = c.parse([]byte(gi))
		if err != nil || c.ps.state != OP_START {
			t.Fatalf("Protocol %q should be fine. Err=%v state=%v", gi, err, c.ps.state)
		}
	}

	// Wrong INFOs
	wrong := []string{"IxNFO {}\r\n", "INxFO {}\r\n", "INFxO {}\r\n", "INFOx {}\r\n", "INFO{}\r\n", "INFO {}"}
	for _, wi := range wrong {
		c.ps = &parseState{}
		err = c.parse([]byte(wi))
		if err == nil && c.ps.state == OP_START {
			t.Fatalf("Protocol %q should have failed", wi)
		}
	}

	checkPool := func(urls ...string) {
		// Check both pool and urls map
		if len(c.srvPool) != len(urls) {
			stackFatalf(t, "Pool should have %d elements, has %d", len(urls), len(c.srvPool))
		}
		if len(c.urls) != len(urls) {
			stackFatalf(t, "Map should have %d elements, has %d", len(urls), len(c.urls))
		}
		for _, url := range urls {
			if _, present := c.urls[url]; !present {
				stackFatalf(t, "Pool should have %q", url)
			}
		}
	}

	// Now test the decoding of "connect_urls"

	// Reset the pool
	c.setupServerPool()
	// Reinitialize the parser
	c.ps = &parseState{}

	info = []byte("INFO {\"connect_urls\":[\"localhost:4222\", \"localhost:5222\"]}\r\n")
	err = c.parse(info)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	// Pool now should contain 127.0.0.1:4222 (the default URL), localhost:4222 and localhost:5222
	checkPool("127.0.0.1:4222", "localhost:4222", "localhost:5222")

	// Make sure that if client receives the same, it is not added again.
	err = c.parse(info)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	// Pool should still contain 127.0.0.1:4222 (the default URL), localhost:4222 and localhost:5222
	checkPool("127.0.0.1:4222", "localhost:4222", "localhost:5222")

	// Receive a new URL
	info = []byte("INFO {\"connect_urls\":[\"localhost:4222\", \"localhost:5222\", \"localhost:6222\"]}\r\n")
	err = c.parse(info)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	// Pool now should contain 127.0.0.1:4222 (the default URL), localhost:4222, localhost:5222 and localhost:6222
	checkPool("127.0.0.1:4222", "localhost:4222", "localhost:5222", "localhost:6222")

	// Check that pool may be randomized on setup, but new URLs are always
	// added at end of pool.
	c.Opts.NoRandomize = false
	c.Opts.Servers = testServers
	// Reset the pool
	c.setupServerPool()
	// Reinitialize the parser
	c.ps = &parseState{}
	// Capture the pool sequence after randomization
	urlsAfterPoolSetup := make([]string, 0, len(c.srvPool))
	for _, srv := range c.srvPool {
		urlsAfterPoolSetup = append(urlsAfterPoolSetup, srv.url.Host)
	}
	checkNewURLsAddedRandomly := func() {
		t.Helper()
		var ok bool
		for i := 0; i < len(urlsAfterPoolSetup); i++ {
			if c.srvPool[i].url.Host != urlsAfterPoolSetup[i] {
				ok = true
				break
			}
		}
		if !ok {
			t.Fatalf("New URLs were not added randmonly: %q", c.Servers())
		}
	}
	// Add new urls
	newURLs := "\"impA:4222\", \"impB:4222\", \"impC:4222\", " +
		"\"impD:4222\", \"impE:4222\", \"impF:4222\", \"impG:4222\", " +
		"\"impH:4222\", \"impI:4222\", \"impJ:4222\""
	info = []byte("INFO {\"connect_urls\":[" + newURLs + "]}\r\n")
	err = c.parse(info)
	if err != nil || c.ps.state != OP_START {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	checkNewURLsAddedRandomly()
	// Check that we have not moved the first URL
	if u := c.srvPool[0].url.Host; u != urlsAfterPoolSetup[0] {
		t.Fatalf("Expected first URL to be %q, got %q", urlsAfterPoolSetup[0], u)
	}
}

func TestConnServers(t *testing.T) {
	opts := GetDefaultOptions()
	c := &Conn{Opts: opts}
	c.ps = &parseState{}
	c.setupServerPool()

	validateURLs := func(serverUrls []string, expectedUrls ...string) {
		var found bool
		if len(serverUrls) != len(expectedUrls) {
			stackFatalf(t, "Array should have %d elements, has %d", len(expectedUrls), len(serverUrls))
		}

		for _, ev := range expectedUrls {
			found = false
			for _, av := range serverUrls {
				if ev == av {
					found = true
					break
				}
			}
			if !found {
				stackFatalf(t, "array is missing %q in %v", ev, serverUrls)
			}
		}
	}

	// check the default url
	validateURLs(c.Servers(), "nats://127.0.0.1:4222")
	if len(c.DiscoveredServers()) != 0 {
		t.Fatalf("Expected no discovered servers")
	}

	// Add a new URL
	err := c.parse([]byte("INFO {\"connect_urls\":[\"localhost:5222\"]}\r\n"))
	if err != nil {
		t.Fatalf("Unexpected: %d : %v\n", c.ps.state, err)
	}
	// Server list should now contain both the default and the new url.
	validateURLs(c.Servers(), "nats://127.0.0.1:4222", "nats://localhost:5222")
	// Discovered servers should only contain the new url.
	validateURLs(c.DiscoveredServers(), "nats://localhost:5222")

	// verify user credentials are stripped out.
	opts.Servers = []string{"nats://user:pass@localhost:4333", "nats://token@localhost:4444"}
	c = &Conn{Opts: opts}
	c.ps = &parseState{}
	c.setupServerPool()

	validateURLs(c.Servers(), "nats://localhost:4333", "nats://localhost:4444")
}

func TestNoEchoOldServer(t *testing.T) {
	opts := GetDefaultOptions()
	opts.Url = DefaultURL
	opts.NoEcho = true

	nc := &Conn{Opts: opts}
	if err := nc.setupServerPool(); err != nil {
		t.Fatalf("Problem setting up Server Pool: %v\n", err)
	}

	// Old style with no proto, meaning 0. We need Proto:1 for NoEcho support.
	oldInfo := "{\"server_id\":\"22\",\"version\":\"1.1.0\",\"go\":\"go1.10.2\",\"port\":4222,\"max_payload\":1048576}"

	err := nc.processInfo(oldInfo)
	if err != nil {
		t.Fatalf("Error processing old style INFO: %v\n", err)
	}

	// Make sure connectProto generates an error.
	_, err = nc.connectProto()
	if err == nil {
		t.Fatalf("Expected an error but got none\n")
	}
}

func TestNoPanicOnSrvPoolSizeChanging(t *testing.T) {
	listeners := []net.Listener{}
	ports := []int{}

	for i := 0; i < 3; i++ {
		l, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("Could not listen on an ephemeral port: %v", err)
		}
		defer l.Close()
		tl := l.(*net.TCPListener)
		ports = append(ports, tl.Addr().(*net.TCPAddr).Port)
		listeners = append(listeners, l)
	}

	wg := sync.WaitGroup{}
	wg.Add(len(listeners))

	connect := int32(0)
	srv := func(l net.Listener) {
		defer wg.Done()
		for {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			defer conn.Close()

			var info string

			reject := atomic.AddInt32(&connect, 1) <= 2
			if reject {
				// Sends a list of 3 servers, where the second does not actually run.
				// This server is going to reject the connect (with auth error), so
				// client will move to 2nd, fail, then go to third...
				info = fmt.Sprintf("INFO {\"server_id\":\"foobar\",\"connect_urls\":[\"127.0.0.1:%d\",\"127.0.0.1:%d\",\"127.0.0.1:%d\"]}\r\n",
					ports[0], ports[1], ports[2])
			} else {
				// This third server will return the INFO with only the original server
				// and the third one, which will make the srvPool size shrink down to 2.
				info = fmt.Sprintf("INFO {\"server_id\":\"foobar\",\"connect_urls\":[\"127.0.0.1:%d\",\"127.0.0.1:%d\"]}\r\n",
					ports[0], ports[2])
			}
			conn.Write([]byte(info))

			// Read connect and ping commands sent from the client
			br := bufio.NewReaderSize(conn, 10*1024)
			br.ReadLine()
			br.ReadLine()

			if reject {
				conn.Write([]byte(fmt.Sprintf("-ERR '%s'\r\n", AUTHORIZATION_ERR)))
				conn.Close()
			} else {
				conn.Write([]byte(pongProto))
				br.ReadLine()
			}
		}
	}

	for _, l := range listeners {
		go srv(l)
	}

	time.Sleep(250 * time.Millisecond)

	nc, err := Connect(fmt.Sprintf("nats://127.0.0.1:%d", ports[0]))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	nc.Close()
	for _, l := range listeners {
		l.Close()
	}
	wg.Wait()
}

func TestHeaderParser(t *testing.T) {
	shouldErr := func(hdr string) {
		t.Helper()
		if _, err := decodeHeadersMsg([]byte(hdr)); err == nil {
			t.Fatalf("Expected an error")
		}
	}
	shouldErr("NATS/1.0")
	shouldErr("NATS/1.0\r\n")
	shouldErr("NATS/1.0\r\nk1:v1")
	shouldErr("NATS/1.0\r\nk1:v1\r\n")

	// Check that we can do inline status and descriptions
	checkStatus := func(hdr string, status int, description string) {
		t.Helper()
		hdrs, err := decodeHeadersMsg([]byte(hdr + "\r\n\r\n"))
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		if code, err := strconv.Atoi(hdrs.Get(statusHdr)); err != nil || code != status {
			t.Fatalf("Expected status of %d, got %s", status, hdrs.Get(statusHdr))
		}
		if len(description) > 0 {
			if descr := hdrs.Get(descrHdr); err != nil || descr != description {
				t.Fatalf("Expected description of %q, got %q", description, descr)
			}
		}
	}

	checkStatus("NATS/1.0 503", 503, "")
	checkStatus("NATS/1.0 503 No Responders", 503, "No Responders")
	checkStatus("NATS/1.0  404   No Messages", 404, "No Messages")
}

func TestLameDuckMode(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Could not listen on an ephemeral port: %v", err)
	}
	tl := l.(*net.TCPListener)
	defer tl.Close()

	addr := tl.Addr().(*net.TCPAddr)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		ldmInfos := []string{"INFO {\"ldm\":true}\r\n", "INFO {\"connect_urls\":[\"127.0.0.1:1234\"],\"ldm\":true}\r\n"}
		for _, ldmInfo := range ldmInfos {
			conn, err := l.Accept()
			if err != nil {
				return
			}
			defer conn.Close()

			info := "INFO {\"server_id\":\"foobar\"}\r\n"
			conn.Write([]byte(info))

			// Read connect and ping commands sent from the client
			br := bufio.NewReaderSize(conn, 10*1024)
			br.ReadLine()
			br.ReadLine()
			conn.Write([]byte(pongProto))

			// Wait a bit and then send a INFO with LDM
			time.Sleep(100 * time.Millisecond)
			conn.Write([]byte(ldmInfo))
			br.ReadLine()
			conn.Close()
		}
	}()

	url := fmt.Sprintf("nats://127.0.0.1:%d", addr.Port)
	time.Sleep(100 * time.Millisecond)

	for _, test := range []struct {
		name  string
		curls bool
	}{
		{"without connect urls", false},
		{"with connect urls", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			ch := make(chan bool, 1)
			errCh := make(chan error, 1)
			nc, err := Connect(url,
				DiscoveredServersHandler(func(nc *Conn) {
					ds := nc.DiscoveredServers()
					if !reflect.DeepEqual(ds, []string{"nats://127.0.0.1:1234"}) {
						errCh <- fmt.Errorf("wrong discovered servers: %q", ds)
					} else {
						errCh <- nil
					}
				}),
				LameDuckModeHandler(func(_ *Conn) {
					ch <- true
				}),
			)
			if err != nil {
				t.Fatalf("Expected to connect, got %v", err)
			}
			defer nc.Close()

			select {
			case <-ch:
			case <-time.After(2 * time.Second):
				t.Fatal("should have been notified of LDM")
			}
			select {
			case e := <-errCh:
				if !test.curls {
					t.Fatal("should not have received connect urls")
				} else if e != nil {
					t.Fatal(e.Error())
				}
			default:
				if test.curls {
					t.Fatal("should have received notification about discovered servers")
				}
			}

			nc.Close()
		})
	}
	wg.Wait()
}
