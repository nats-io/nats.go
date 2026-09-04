// Copyright 2026 The NATS Authors
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
	"context"
	"fmt"
	"net"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/synadia-io/orbit.go/ntf"
)

func TestMain(m *testing.M) {
	testerURL = os.Getenv("TESTER_NATS_URL")
	if testerURL == "" {
		// CI must use the pinned synadia/ntf-server image, which ships its own
		// nats-server build. Falling back silently would change what CI tests
		// against without anyone noticing.
		if os.Getenv("CI") != "" {
			fmt.Fprintln(os.Stderr, "TESTER_NATS_URL must be set in CI; refusing to fall back to the in-process tester")
			os.Exit(1)
		}
		svc, err := startInProcessTester()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		defer svc.Close()
	}
	m.Run()
}

// startInProcessTester runs the tester in this binary, so `go test` needs no
// docker. The embedded server binds the wildcard address, so ClientURL reports
// 0.0.0.0; the ntf client derives every managed server URL from that host, and
// 0.0.0.0 is neither what the servers advertise nor a SAN on the generated certs.
func startInProcessTester() (*ntf.Service, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	svc, err := ntf.New(ctx, ntf.Options{AdvertiseHost: "localhost"})
	if err != nil {
		return nil, fmt.Errorf("could not start the in-process tester: %w", err)
	}
	u, err := url.Parse(svc.ClientURL())
	if err != nil {
		svc.Close()
		return nil, fmt.Errorf("could not parse the in-process tester URL %q: %w", svc.ClientURL(), err)
	}
	testerURL = "nats://" + net.JoinHostPort("localhost", u.Port())
	return svc, nil
}
