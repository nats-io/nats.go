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
	"os"
	"testing"

	"github.com/synadia-io/orbit.go/ntf"
)

// Keep in sync with the copies in jetstream/test and micro/test.
func TestMain(m *testing.M) {
	testerURL = os.Getenv("TESTER_NATS_URL")
	if testerURL == "" {
		// CI must use the pinned synadia/ntf-server image, which ships its own
		// nats-server build.
		if os.Getenv("GITHUB_ACTIONS") != "" {
			fmt.Fprintln(os.Stderr, "TESTER_NATS_URL must be set in CI; refusing to fall back to the in-process tester")
			os.Exit(1)
		}
		// ntf's embedded server binds the wildcard address, so ClientURL reports
		// 0.0.0.0 — neither reachable nor a SAN on the certs it generates. Pin
		// both this URL and what managed servers advertise to localhost.
		svc, err := ntf.New(context.Background(), ntf.Options{AdvertiseHost: "localhost"})
		if err != nil {
			fmt.Fprintln(os.Stderr, "could not start the in-process tester:", err)
			os.Exit(1)
		}
		defer svc.Close()
		testerURL = fmt.Sprintf("nats://localhost:%d", svc.Port())
	}
	m.Run()
}
