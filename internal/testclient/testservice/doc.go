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

// Package testservice is a vendored client for the synadia-labs/testing.go
// server-tester service. Tests use it to create real nats-server instances
// (servers, clusters, super-clusters) on demand via a NATS micro service.
//
// Source of truth: https://github.com/synadia-labs/testing.go
//
// To refresh:
//  1. Pull the latest client.go and api.go from synadia-labs/testing.go.
//  2. Rewrite the api import path to github.com/nats-io/nats.go/internal/testclient/api.
//  3. Re-run the testservice CI job to confirm wire compatibility.
package testservice
