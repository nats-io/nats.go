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

package jetstream

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestStreamInfoUnmarshalServerFields(t *testing.T) {
	// Shaped after nats-server's StreamInfo JSON.
	data := []byte(`{
		"config": {"name": "foo"},
		"state": {
			"messages": 10,
			"lost": {"msgs": [3, 4], "bytes": 128}
		},
		"mirror": {
			"name": "origin",
			"external": {"api": "$JS.hub.API", "deliver": "deliver.hub"},
			"lag": 0,
			"active": -1,
			"error": {"code": 500, "err_code": 10059, "description": "stream not found"}
		},
		"alternates": [
			{"name": "foo", "cluster": "C1"},
			{"name": "foo_mirror", "domain": "hub", "cluster": "C2"}
		]
	}`)
	var info StreamInfo
	if err := json.Unmarshal(data, &info); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}

	expectedLost := &LostStreamData{Msgs: []uint64{3, 4}, Bytes: 128}
	if !reflect.DeepEqual(info.State.Lost, expectedLost) {
		t.Fatalf("Invalid lost data; want: %+v; got: %+v", expectedLost, info.State.Lost)
	}

	if info.Mirror == nil {
		t.Fatal("Expected mirror info")
	}
	expectedExternal := &ExternalStream{APIPrefix: "$JS.hub.API", DeliverPrefix: "deliver.hub"}
	if !reflect.DeepEqual(info.Mirror.External, expectedExternal) {
		t.Fatalf("Invalid external; want: %+v; got: %+v", expectedExternal, info.Mirror.External)
	}
	expectedErr := &APIError{Code: 500, ErrorCode: JSErrCodeStreamNotFound, Description: "stream not found"}
	if !reflect.DeepEqual(info.Mirror.Error, expectedErr) {
		t.Fatalf("Invalid error; want: %+v; got: %+v", expectedErr, info.Mirror.Error)
	}
	if info.Mirror.Active != -1 {
		t.Fatalf("Invalid active; want: -1; got: %v", info.Mirror.Active)
	}

	expectedAlts := []StreamAlternate{
		{Name: "foo", Cluster: "C1"},
		{Name: "foo_mirror", Domain: "hub", Cluster: "C2"},
	}
	if !reflect.DeepEqual(info.Alternates, expectedAlts) {
		t.Fatalf("Invalid alternates; want: %+v; got: %+v", expectedAlts, info.Alternates)
	}

	// Fields are omitted when the server does not send them.
	var empty StreamInfo
	if err := json.Unmarshal([]byte(`{"config":{"name":"foo"},"state":{},"mirror":{"name":"origin"}}`), &empty); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if empty.State.Lost != nil || empty.Alternates != nil || empty.Mirror.External != nil || empty.Mirror.Error != nil {
		t.Fatalf("Expected unset fields to be nil; got %+v", empty)
	}
}
