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
	"testing"
)

// NumDeleted and Consumers must hold values beyond the 32-bit int range,
// otherwise decoding StreamInfo fails on 32-bit platforms, e.g. for streams
// with many interior deletes (MaxMsgsPerSubject=1 with heavy churn).
func TestStreamStateLargeCounters(t *testing.T) {
	const large = uint64(1) << 32
	data := []byte(`{"messages":1,"first_seq":1,"last_seq":4294967298,"num_deleted":4294967296,"consumer_count":4294967296}`)

	var state StreamState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if state.NumDeleted != large {
		t.Fatalf("Invalid NumDeleted; want: %d; got: %d", large, state.NumDeleted)
	}
	if state.Consumers != large {
		t.Fatalf("Invalid Consumers; want: %d; got: %d", large, state.Consumers)
	}
}
