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
	"fmt"
	"math"
	"testing"
)

// On 32-bit platforms int cannot hold every num_deleted value the server can
// report (e.g. for a stream with MaxMsgsPerSubject=1 and heavy churn). Such
// values must be capped at math.MaxInt instead of failing to decode.
func TestStreamStateNumDeletedOverflow(t *testing.T) {
	tests := []struct {
		name       string
		numDeleted int64
	}{
		{name: "zero", numDeleted: 0},
		{name: "max int32", numDeleted: math.MaxInt32},
		{name: "above int32", numDeleted: 1 << 32},
		{name: "max int64", numDeleted: math.MaxInt64},
		{name: "negative", numDeleted: -1},
		{name: "below int32", numDeleted: -(1 << 32)},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data := fmt.Appendf(nil, `{"messages":5,"last_seq":10,"consumer_count":3,"num_deleted":%d,"deleted":[2,4]}`, test.numDeleted)

			var state StreamState
			if err := json.Unmarshal(data, &state); err != nil {
				t.Fatalf("Unexpected error: %v", err)
			}

			want := test.numDeleted
			if want > math.MaxInt {
				want = math.MaxInt
			} else if want < math.MinInt {
				want = math.MinInt
			}
			if int64(state.NumDeleted) != want {
				t.Fatalf("Invalid NumDeleted; want: %d; got: %d", want, state.NumDeleted)
			}
			if state.Msgs != 5 || state.LastSeq != 10 || state.Consumers != 3 || len(state.Deleted) != 2 {
				t.Fatalf("Other fields not decoded correctly: %+v", state)
			}
		})
	}

	// A response without num_deleted leaves the field untouched, as for any
	// other field.
	state := StreamState{NumDeleted: 7}
	if err := json.Unmarshal([]byte(`{"messages":1}`), &state); err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	if state.NumDeleted != 7 || state.Msgs != 1 {
		t.Fatalf("Unexpected state: %+v", state)
	}
}
