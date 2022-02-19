// Copyright 2022 The NATS Authors
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

import (
	"testing"
)

func TestKeyValueDiscardOldToDiscardNew(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	checkDiscard := func(expected DiscardPolicy) KeyValue {
		t.Helper()
		kv, err := js.CreateKeyValue(&KeyValueConfig{Bucket: "TEST", History: 1})
		if err != nil {
			t.Fatalf("Error creating store: %v", err)
		}
		si, err := js.StreamInfo("KV_TEST")
		if err != nil {
			t.Fatalf("Error getting stream info: %v", err)
		}
		if si.Config.Discard != expected {
			t.Fatalf("Expected discard policy %v, got %+v", expected, si)
		}
		return kv
	}

	// We are going to go from 2.7.1->2.7.2->2.7.1 and 2.7.2 again.
	for i := 0; i < 2; i++ {
		// Change the server version in the connection to
		// create as-if we were connecting to a v2.7.1 server.
		nc.mu.Lock()
		nc.info.Version = "2.7.1"
		nc.mu.Unlock()

		kv := checkDiscard(DiscardOld)
		if i == 0 {
			if _, err := kv.PutString("foo", "value"); err != nil {
				t.Fatalf("Error adding key: %v", err)
			}
		}

		// Now change version to 2.7.2
		nc.mu.Lock()
		nc.info.Version = "2.7.2"
		nc.mu.Unlock()

		kv = checkDiscard(DiscardNew)
		// Make sure the key still exists
		if e, err := kv.Get("foo"); err != nil || string(e.Value()) != "value" {
			t.Fatalf("Error getting key: err=%v e=%+v", err, e)
		}
	}
}
