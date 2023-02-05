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
	"errors"
	"fmt"
	"testing"
	"time"
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

func TestKeyValueNonDirectGet(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kvi, err := js.CreateKeyValue(&KeyValueConfig{Bucket: "TEST"})
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	si, err := js.StreamInfo("KV_TEST")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}
	if !si.Config.AllowDirect {
		t.Fatal("Expected allow direct to be set, it was not")
	}

	kv := kvi.(*kvs)
	if !kv.useDirect {
		t.Fatal("useDirect should have been true, it was not")
	}
	kv.useDirect = false

	if _, err := kv.PutString("key1", "val1"); err != nil {
		t.Fatalf("Error putting key: %v", err)
	}
	if _, err := kv.PutString("key2", "val2"); err != nil {
		t.Fatalf("Error putting key: %v", err)
	}
	if v, err := kv.Get("key2"); err != nil || string(v.Value()) != "val2" {
		t.Fatalf("Error on get: v=%+v err=%v", v, err)
	}
	if v, err := kv.GetRevision("key1", 1); err != nil || string(v.Value()) != "val1" {
		t.Fatalf("Error on get revisiong: v=%+v err=%v", v, err)
	}
	if v, err := kv.GetRevision("key1", 2); err == nil {
		t.Fatalf("Expected error, got %+v", v)
	}
}

func TestKeyValueRePublish(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	if _, err := js.CreateKeyValue(&KeyValueConfig{
		Bucket: "TEST_UPDATE",
	}); err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	// This is expected to fail since server does not support as of now
	// the update of RePublish.
	if _, err := js.CreateKeyValue(&KeyValueConfig{
		Bucket:    "TEST_UPDATE",
		RePublish: &RePublish{Source: ">", Destination: "bar.>"},
	}); err == nil {
		t.Fatal("Expected failure, did not get one")
	}

	kv, err := js.CreateKeyValue(&KeyValueConfig{
		Bucket:    "TEST",
		RePublish: &RePublish{Source: ">", Destination: "bar.>"},
	})
	if err != nil {
		t.Fatalf("Error creating store: %v", err)
	}
	si, err := js.StreamInfo("KV_TEST")
	if err != nil {
		t.Fatalf("Error getting stream info: %v", err)
	}
	if si.Config.RePublish == nil {
		t.Fatal("Expected republish to be set, it was not")
	}

	sub, err := nc.SubscribeSync("bar.>")
	if err != nil {
		t.Fatalf("Error on sub: %v", err)
	}
	if _, err := kv.Put("foo", []byte("value")); err != nil {
		t.Fatalf("Error on put: %v", err)
	}
	msg, err := sub.NextMsg(time.Second)
	if err != nil {
		t.Fatalf("Error on next: %v", err)
	}
	if v := string(msg.Data); v != "value" {
		t.Fatalf("Unexpected value: %s", v)
	}
	// The message should also have a header with the actual subject
	expected := fmt.Sprintf(kvSubjectsPreTmpl, "TEST") + "foo"
	if v := msg.Header.Get(JSSubject); v != expected {
		t.Fatalf("Expected subject header %q, got %q", expected, v)
	}
}

func TestKeyValueMirrorDirectGet(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&KeyValueConfig{Bucket: "TEST"})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}
	_, err = js.AddStream(&StreamConfig{
		Name:         "MIRROR",
		Mirror:       &StreamSource{Name: "KV_TEST"},
		MirrorDirect: true,
	})
	if err != nil {
		t.Fatalf("Error creating mirror: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("KEY.%d", i)
		if _, err := kv.PutString(key, "42"); err != nil {
			t.Fatalf("Error adding key: %v", err)
		}
	}

	// Make sure all gets work.
	for i := 0; i < 100; i++ {
		if _, err := kv.Get("KEY.22"); err != nil {
			t.Fatalf("Got error getting key: %v", err)
		}
	}
}

func TestKeyValueCreate(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&KeyValueConfig{Bucket: "TEST"})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	_, err = kv.Create("key", []byte("1"))
	if err != nil {
		t.Fatalf("Error creating key: %v", err)
	}

	_, err = kv.Create("key", []byte("1"))
	expected := "nats: wrong last sequence: 1: key exists"
	if err.Error() != expected {
		t.Fatalf("Expected %q, got: %v", expected, err)
	}
	if !errors.Is(err, ErrKeyExists) {
		t.Fatalf("Expected ErrKeyExists, got: %v", err)
	}
	aerr := &APIError{}
	if !errors.As(err, &aerr) {
		t.Fatalf("Expected APIError, got: %v", err)
	}
	if aerr.Description != "wrong last sequence: 1" {
		t.Fatalf("Unexpected APIError message, got: %v", aerr.Description)
	}
	if aerr.ErrorCode != 10071 {
		t.Fatalf("Unexpected error code, got: %v", aerr.ErrorCode)
	}
	if aerr.Code != ErrKeyExists.APIError().Code {
		t.Fatalf("Unexpected error code, got: %v", aerr.Code)
	}
	var kerr JetStreamError
	if !errors.As(err, &kerr) {
		t.Fatalf("Expected KeyValueError, got: %v", err)
	}
	if kerr.APIError().ErrorCode != 10071 {
		t.Fatalf("Unexpected error code, got: %v", kerr.APIError().ErrorCode)
	}
}

func TestKeyValueSourcing(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kvA, err := js.CreateKeyValue(&KeyValueConfig{Bucket: "A"})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	_, err = kvA.Create("keyA", []byte("1"))
	if err != nil {
		t.Fatalf("Error creating key: %v", err)
	}

	if _, err := kvA.Get("keyA"); err != nil {
		t.Fatalf("Got error getting keyA from A: %v", err)
	}

	kvB, err := js.CreateKeyValue(&KeyValueConfig{Bucket: "B"})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	_, err = kvB.Create("keyB", []byte("1"))
	if err != nil {
		t.Fatalf("Error creating key: %v", err)
	}

	kvC, err := js.CreateKeyValue(&KeyValueConfig{Bucket: "C", Sources: []*StreamSource{{Name: "A"}, {Name: "B"}}})
	if err != nil {
		t.Fatalf("Error creating kv: %v", err)
	}

	// Wait half a second to make sure it has time to populate the stream from it's sources
	i := 0
	for {
		status, err := kvC.Status()
		if err != nil {
			t.Fatalf("Error getting bucket status: %v", err)
		}
		if status.Values() == 2 {
			break
		} else {
			i++
			if i > 3 {
				t.Fatalf("Error sourcing bucket does not contain the expected number of values")
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	if _, err := kvC.Get("keyA"); err != nil {
		t.Fatalf("Got error getting keyA from C: %v", err)
	}

	if _, err := kvC.Get("keyB"); err != nil {
		t.Fatalf("Got error getting keyB from C: %v", err)
	}
}
