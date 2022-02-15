// Copyright 2021-2022 The NATS Authors
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
	"reflect"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
)

func TestKeyValueBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST", History: 5, TTL: time.Hour})
	expectOk(t, err)

	if kv.Bucket() != "TEST" {
		t.Fatalf("Expected bucket name to be %q, got %q", "TEST", kv.Bucket())
	}

	// Simple Put
	r, err := kv.Put("name", []byte("derek"))
	expectOk(t, err)
	if r != 1 {
		t.Fatalf("Expected 1 for the revision, got %d", r)
	}
	// Simple Get
	e, err := kv.Get("name")
	expectOk(t, err)
	if string(e.Value()) != "derek" {
		t.Fatalf("Got wrong value: %q vs %q", e.Value(), "derek")
	}
	if e.Revision() != 1 {
		t.Fatalf("Expected 1 for the revision, got %d", e.Revision())
	}

	// Delete
	err = kv.Delete("name")
	expectOk(t, err)
	_, err = kv.Get("name")
	expectErr(t, err, nats.ErrKeyNotFound)
	r, err = kv.Create("name", []byte("derek"))
	expectOk(t, err)
	if r != 3 {
		t.Fatalf("Expected 3 for the revision, got %d", r)
	}

	// Conditional Updates.
	r, err = kv.Update("name", []byte("rip"), 3)
	expectOk(t, err)
	_, err = kv.Update("name", []byte("ik"), 3)
	expectErr(t, err)
	_, err = kv.Update("name", []byte("ik"), r)
	expectOk(t, err)
	r, err = kv.Create("age", []byte("22"))
	expectOk(t, err)
	_, err = kv.Update("age", []byte("33"), r)
	expectOk(t, err)

	// Status
	status, err := kv.Status()
	expectOk(t, err)
	if status.History() != 5 {
		t.Fatalf("expected history of 5 got %d", status.History())
	}
	if status.Bucket() != "TEST" {
		t.Fatalf("expected bucket TEST got %v", status.Bucket())
	}
	if status.TTL() != time.Hour {
		t.Fatalf("expected 1 hour TTL got %v", status.TTL())
	}
	if status.Values() != 7 {
		t.Fatalf("expected 7 values got %d", status.Values())
	}
	if status.BackingStore() != "JetStream" {
		t.Fatalf("invalid backing store kind %s", status.BackingStore())
	}

	kvs := status.(*nats.KeyValueBucketStatus)
	si := kvs.StreamInfo()
	if si == nil {
		t.Fatalf("StreamInfo not received")
	}
}

func TestKeyValueHistory(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "LIST", History: 10})
	expectOk(t, err)

	for i := 0; i < 50; i++ {
		age := strconv.FormatUint(uint64(i+22), 10)
		_, err := kv.Put("age", []byte(age))
		expectOk(t, err)
	}

	vl, err := kv.History("age")
	expectOk(t, err)

	if len(vl) != 10 {
		t.Fatalf("Expected %d values, got %d", 10, len(vl))
	}
	for i, v := range vl {
		if v.Key() != "age" {
			t.Fatalf("Expected key of %q, got %q", "age", v.Key())
		}
		if v.Revision() != uint64(i+41) {
			// History of 10, sent 50..
			t.Fatalf("Expected revision of %d, got %d", i+41, v.Revision())
		}
		age, err := strconv.Atoi(string(v.Value()))
		expectOk(t, err)
		if age != i+62 {
			t.Fatalf("Expected data value of %d, got %d", i+22, age)
		}
	}
}

func TestKeyValueWatch(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	watcher, err := kv.WatchAll()
	expectOk(t, err)
	defer watcher.Stop()

	expectUpdate := func(key, value string, revision uint64) {
		t.Helper()
		select {
		case v := <-watcher.Updates():
			if v.Key() != key || string(v.Value()) != value || v.Revision() != revision {
				t.Fatalf("Did not get expected: %+v vs %q %q %d", v, key, value, revision)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an update like expected")
		}
	}
	expectDelete := func(key string, revision uint64) {
		t.Helper()
		select {
		case v := <-watcher.Updates():
			if v.Operation() != nats.KeyValueDelete {
				t.Fatalf("Expected a delete operation but got %+v", v)
			}
			if v.Revision() != revision {
				t.Fatalf("Did not get expected revision: %d vs %d", revision, v.Revision())
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an update like expected")
		}
	}
	expectInitDone := func() {
		t.Helper()
		select {
		case v := <-watcher.Updates():
			if v != nil {
				t.Fatalf("Did not get expected: %+v", v)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive a init done like expected")
		}
	}

	// Make sure we already got an initial value marker.
	expectInitDone()

	kv.Create("name", []byte("derek"))
	expectUpdate("name", "derek", 1)
	kv.Put("name", []byte("rip"))
	expectUpdate("name", "rip", 2)
	kv.Put("name", []byte("ik"))
	expectUpdate("name", "ik", 3)
	kv.Put("age", []byte("22"))
	expectUpdate("age", "22", 4)
	kv.Put("age", []byte("33"))
	expectUpdate("age", "33", 5)
	kv.Delete("age")
	expectDelete("age", 6)

	// Stop first watcher.
	watcher.Stop()

	// Now try wildcard matching and make sure we only get last value when starting.
	kv.Put("t.name", []byte("rip"))
	kv.Put("t.name", []byte("ik"))
	kv.Put("t.age", []byte("22"))
	kv.Put("t.age", []byte("44"))

	watcher, err = kv.Watch("t.*")
	expectOk(t, err)
	defer watcher.Stop()

	expectUpdate("t.name", "ik", 8)
	expectUpdate("t.age", "44", 10)
	expectInitDone()
}

func TestKeyValueWatchContext(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCHCTX"})
	expectOk(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcher, err := kv.WatchAll(nats.Context(ctx))
	expectOk(t, err)
	defer watcher.Stop()

	// Trigger unsubscribe internally.
	cancel()

	// Wait for a bit for unsubscribe to be done.
	time.Sleep(500 * time.Millisecond)

	// Stopping watch that is already stopped via cancellation propagation is an error.
	err = watcher.Stop()
	if err == nil || err != nats.ErrBadSubscription {
		t.Errorf("Expected invalid subscription, got: %v", err)
	}
}

func TestKeyValueWatchContextUpdates(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCHCTX"})
	expectOk(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	watcher, err := kv.WatchAll(nats.Context(ctx))
	expectOk(t, err)
	defer watcher.Stop()

	// Pull the initial state done marker which is nil.
	select {
	case v := <-watcher.Updates():
		if v != nil {
			t.Fatalf("Expected nil marker, got %+v", v)
		}
	case <-time.After(time.Second):
		t.Fatalf("Did not receive nil marker like expected")
	}

	// Fire a timer and cancel the context after 250ms.
	time.AfterFunc(250*time.Millisecond, cancel)

	// Make sure canceling will break us out here.
	select {
	case <-watcher.Updates():
	case <-time.After(5 * time.Second):
		t.Fatalf("Did not break out like expected")
	}
}

func TestKeyValueBindStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	// Now bind to it..
	_, err = js.KeyValue("WATCH")
	expectOk(t, err)

	// Make sure we can't bind to a non-kv style stream.
	// We have some protection with stream name prefix.
	_, err = js.AddStream(&nats.StreamConfig{
		Name:     "KV_TEST",
		Subjects: []string{"foo"},
	})
	expectOk(t, err)

	_, err = js.KeyValue("TEST")
	expectErr(t, err)
	if err != nats.ErrBadBucket {
		t.Fatalf("Expected %v but got %v", nats.ErrBadBucket, err)
	}
}

func TestKeyValueDeleteStore(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	_, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "WATCH"})
	expectOk(t, err)

	err = js.DeleteKeyValue("WATCH")
	expectOk(t, err)

	_, err = js.KeyValue("WATCH")
	expectErr(t, err)
}

func TestKeyValueDeleteVsPurge(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	// Put in a few names and ages.
	put("name", "derek")
	put("age", "22")
	put("name", "ivan")
	put("age", "33")
	put("name", "rip")
	put("age", "44")

	kv.Delete("age")
	entries, err := kv.History("age")
	expectOk(t, err)
	// Expect three entries and delete marker.
	if len(entries) != 4 {
		t.Fatalf("Expected 4 entries for age after delete, got %d", len(entries))
	}
	err = kv.Purge("name")
	expectOk(t, err)
	// Check marker
	e, err := kv.Get("name")
	expectErr(t, err, nats.ErrKeyNotFound)
	if e != nil {
		t.Fatalf("Expected a nil entry but got %v", e)
	}
	entries, err = kv.History("name")
	expectOk(t, err)
	if len(entries) != 1 {
		t.Fatalf("Expected only 1 entry for age after delete, got %d", len(entries))
	}
	// Make sure history also reports the purge operation.
	if e := entries[0]; e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Expected a purge operation but got %v", e.Operation())
	}
}

func TestKeyValueDeleteTombstones(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	v := strings.Repeat("ABC", 33)
	for i := 1; i <= 100; i++ {
		put(fmt.Sprintf("key-%d", i), v)
	}
	// Now delete them.
	for i := 1; i <= 100; i++ {
		err := kv.Delete(fmt.Sprintf("key-%d", i))
		expectOk(t, err)
	}

	// Now cleanup.
	err = kv.PurgeDeletes(nats.DeleteMarkersOlderThan(-1))
	expectOk(t, err)

	si, err := js.StreamInfo("KV_KVS")
	expectOk(t, err)
	if si.State.Msgs != 0 {
		t.Fatalf("Expected no stream msgs to be left, got %d", si.State.Msgs)
	}

	// Try with context
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
	defer cancel()
	err = kv.PurgeDeletes(nats.Context(ctx))
	expectOk(t, err)
}

func TestKeyValuePurgeDeletesMarkerThreshold(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 10})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	put("foo", "foo1")
	put("bar", "bar1")
	put("foo", "foo2")
	err = kv.Delete("foo")
	expectOk(t, err)

	time.Sleep(200 * time.Millisecond)

	err = kv.Delete("bar")
	expectOk(t, err)

	err = kv.PurgeDeletes(nats.DeleteMarkersOlderThan(100 * time.Millisecond))
	expectOk(t, err)

	// The key foo should have been completely cleared of the data
	// and the delete marker.
	fooEntries, err := kv.History("foo")
	if err != nats.ErrKeyNotFound {
		t.Fatalf("Expected all entries for key foo to be gone, got err=%v entries=%v", err, fooEntries)
	}
	barEntries, err := kv.History("bar")
	expectOk(t, err)
	if len(barEntries) != 1 {
		t.Fatalf("Expected 1 entry, got %v", barEntries)
	}
	if e := barEntries[0]; e.Operation() != nats.KeyValueDelete {
		t.Fatalf("Unexpected entry: %+v", e)
	}
}

func TestKeyValueKeys(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "KVS", History: 2})
	expectOk(t, err)

	put := func(key, value string) {
		t.Helper()
		_, err := kv.Put(key, []byte(value))
		expectOk(t, err)
	}

	_, err = kv.Keys()
	expectErr(t, err, nats.ErrNoKeysFound)

	// Put in a few names and ages.
	put("name", "derek")
	put("age", "22")
	put("country", "US")
	put("name", "ivan")
	put("age", "33")
	put("country", "US")
	put("name", "rip")
	put("age", "44")
	put("country", "MT")

	keys, err := kv.Keys()
	expectOk(t, err)

	kmap := make(map[string]struct{})
	for _, key := range keys {
		if _, ok := kmap[key]; ok {
			t.Fatalf("Already saw %q", key)
		}
		kmap[key] = struct{}{}
	}
	if len(kmap) != 3 {
		t.Fatalf("Expected 3 total keys, got %d", len(kmap))
	}
	expected := map[string]struct{}{
		"name":    struct{}{},
		"age":     struct{}{},
		"country": struct{}{},
	}
	if !reflect.DeepEqual(kmap, expected) {
		t.Fatalf("Expected %+v but got %+v", expected, kmap)
	}
	// Make sure delete and purge do the right thing and not return the keys.
	err = kv.Delete("name")
	expectOk(t, err)
	err = kv.Purge("country")
	expectOk(t, err)

	keys, err = kv.Keys()
	expectOk(t, err)

	kmap = make(map[string]struct{})
	for _, key := range keys {
		if _, ok := kmap[key]; ok {
			t.Fatalf("Already saw %q", key)
		}
		kmap[key] = struct{}{}
	}
	if len(kmap) != 1 {
		t.Fatalf("Expected 1 total key, got %d", len(kmap))
	}
	if _, ok := kmap["age"]; !ok {
		t.Fatalf("Expected %q to be only key present", "age")
	}
}

func TestKeyValueDiscardNew(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	kv, err := js.CreateKeyValue(&nats.KeyValueConfig{Bucket: "TEST", History: 1, MaxBytes: 256})
	expectOk(t, err)

	vc := func() (major, minor, patch int) {
		semVerRe := regexp.MustCompile(`\Av?([0-9]+)\.?([0-9]+)?\.?([0-9]+)?`)
		m := semVerRe.FindStringSubmatch(nc.ConnectedServerVersion())
		expectOk(t, err)
		major, err = strconv.Atoi(m[1])
		expectOk(t, err)
		minor, err = strconv.Atoi(m[2])
		expectOk(t, err)
		patch, err = strconv.Atoi(m[3])
		expectOk(t, err)
		return major, minor, patch
	}

	major, minor, patch := vc()
	status, err := kv.Status()
	expectOk(t, err)
	kvs := status.(*nats.KeyValueBucketStatus)
	si := kvs.StreamInfo()

	// If we are 2.7.1 or below DiscardOld should be used.
	// If 2.7.2 or above should be DiscardNew
	if major <= 2 && minor <= 7 && patch <= 1 {
		if si.Config.Discard != nats.DiscardOld {
			t.Fatalf("Expected Discard Old for server version %d.%d.%d", major, minor, patch)
		}
	} else {
		if si.Config.Discard != nats.DiscardNew {
			t.Fatalf("Expected Discard New for server version %d.%d.%d", major, minor, patch)
		}
	}
}

func TestKeyValueCrossAccounts(t *testing.T) {
	conf := createConfFile(t, []byte(`
        jetstream: enabled
        accounts: {
           A: {
               users: [ {user: a, password: a} ]
               jetstream: enabled
               exports: [
                   {service: '$JS.API.>' }
                   {service: '$KV.>'}
                   {stream: 'accI.>'}
               ]
           },
           I: {
               users: [ {user: i, password: i} ]
               imports: [
                   {service: {account: A, subject: '$JS.API.>'}, to: 'fromA.>' }
                   {service: {account: A, subject: '$KV.>'}, to: 'fromA.$KV.>' }
                   {stream: {subject: 'accI.>', account: A}}
               ]
           }
		}`))
	defer os.Remove(conf)
	s, _ := RunServerWithConfig(conf)
	defer shutdownJSServerAndRemoveStorage(t, s)

	watchNext := func(w nats.KeyWatcher) nats.KeyValueEntry {
		t.Helper()
		select {
		case e := <-w.Updates():
			return e
		case <-time.After(time.Second):
			t.Fatal("Fail to get the next update")
		}
		return nil
	}

	nc1, js1 := jsClient(t, s, nats.UserInfo("a", "a"))
	defer nc1.Close()

	kv1, err := js1.CreateKeyValue(&nats.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w1, err := kv1.Watch("map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w1); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	nc2, err := nats.Connect(s.ClientURL(), nats.UserInfo("i", "i"), nats.CustomInboxPrefix("accI"))
	if err != nil {
		t.Fatalf("Error on connect: %v", err)
	}
	defer nc2.Close()
	js2, err := nc2.JetStream(nats.APIPrefix("fromA"))
	if err != nil {
		t.Fatalf("Error getting jetstream context: %v", err)
	}

	kv2, err := js2.CreateKeyValue(&nats.KeyValueConfig{Bucket: "Map", History: 10})
	if err != nil {
		t.Fatalf("Error creating kv store: %v", err)
	}

	w2, err := kv2.Watch("map")
	if err != nil {
		t.Fatalf("Error creating watcher: %v", err)
	}
	if e := watchNext(w2); e != nil {
		t.Fatalf("Expected nil entry, got %+v", e)
	}

	// Do a Put from kv2
	rev, err := kv2.Put("map", []byte("value"))
	if err != nil {
		t.Fatalf("Error on put: %v", err)
	}

	// Get from kv1
	e, err := kv1.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Watcher 1
	if e := watchNext(w1); e == nil || e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Watcher 2
	if e := watchNext(w2); e == nil || e.Key() != "map" || string(e.Value()) != "value" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Try an update form kv2
	if _, err := kv2.Update("map", []byte("updated"), rev); err != nil {
		t.Fatalf("Failed to update: %v", err)
	}

	// Get from kv1
	e, err = kv1.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Get from kv2
	e, err = kv2.Get("map")
	if err != nil {
		t.Fatalf("Error on get: %v", err)
	}
	if e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: +%v", e)
	}

	// Watcher 1
	if e := watchNext(w1); e == nil || e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Watcher 2
	if e := watchNext(w2); e == nil || e.Key() != "map" || string(e.Value()) != "updated" {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Purge from kv2
	if err := kv2.Purge("map"); err != nil {
		t.Fatalf("Error on purge: %v", err)
	}

	// Check purge ok from w1
	if e := watchNext(w1); e == nil || e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Check purge ok from w2
	if e := watchNext(w2); e == nil || e.Operation() != nats.KeyValuePurge {
		t.Fatalf("Unexpected entry: %+v", e)
	}

	// Delete purge records from kv2
	if err := kv2.PurgeDeletes(nats.DeleteMarkersOlderThan(-1)); err != nil {
		t.Fatalf("Error on purge deletes: %v", err)
	}

	// Check all gone from js1
	if si, err := js1.StreamInfo("KV_Map"); err != nil || si == nil || si.State.Msgs != 0 {
		t.Fatalf("Error getting stream info: err=%v si=%+v", err, si)
	}

	// Delete key from kv2
	if err := kv2.Delete("map"); err != nil {
		t.Fatalf("Error on delete: %v", err)
	}

	// Check key gone from kv1
	if e, err := kv1.Get("map"); err != nats.ErrKeyNotFound || e != nil {
		t.Fatalf("Expected key not found, got err=%v e=%+v", err, e)
	}
}

// Helpers

func client(t *testing.T, s *server.Server, opts ...nats.Option) *nats.Conn {
	t.Helper()
	nc, err := nats.Connect(s.ClientURL(), opts...)
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
	return nc
}

func jsClient(t *testing.T, s *server.Server, opts ...nats.Option) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	nc := client(t, s, opts...)
	js, err := nc.JetStream(nats.MaxWait(10 * time.Second))
	if err != nil {
		t.Fatalf("Unexpected error getting JetStream context: %v", err)
	}
	return nc, js
}

func expectOk(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("Unexpected error: %v", err)
	}
}

func expectErr(t *testing.T, err error, expected ...error) {
	t.Helper()
	if err == nil {
		t.Fatalf("Expected error but got none")
	}
	if len(expected) == 0 {
		return
	}
	for _, e := range expected {
		if err == e || strings.Contains(e.Error(), err.Error()) {
			return
		}
	}
	t.Fatalf("Expected one of %+v, got '%v'", expected, err)
}
