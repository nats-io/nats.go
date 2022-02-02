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

package test

import (
	"bytes"
	"crypto/rand"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestObjectBasics(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS", Description: "testing"})
	expectOk(t, err)

	// Create ~16MB object.
	blob := make([]byte, 16*1024*1024+22)
	rand.Read(blob)

	now := time.Now().UTC().Round(time.Second)
	_, err = obs.PutBytes("BLOB", blob)
	expectOk(t, err)

	// Test info
	info, err := obs.GetInfo("BLOB")
	expectOk(t, err)
	if len(info.NUID) == 0 {
		t.Fatalf("Expected object to have a NUID")
	}
	if info.ModTime.IsZero() {
		t.Fatalf("Expected object to have a non-zero ModTime")
	}
	if mt := info.ModTime.Round(time.Second); mt.Sub(now) != 0 && mt.Sub(now) != time.Second {
		t.Fatalf("Expected ModTime to be about %v, got %v", now, mt)
	}

	// Make sure the stream is sealed.
	err = obs.Seal()
	expectOk(t, err)
	si, err := js.StreamInfo("OBJ_OBJS")
	expectOk(t, err)
	if !si.Config.Sealed {
		t.Fatalf("Expected the object stream to be sealed, got %+v", si)
	}

	status, err := obs.Status()
	expectOk(t, err)
	if !status.Sealed() {
		t.Fatalf("exected sealed status")
	}
	if status.Size() == 0 {
		t.Fatalf("size is 0")
	}
	if status.Storage() != nats.FileStorage {
		t.Fatalf("stauts reports %d storage", status.Storage())
	}
	if status.Description() != "testing" {
		t.Fatalf("invalid description: '%s'", status.Description())
	}

	// Check simple errors.
	_, err = obs.Get("FOO")
	expectErr(t, err)

	// Now get the object back.
	result, err := obs.Get("BLOB")
	expectOk(t, err)
	expectOk(t, result.Error())
	defer result.Close()

	// Check info.
	info, err = result.Info()
	expectOk(t, err)
	if info.Size != uint64(len(blob)) {
		t.Fatalf("Size does not match, %d vs %d", info.Size, len(blob))
	}

	// Check result.
	copy, err := ioutil.ReadAll(result)
	expectOk(t, err)
	if !bytes.Equal(copy, blob) {
		t.Fatalf("Result not the same")
	}
	// Test delete.
	err = js.DeleteObjectStore("OBJS")
	expectOk(t, err)
	_, err = obs.Get("BLOB")
	expectErr(t, err, nats.ErrStreamNotFound)
}

func TestDefaultObjectStatus(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS", Description: "testing"})
	expectOk(t, err)

	blob := make([]byte, 1024*1024+22)
	rand.Read(blob)

	_, err = obs.PutBytes("BLOB", blob)
	expectOk(t, err)

	status, err := obs.Status()
	expectOk(t, err)
	if status.BackingStore() != "JetStream" {
		t.Fatalf("invalid backing store kind: %s", status.BackingStore())
	}
	bs := status.(*nats.ObjectBucketStatus)
	info := bs.StreamInfo()
	if info.Config.Name != "OBJ_OBJS" {
		t.Fatalf("invalid stream name %+v", info)
	}
}

func TestObjectFileBasics(t *testing.T) {
	// The filename is likely to be something like C:\xxx and we don't allow
	// the '\' or ':' characters. We could translate the tmpFile.Name() from
	// C:\foo\bar to C:/foo/bar (since Windows is OK with that), however,
	// that would still fail on key validation because `:` is not allowed.
	if runtime.GOOS == "windows" {
		t.SkipNow()
	}
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "FILES"})
	expectOk(t, err)

	// Create ~8MB object.
	blob := make([]byte, 8*1024*1024+33)
	rand.Read(blob)

	tmpFile, err := ioutil.TempFile("", "objfile")
	expectOk(t, err)
	defer os.Remove(tmpFile.Name()) // clean up
	err = ioutil.WriteFile(tmpFile.Name(), blob, 0600)
	expectOk(t, err)

	_, err = obs.PutFile(tmpFile.Name())
	expectOk(t, err)

	tmpResult, err := ioutil.TempFile("", "objfileresult")
	expectOk(t, err)
	defer os.Remove(tmpResult.Name()) // clean up

	err = obs.GetFile(tmpFile.Name(), tmpResult.Name())
	expectOk(t, err)

	// Make sure they are the same.
	original, err := ioutil.ReadFile(tmpFile.Name())
	expectOk(t, err)

	restored, err := ioutil.ReadFile(tmpResult.Name())
	expectOk(t, err)

	if !bytes.Equal(original, restored) {
		t.Fatalf("Files did not match")
	}
}

func TestObjectMulti(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "TEST_FILES"})
	expectOk(t, err)

	numFiles := 0
	fis, _ := ioutil.ReadDir(".")
	for _, fi := range fis {
		fn := fi.Name()
		// Just grab clean test files.
		if filepath.Ext(fn) != ".go" || fn[0] == '.' || fn[0] == '#' {
			continue
		}
		_, err = obs.PutFile(fn)
		expectOk(t, err)
		numFiles++
	}
	expectOk(t, obs.Seal())

	_, err = js.StreamInfo("OBJ_TEST_FILES")
	expectOk(t, err)

	result, err := obs.Get("object_test.go")
	expectOk(t, err)
	expectOk(t, result.Error())
	defer result.Close()

	_, err = result.Info()
	expectOk(t, err)

	copy, err := ioutil.ReadAll(result)
	expectOk(t, err)

	orig, err := ioutil.ReadFile(path.Join(".", "object_test.go"))
	expectOk(t, err)

	if !bytes.Equal(orig, copy) {
		t.Fatalf("Files did not match")
	}
}

func TestObjectDeleteMarkers(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	msg := bytes.Repeat([]byte("A"), 100)
	_, err = obs.PutBytes("A", msg)
	expectOk(t, err)

	err = obs.Delete("A")
	expectOk(t, err)

	si, err := js.StreamInfo("OBJ_OBJS")
	expectOk(t, err)

	// We should have one message left. The delete marker.
	if si.State.Msgs != 1 {
		t.Fatalf("Expected 1 marker msg, got %d msgs", si.State.Msgs)
	}
	// Make sure we have a delete marker, this will be there to drive Watch functionality.
	info, err := obs.GetInfo("A")
	expectOk(t, err)
	if !info.Deleted {
		t.Fatalf("Expected info to be marked as deleted")
	}
}

func TestObjectMultiWithDelete(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "2OD"})
	expectOk(t, err)

	pa := bytes.Repeat([]byte("A"), 2_000_000)
	pb := bytes.Repeat([]byte("B"), 3_000_000)

	_, err = obs.PutBytes("A", pa)
	expectOk(t, err)

	// Hold onto this so we can make sure DeleteObject clears all messages, chunks and meta.
	si, err := js.StreamInfo("OBJ_2OD")
	expectOk(t, err)

	_, err = obs.PutBytes("B", pb)
	expectOk(t, err)

	pb2, err := obs.GetBytes("B")
	expectOk(t, err)

	if !bytes.Equal(pb, pb2) {
		t.Fatalf("Did not retrieve same object")
	}

	// Now delete B
	err = obs.Delete("B")
	expectOk(t, err)

	siad, err := js.StreamInfo("OBJ_2OD")
	expectOk(t, err)
	if siad.State.Msgs != si.State.Msgs+1 { // +1 more delete marker.
		t.Fatalf("Expected to have %d msgs after delete, got %d", siad.State.Msgs, si.State.Msgs+1)
	}
}

func TestObjectNames(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	// Test filename like naming.
	_, err = obs.PutString("BLOB.txt", "A")
	expectOk(t, err)
	// Spaces ok
	_, err = obs.PutString("foo bar", "A")
	expectOk(t, err)

	// Errors
	_, err = obs.PutString("*", "A")
	expectErr(t, err)
	_, err = obs.PutString(">", "A")
	expectErr(t, err)
	_, err = obs.PutString("", "A")
	expectErr(t, err)
	_, err = obs.PutString("", "\t")
	expectErr(t, err)
	_, err = obs.PutString("", "\n")
	expectErr(t, err)
}

func TestObjectMetadata(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "META-TEST"})
	expectOk(t, err)

	// Simple with no Meta.
	_, err = obs.PutString("A", "AAA")
	expectOk(t, err)

	meta := &nats.ObjectMeta{Name: "B"}

	err = obs.UpdateMeta("A", meta)
	expectOk(t, err)

	info, err := obs.GetInfo("B")
	expectOk(t, err)
	if info.Name != "B" {
		t.Fatalf("Update failed: %+v", info)
	}
	meta.Headers = make(nats.Header)
	meta.Headers.Set("color", "red")

	err = obs.UpdateMeta("B", meta)
	expectOk(t, err)

	info, err = obs.GetInfo("B")
	expectOk(t, err)
	if info.Headers == nil || info.Headers.Get("color") != "red" {
		t.Fatalf("Update failed: %+v", info)
	}
}

func TestObjectWatch(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "WATCH-TEST"})
	expectOk(t, err)

	watcher, err := obs.Watch()
	expectOk(t, err)
	defer watcher.Stop()

	expectUpdate := func(name string) {
		t.Helper()
		select {
		case info := <-watcher.Updates():
			if false && info.Name != name {
				t.Fatalf("Expected update for %q, but got %+v", name, info)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive an update like expected")
		}
	}

	expectNoMoreUpdates := func() {
		t.Helper()
		select {
		case info := <-watcher.Updates():
			t.Fatalf("Got an unexpected update: %+v", info)
		case <-time.After(100 * time.Millisecond):
		}
	}

	expectInitDone := func() {
		t.Helper()
		select {
		case info := <-watcher.Updates():
			if info != nil {
				t.Fatalf("Did not get expected: %+v", info)
			}
		case <-time.After(time.Second):
			t.Fatalf("Did not receive a init done like expected")
		}
	}

	// We should get a marker that is nil when all initital values are delivered.
	expectInitDone()

	_, err = obs.PutString("A", "AAA")
	expectOk(t, err)
	_, err = obs.PutString("B", "BBB")
	expectOk(t, err)

	// Initial Values.
	expectUpdate("A")
	expectUpdate("B")
	expectNoMoreUpdates()

	// Delete
	err = obs.Delete("A")
	expectOk(t, err)

	expectUpdate("A")
	expectNoMoreUpdates()

	// New
	_, err = obs.PutString("C", "CCC")
	expectOk(t, err)

	// Update Meta
	info, err := obs.GetInfo("A")
	expectOk(t, err)
	meta := &info.ObjectMeta
	meta.Description = "A's are better than B's"
	err = obs.UpdateMeta("A", meta)
	expectOk(t, err)
}

func TestObjectLinks(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	root, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "ROOT"})
	expectOk(t, err)

	_, err = root.PutString("A", "AAA")
	expectOk(t, err)
	_, err = root.PutString("B", "BBB")
	expectOk(t, err)

	info, err := root.GetInfo("A")
	expectOk(t, err)

	// Self link to individual object.
	_, err = root.AddLink("LA", info)
	expectOk(t, err)

	dir, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "DIR"})
	expectOk(t, err)

	_, err = dir.PutString("DIR/A", "DIR-AAA")
	expectOk(t, err)
	_, err = dir.PutString("DIR/B", "DIR-BBB")
	expectOk(t, err)

	info, err = dir.GetInfo("DIR/B")
	expectOk(t, err)

	binfo, err := root.AddLink("DBL", info)
	expectOk(t, err)

	if binfo.Name != "DBL" || binfo.NUID == "" || binfo.ModTime.IsZero() {
		t.Fatalf("Link info not what was expected: %+v", binfo)
	}

	// Now add whole other store as a link, like a directory.
	_, err = root.AddBucketLink("dir", dir)
	expectOk(t, err)

	// Now try to get a linked object.
	dbl, err := root.GetString("DBL")
	expectOk(t, err)

	if dbl != "DIR-BBB" {
		t.Fatalf("Expected %q but got %q", "DIR-BBB", dbl)
	}
}

// Right now no history, just make sure we are cleaning up after ourselves.
func TestObjectHistory(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	_, err = obs.PutBytes("A", bytes.Repeat([]byte("A"), 100))
	expectOk(t, err)

	_, err = obs.PutBytes("A", bytes.Repeat([]byte("a"), 100))
	expectOk(t, err)

	// Should only be 1 copy of 'A', so 1 data and 1 meta since history was not selected.
	si, err := js.StreamInfo("OBJ_OBJS")
	expectOk(t, err)

	if si.State.Msgs != 2 {
		t.Fatalf("Expected 2 msgs (1 data 1 meta) but got %d", si.State.Msgs)
	}
}

func TestObjectList(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	root, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "ROOT"})
	expectOk(t, err)

	put := func(name, value string) {
		_, err = root.PutString(name, value)
		expectOk(t, err)
	}

	put("A", "AAA")
	put("B", "BBB")
	put("C", "CCC")
	put("B", "bbb")

	// Self link
	info, err := root.GetInfo("B")
	expectOk(t, err)
	_, err = root.AddLink("b", info)
	expectOk(t, err)

	put("D", "DDD")
	err = root.Delete("D")
	expectOk(t, err)

	lch, err := root.List()
	expectOk(t, err)

	omap := make(map[string]struct{})
	for _, info := range lch {
		if _, ok := omap[info.Name]; ok {
			t.Fatalf("Already saw %q", info.Name)
		}
		omap[info.Name] = struct{}{}
	}
	if len(omap) != 4 {
		t.Fatalf("Expected 4 total objects, got %d", len(omap))
	}
	expected := map[string]struct{}{
		"A": struct{}{},
		"B": struct{}{},
		"C": struct{}{},
		"b": struct{}{},
	}
	if !reflect.DeepEqual(omap, expected) {
		t.Fatalf("Expected %+v but got %+v", expected, omap)
	}
}
