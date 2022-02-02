// Copyright 2019-2022 The NATS Authors
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

//go:build !race
// +build !race

package test

import (
	"bytes"
	"context"
	"crypto/rand"
	"io"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestNoRaceObjectContextOpt(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	time.AfterFunc(100*time.Millisecond, cancel)

	start := time.Now()
	_, err = obs.Put(&nats.ObjectMeta{Name: "TEST"}, &slow{1000}, nats.Context(ctx))
	expectErr(t, err)
	if delta := time.Since(start); delta > time.Second {
		t.Fatalf("Cancel took too long: %v", delta)
	}
	si, err := js.StreamInfo("OBJ_OBJS")
	expectOk(t, err)
	if si.State.Msgs != 0 {
		t.Fatalf("Expected no messages after canceling put, got %+v", si.State)
	}

	// Now put a large object in there.
	blob := make([]byte, 8*1024*1024)
	rand.Read(blob)
	_, err = obs.PutBytes("BLOB", blob)
	expectOk(t, err)

	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	time.AfterFunc(100*time.Millisecond, cancel)

	time.AfterFunc(20*time.Millisecond, func() { shutdownJSServerAndRemoveStorage(t, s) })
	start = time.Now()
	_, err = obs.GetBytes("BLOB", nats.Context(ctx))
	expectErr(t, err)
	if delta := time.Since(start); delta > 2500*time.Millisecond {
		t.Fatalf("Cancel took too long: %v", delta)
	}
}

type slow struct{ n int }

func (sr *slow) Read(p []byte) (n int, err error) {
	if sr.n <= 0 {
		return 0, io.EOF
	}
	sr.n--
	time.Sleep(10 * time.Millisecond)
	p[0] = 'A'
	return 1, nil
}

func TestNoRaceObjectDoublePut(t *testing.T) {
	s := RunBasicJetStreamServer()
	defer shutdownJSServerAndRemoveStorage(t, s)

	nc, js := jsClient(t, s)
	defer nc.Close()

	obs, err := js.CreateObjectStore(&nats.ObjectStoreConfig{Bucket: "OBJS"})
	expectOk(t, err)

	_, err = obs.PutBytes("A", bytes.Repeat([]byte("A"), 1_000_000))
	expectOk(t, err)

	_, err = obs.PutBytes("A", bytes.Repeat([]byte("a"), 20_000_000))
	expectOk(t, err)

	_, err = obs.GetBytes("A")
	expectOk(t, err)
}
