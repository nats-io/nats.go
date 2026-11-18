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

//go:build internal_testing

package test

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestKeyValueSnapshotInterrupted(t *testing.T) {
	for _, operation := range []string{"Keys", "History"} {
		for _, tc := range []struct {
			name string
			err  error
		}{
			{"canceled", context.Canceled},
			{"deadline", context.DeadlineExceeded},
			{"connection closed", jetstream.ErrKeyWatcherClosed},
		} {
			t.Run(operation+"/"+tc.name, func(t *testing.T) {
				withJSServer(t, func(t *testing.T, nc *nats.Conn, js jetstream.JetStream) {
					ctx := newTesterCtx(t, 10*time.Second)
					kv, err := js.CreateKeyValue(ctx, jetstream.KeyValueConfig{Bucket: "SNAPSHOT", History: 3})
					expectOk(t, err)
					for _, key := range []string{"first", "second", "last", "last", "last"} {
						_, err := kv.PutString(ctx, key, "value")
						expectOk(t, err)
					}

					// Drop the final delivery to leave the real watcher with
					// entries but without the initial-values marker.
					last := make(chan struct{}, 1)
					nc.AddMsgFilter("$KV.SNAPSHOT.last", func(m *nats.Msg) *nats.Msg {
						meta, err := m.Metadata()
						if err == nil && meta.NumPending == 0 {
							select {
							case last <- struct{}{}:
							default:
							}
							return nil
						}
						return m
					})
					defer nc.RemoveMsgFilter("$KV.SNAPSHOT.last")

					timeout := 5 * time.Second
					if tc.err == context.DeadlineExceeded {
						timeout = time.Second
					}
					listCtx, cancel := context.WithTimeout(ctx, timeout)
					defer cancel()

					waiting := make(chan bool, 1)
					go func() {
						select {
						case <-last:
						case <-listCtx.Done():
							waiting <- false
							return
						}

						// Once the callbacks have run, the watcher is set up
						// and has delivered its earlier entries.
						delivered := make(chan struct{})
						if nc.Barrier(func() { close(delivered) }) != nil {
							waiting <- false
							return
						}
						select {
						case <-delivered:
						case <-listCtx.Done():
							waiting <- false
							return
						}

						waiting <- true
						switch tc.err {
						case context.Canceled:
							cancel()
						case jetstream.ErrKeyWatcherClosed:
							nc.Close()
						}
					}()

					if operation == "Keys" {
						keys, err := kv.Keys(listCtx)
						expectErr(t, err, tc.err)
						if keys != nil {
							t.Fatalf("Keys returned partial results: %v", keys)
						}
					} else {
						entries, err := kv.History(listCtx, "last")
						expectErr(t, err, tc.err)
						if entries != nil {
							t.Fatalf("History returned %d entries for an interrupted snapshot", len(entries))
						}
					}
					if !<-waiting {
						t.Fatal("Watcher did not receive the initial entries before interruption")
					}
				})
			})
		}
	}
}
