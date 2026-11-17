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
	"context"
	"errors"
	"fmt"
	"testing"
	"testing/synctest"
	"time"
)

func TestKeyValueSnapshotContext(t *testing.T) {
	for _, operation := range []string{"Keys", "History"} {
		for _, partial := range []bool{false, true} {
			for _, deadline := range []bool{false, true} {
				for _, closed := range []bool{false, true} {
					t.Run(fmt.Sprintf("%s/partial=%v/deadline=%v/closed=%v", operation, partial, deadline, closed), func(t *testing.T) {
						synctest.Test(t, func(t *testing.T) {
							ctx, cancel := context.WithCancel(context.Background())
							wantErr := context.Canceled
							if deadline {
								cancel()
								ctx, cancel = context.WithTimeout(context.Background(), time.Second)
								wantErr = context.DeadlineExceeded
							}
							defer cancel()
							updates := make(chan KeyValueEntry)
							done := make(chan struct{})
							go func() {
								defer close(done)
								if operation == "Keys" {
									keys, err := collectKeys(ctx, updates)
									if !errors.Is(err, wantErr) || keys != nil {
										t.Errorf("Keys returned %v, %v; want nil, %v", keys, err, wantErr)
									}
								} else {
									entries, err := collectHistory(ctx, updates)
									if !errors.Is(err, wantErr) || entries != nil {
										t.Errorf("History returned %v, %v; want nil, %v", entries, err, wantErr)
									}
								}
							}()
							if partial {
								updates <- &kve{key: "key", value: []byte("value")}
							}
							synctest.Wait()
							if deadline {
								<-ctx.Done()
							} else {
								cancel()
							}
							// Cover both a watcher closing on expiry and a silent watcher that
							// leaves its updates channel open. Neither has sent the nil marker.
							if closed {
								close(updates)
							}
							<-done
						})
					})
				}
			}
		}
	}
}

func TestKeyValueSnapshotComplete(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(fmt.Sprintf("empty=%v", empty), func(t *testing.T) {
			snapshot := func() <-chan KeyValueEntry {
				updates := make(chan KeyValueEntry, 5)
				if !empty {
					updates <- &kve{key: "b", revision: 1}
					updates <- &kve{key: "a", revision: 2}
					updates <- &kve{key: "a", revision: 3}
				}
				updates <- nil
				// Updates after the initial-values marker are outside the snapshot.
				updates <- &kve{key: "later", revision: 4}
				return updates
			}
			ctx := context.Background()
			keys, err := collectKeys(ctx, snapshot())
			if empty {
				if keys != nil || !errors.Is(err, ErrNoKeysFound) {
					t.Fatalf("Keys returned %v, %v; want nil, ErrNoKeysFound", keys, err)
				}
			} else if err != nil || len(keys) != 2 || keys[0] != "a" || keys[1] != "b" {
				t.Fatalf("Keys returned %v, %v; want [a b], nil", keys, err)
			}
			entries, err := collectHistory(ctx, snapshot())
			if empty {
				if entries != nil || !errors.Is(err, ErrKeyNotFound) {
					t.Fatalf("History returned %v, %v; want nil, ErrKeyNotFound", entries, err)
				}
			} else {
				if err != nil || len(entries) != 3 {
					t.Fatalf("History returned %v, %v; want three entries, nil", entries, err)
				}
				for i, entry := range entries {
					if entry.Revision() != uint64(i+1) {
						t.Fatalf("History entry %d has revision %d", i, entry.Revision())
					}
				}
			}
		})
	}
}

func TestKeyValueSnapshotCanceledWithBufferedMarker(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	snapshot := func() <-chan KeyValueEntry {
		updates := make(chan KeyValueEntry, 2)
		updates <- &kve{key: "key"}
		updates <- nil
		return updates
	}
	if keys, err := collectKeys(ctx, snapshot()); keys != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("Keys returned %v, %v; want nil, context.Canceled", keys, err)
	}
	if entries, err := collectHistory(ctx, snapshot()); entries != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("History returned %v, %v; want nil, context.Canceled", entries, err)
	}
}
