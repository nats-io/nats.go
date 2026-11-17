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

	"github.com/nats-io/nats.go"
)

// Hold deliveries short of the initial-values marker, without needing a server
// or depending on how many messages fit into a short timeout.
type incompleteKVSubscription struct {
	nats.JetStreamContext
	callback chan nats.MsgHandler
}

func (js *incompleteKVSubscription) Subscribe(_ string, cb nats.MsgHandler, _ ...nats.SubOpt) (*nats.Subscription, error) {
	js.callback <- cb
	return &nats.Subscription{}, nil
}

func TestKeyValueSnapshotContext(t *testing.T) {
	for _, operation := range []string{"Keys", "History"} {
		for _, partial := range []bool{false, true} {
			for _, deadline := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/partial=%v/deadline=%v", operation, partial, deadline), func(t *testing.T) {
					synctest.Test(t, func(t *testing.T) {
						ctx, cancel := context.WithCancel(context.Background())
						wantErr := context.Canceled
						if deadline {
							cancel()
							ctx, cancel = context.WithTimeout(context.Background(), time.Second)
							wantErr = context.DeadlineExceeded
						}
						defer cancel()
						js := &incompleteKVSubscription{callback: make(chan nats.MsgHandler, 1)}
						kv := &kvs{name: "test", pre: "$KV.test.", streamName: "KV_test", pushJS: js}
						done := make(chan struct{})
						go func() {
							defer close(done)
							// UpdatesOnly suppresses the initial-values marker, allowing explicit
							// control of an incomplete snapshot even after delivering an entry.
							if operation == "Keys" {
								keys, err := kv.Keys(ctx, UpdatesOnly())
								if !errors.Is(err, wantErr) || keys != nil {
									t.Errorf("Keys returned %v, %v; want nil, %v", keys, err, wantErr)
								}
							} else {
								entries, err := kv.History(ctx, "key", UpdatesOnly())
								if !errors.Is(err, wantErr) || entries != nil {
									t.Errorf("History returned %v, %v; want nil, %v", entries, err, wantErr)
								}
							}
						}()
						callback := <-js.callback
						synctest.Wait()
						if partial {
							callback(&nats.Msg{Subject: "$KV.test.key", Reply: "$JS.ACK.KV_test.consumer.1.1.1.1.1", Data: []byte("value")})
							synctest.Wait()
						}
						if deadline {
							time.Sleep(time.Second)
						} else {
							cancel()
						}
						<-done
					})
				})
			}
		}
	}
}
