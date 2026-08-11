// Copyright 2012-2019 The NATS Authors
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
	"fmt"
	"math/rand/v2"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func BenchmarkPublishSpeed(b *testing.B) {
	withServerB(b, func(b *testing.B, nc *nats.Conn) {
		msg := []byte("Hello World")
		b.ResetTimer()
		for range b.N {
			if err := nc.Publish("foo", msg); err != nil {
				b.Fatalf("Error in benchmark during Publish: %v\n", err)
			}
		}
		nc.Flush()
		b.StopTimer()
	})
}

func BenchmarkPublishSpeedHeaders(b *testing.B) {
	withServerB(b, func(b *testing.B, nc *nats.Conn) {
		headers := make(nats.Header, 10)
		for i := range 10 {
			headers.Add(
				fmt.Sprintf("header_%d", i),
				generateRandomString(32),
			)
		}
		msg := &nats.Msg{
			Subject: "foo",
			Header:  headers,
			Data:    []byte("Hello World"),
		}

		b.ResetTimer()
		for range b.N {
			if err := nc.PublishMsg(msg); err != nil {
				b.Fatalf("Error in benchmark during PublishMsg: %v\n", err)
			}
		}
		nc.Flush()
		b.StopTimer()
	})
}

func generateRandomString(length int) string {
	const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, length)
	for i := range b {
		b[i] = charset[rand.IntN(len(charset))]
	}
	return string(b)
}

func BenchmarkPubSubSpeed(b *testing.B) {
	withServerB(b, func(b *testing.B, nc *nats.Conn) {
		ch := make(chan bool)

		nc.SetErrorHandler(func(nc *nats.Conn, s *nats.Subscription, err error) {
			b.Fatalf("Error : %v\n", err)
		})

		received := int32(0)

		nc.Subscribe("foo", func(m *nats.Msg) {
			if nr := atomic.AddInt32(&received, 1); nr >= int32(b.N) {
				ch <- true
			}
		})

		msg := []byte("Hello World")

		b.ResetTimer()
		for range b.N {
			if err := nc.Publish("foo", msg); err != nil {
				b.Fatalf("Error in benchmark during Publish: %v\n", err)
			}
		}

		err := WaitTime(ch, 10*time.Second)
		if err != nil {
			b.Fatal("Timed out waiting for messages")
		} else if atomic.LoadInt32(&received) != int32(b.N) {
			b.Fatalf("Received: %d, err:%v", received, nc.LastError())
		}
		b.StopTimer()
	})
}

func BenchmarkAsyncSubscriptionCreationSpeed(b *testing.B) {
	withServerB(b, func(b *testing.B, nc *nats.Conn) {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			nc.Subscribe("foo", func(m *nats.Msg) {})
		}
	})
}

func BenchmarkSyncSubscriptionCreationSpeed(b *testing.B) {
	withServerB(b, func(b *testing.B, nc *nats.Conn) {
		b.ResetTimer()
		b.ReportAllocs()
		for range b.N {
			nc.SubscribeSync("foo")
		}
	})
}

func BenchmarkInboxCreation(b *testing.B) {
	for range b.N {
		nats.NewInbox()
	}
}

func BenchmarkNewInboxCreation(b *testing.B) {
	withServerB(b, func(b *testing.B, nc *nats.Conn) {
		b.ResetTimer()
		for range b.N {
			nc.NewRespInbox()
		}
	})
}

func BenchmarkRequest(b *testing.B) {
	withServerB(b, func(b *testing.B, nc *nats.Conn) {
		ok := []byte("ok")
		nc.Subscribe("req", func(m *nats.Msg) {
			nc.Publish(m.Reply, ok)
		})
		b.ResetTimer()
		b.ReportAllocs()
		q := []byte("q")
		for range b.N {
			_, err := nc.Request("req", q, 1*time.Second)
			if err != nil {
				b.Fatalf("Err %v\n", err)
			}
		}
	})
}

func BenchmarkOldRequest(b *testing.B) {
	c := newTester(b)
	inst := c.CreateServer(b, false)
	b.Cleanup(func() { inst.Destroy(b) })
	nc := dialInstance(b, inst, nats.UseOldRequestStyle())

	ok := []byte("ok")
	nc.Subscribe("req", func(m *nats.Msg) {
		nc.Publish(m.Reply, ok)
	})
	b.ResetTimer()
	b.ReportAllocs()
	q := []byte("q")
	for range b.N {
		_, err := nc.Request("req", q, 1*time.Second)
		if err != nil {
			b.Fatalf("Err %v\n", err)
		}
	}
}

func BenchmarkPublishValidation(b *testing.B) {
	msgPayload := []byte("test")
	shortSubject := "foo.bar"                                      // 7 chars
	longSubject := "metrics.production.server01.cpu.usage.percent" // 45 chars

	run := func(name string, subject string, opts ...nats.Option) {
		b.Run(name, func(b *testing.B) {
			c := newTester(b)
			inst := c.CreateServer(b, false)
			b.Cleanup(func() { inst.Destroy(b) })
			nc := dialInstance(b, inst, opts...)

			b.ResetTimer()
			for b.Loop() {
				if err := nc.Publish(subject, msgPayload); err != nil {
					b.Fatalf("Error publishing message: %v", err)
				}
			}
			nc.Flush()
			b.StopTimer()
		})
	}

	run("skip validation, short subject", shortSubject, nats.SkipSubjectValidation())
	run("skip validation, long subject", longSubject, nats.SkipSubjectValidation())
	run("with validation, short subject", shortSubject)
	run("with validation, long subject", longSubject)
}

func BenchmarkPublishWithWriteBufferSize(b *testing.B) {
	payloads := []struct {
		name string
		size int
	}{
		{"16B", 16},
		{"128B", 128},
		{"512B", 512},
	}
	bufSizes := []int{512, 4096, 8192, 16384, 32768, 65536, 131072}

	for _, p := range payloads {
		msg := make([]byte, p.size)
		for i := range msg {
			msg[i] = byte(i)
		}
		for _, sz := range bufSizes {
			b.Run(fmt.Sprintf("payload_%s/buf_%d", p.name, sz), func(b *testing.B) {
				c := newTester(b)
				inst := c.CreateServer(b, false)
				b.Cleanup(func() { inst.Destroy(b) })
				nc := dialInstance(b, inst, nats.WriteBufferSize(sz))

				b.ResetTimer()
				for range b.N {
					if err := nc.Publish("foo", msg); err != nil {
						b.Fatalf("Error publishing: %v", err)
					}
				}
				b.StopTimer()
				nc.Flush()
			})
		}
	}
}
