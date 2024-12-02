// Copyright 2012-2024 The NATS Authors
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

//go:build go1.23
// +build go1.23

package test

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

func TestSubscribeIterator(t *testing.T) {
	t.Run("with timeout", func(t *testing.T) {
		s := RunServerOnPort(-1)
		defer s.Shutdown()

		nc, err := nats.Connect(s.ClientURL(), nats.PermissionErrOnSubscribe(true))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		sub, err := nc.SubscribeSync("foo")
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}
		defer sub.Unsubscribe()

		total := 100
		for i := 0; i < total/2; i++ {
			if err := nc.Publish("foo", []byte("Hello")); err != nil {
				t.Fatalf("Error on publish: %v", err)
			}
		}

		// publish some more messages asynchronously
		errCh := make(chan error, 1)
		go func() {
			for i := 0; i < total/2; i++ {
				if err := nc.Publish("foo", []byte("Hello")); err != nil {
					errCh <- err
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			close(errCh)
		}()

		received := 0
		for _, err := range sub.MsgsTimeout(100 * time.Millisecond) {
			if err != nil {
				if !errors.Is(err, nats.ErrTimeout) {
					t.Fatalf("Error on subscribe: %v", err)
				}
				break
			} else {
				received++
			}
		}
		if received != total {
			t.Fatalf("Expected %d messages, got %d", total, received)
		}
	})

	t.Run("no timeout", func(t *testing.T) {
		s := RunServerOnPort(-1)
		defer s.Shutdown()

		nc, err := nats.Connect(s.ClientURL(), nats.PermissionErrOnSubscribe(true))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		sub, err := nc.SubscribeSync("foo")
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}
		defer sub.Unsubscribe()

		// Send some messages to ourselves.
		total := 100
		for i := 0; i < total/2; i++ {
			if err := nc.Publish("foo", []byte("Hello")); err != nil {
				t.Fatalf("Error on publish: %v", err)
			}
		}

		received := 0

		// publish some more messages asynchronously
		errCh := make(chan error, 1)
		go func() {
			for i := 0; i < total/2; i++ {
				if err := nc.Publish("foo", []byte("Hello")); err != nil {
					errCh <- err
					return
				}
				time.Sleep(10 * time.Millisecond)
			}
			close(errCh)
		}()

		for _, err := range sub.Msgs() {
			if err != nil {
				t.Fatalf("Error getting msg: %v", err)
			}
			received++
			if received >= total {
				break
			}
		}
		err = <-errCh
		if err != nil {
			t.Fatalf("Error on publish: %v", err)
		}
		_, err = sub.NextMsg(100 * time.Millisecond)
		if !errors.Is(err, nats.ErrTimeout) {
			t.Fatalf("Expected timeout waiting for next message, got %v", err)
		}
	})

	t.Run("permissions violation", func(t *testing.T) {
		conf := createConfFile(t, []byte(`
			listen: 127.0.0.1:-1
			authorization: {
				users = [
					{
						user: test
						password: test
						permissions: {
							subscribe: {
								deny: "foo"
							}
						}
					}
				]
			}
		`))
		defer os.Remove(conf)

		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()

		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("test", "test"), nats.PermissionErrOnSubscribe(true))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		sub, err := nc.SubscribeSync("foo")
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		defer sub.Unsubscribe()

		errs := make(chan error)
		go func() {
			var err error
			for _, err = range sub.Msgs() {
				break
			}
			errs <- err
		}()

		select {
		case e := <-errs:
			if !errors.Is(e, nats.ErrPermissionViolation) {
				t.Fatalf("Expected permissions error, got %v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get the permission error")
		}

		_, err = sub.NextMsg(100 * time.Millisecond)
		if !errors.Is(err, nats.ErrPermissionViolation) {
			t.Fatalf("Expected permissions violation error, got %v", err)
		}
	})

	t.Run("attempt iterator on async sub", func(t *testing.T) {
		s := RunServerOnPort(-1)
		defer s.Shutdown()

		nc, err := nats.Connect(s.ClientURL(), nats.PermissionErrOnSubscribe(true))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		sub, err := nc.Subscribe("foo", func(msg *nats.Msg) {})
		if err != nil {
			t.Fatal("Failed to subscribe: ", err)
		}
		defer sub.Unsubscribe()

		for _, err := range sub.MsgsTimeout(100 * time.Millisecond) {
			if !errors.Is(err, nats.ErrSyncSubRequired) {
				t.Fatalf("Error on subscribe: %v", err)
			}
		}
		for _, err := range sub.Msgs() {
			if !errors.Is(err, nats.ErrSyncSubRequired) {
				t.Fatalf("Error on subscribe: %v", err)
			}
		}
	})
}

func TestQueueSubscribeIterator(t *testing.T) {
	t.Run("basic", func(t *testing.T) {
		s := RunServerOnPort(-1)
		defer s.Shutdown()

		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		subs := make([]*nats.Subscription, 4)
		for i := 0; i < 4; i++ {
			sub, err := nc.QueueSubscribeSync("foo", "q")
			if err != nil {
				t.Fatal("Failed to subscribe: ", err)
			}
			subs[i] = sub
			defer sub.Unsubscribe()
		}

		// Send some messages to ourselves.
		total := 100
		for i := 0; i < total; i++ {
			if err := nc.Publish("foo", []byte(fmt.Sprintf("%d", i))); err != nil {
				t.Fatalf("Error on publish: %v", err)
			}
		}

		wg := sync.WaitGroup{}
		wg.Add(100)
		startWg := sync.WaitGroup{}
		startWg.Add(4)

		for i := range subs {
			go func(i int) {
				startWg.Done()
				for _, err := range subs[i].MsgsTimeout(100 * time.Millisecond) {
					if err != nil {
						break
					}
					wg.Done()
				}
			}(i)
		}

		startWg.Wait()

		wg.Wait()

		for _, sub := range subs {
			if _, err = sub.NextMsg(100 * time.Millisecond); !errors.Is(err, nats.ErrTimeout) {
				t.Fatalf("Expected timeout waiting for next message, got %v", err)
			}
		}
	})

	t.Run("permissions violation", func(t *testing.T) {
		conf := createConfFile(t, []byte(`
			listen: 127.0.0.1:-1
			authorization: {
				users = [
					{
						user: test
						password: test
						permissions: {
							subscribe: {
								deny: "foo"
							}
						}
					}
				]
			}
		`))
		defer os.Remove(conf)

		s, _ := RunServerWithConfig(conf)
		defer s.Shutdown()

		nc, err := nats.Connect(s.ClientURL(), nats.UserInfo("test", "test"), nats.PermissionErrOnSubscribe(true))
		if err != nil {
			t.Fatalf("Error on connect: %v", err)
		}
		defer nc.Close()

		sub, err := nc.QueueSubscribeSync("foo", "q")
		if err != nil {
			t.Fatalf("Error on subscribe: %v", err)
		}
		defer sub.Unsubscribe()

		errs := make(chan error)
		go func() {
			var err error
			for _, err = range sub.MsgsTimeout(2 * time.Second) {
				break
			}
			errs <- err
		}()

		select {
		case e := <-errs:
			if !errors.Is(e, nats.ErrPermissionViolation) {
				t.Fatalf("Expected permissions error, got %v", e)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("Did not get the permission error")
		}
	})
}
