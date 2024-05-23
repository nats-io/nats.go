// Copyright 2023 The NATS Authors
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

//go:build compat
// +build compat

package test

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/nats-io/nats.go/micro"
)

type objectStepConfig[T any] struct {
	Suite   string `json:"suite"`
	Test    string `json:"test"`
	Command string `json:"command"`
	URL     string `json:"url"`
	Bucket  string `json:"bucket"`
	Object  string `json:"object"`
	Config  T      `json:"config"`
}

func TestCompatibilityObjectStoreDefaultBucket(t *testing.T) {
	t.Parallel()
	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.default-bucket.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	// 1. Create default bucket
	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	ctx := context.Background()

	_, err = js.CreateObjectStore(ctx, jetstream.ObjectStoreConfig{
		Bucket: "test",
	})
	if err != nil {
		t.Fatalf("Error creating object store: %v", err)
	}
	// send empty response to indicate client is done
	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateTestResult(t, sub)
}

func TestCompatibilityObjectStoreCustomBucket(t *testing.T) {
	t.Parallel()
	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	ctx := context.Background()

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.custom-bucket.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	// 1. Create custom bucket
	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	var cfg objectStepConfig[jetstream.ObjectStoreConfig]
	if err := json.Unmarshal(msg.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}

	_, err = js.CreateObjectStore(ctx, cfg.Config)
	if err != nil {
		t.Fatalf("Error creating object store: %v", err)
	}
	// send empty response to indicate client is done
	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}

	validateTestResult(t, sub)
}

func TestCompatibilityObjectStoreGetObject(t *testing.T) {
	t.Parallel()
	type config struct {
		Bucket string `json:"bucket"`
		Object string `json:"object"`
	}

	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	ctx := context.Background()

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.get-object.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	var cfg config
	if err := json.Unmarshal(msg.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	// Get object
	os, err := js.ObjectStore(ctx, cfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	obj, err := os.Get(ctx, cfg.Object)
	if err != nil {
		t.Fatalf("Error creating object store: %v", err)
	}
	data, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("Error reading object: %v", err)
	}

	// calculate sha256 of the object
	h := sha256.New()
	h.Write(data)
	sha := h.Sum(nil)

	// send response to indicate client is done
	if err := msg.Respond(sha); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateTestResult(t, sub)
}

func TestCompatibilityObjectStorePutObject(t *testing.T) {
	t.Parallel()

	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	ctx := context.Background()

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.put-object.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	// Put object
	var putObjectCfg objectStepConfig[jetstream.ObjectMeta]
	if err := json.Unmarshal(msg.Data, &putObjectCfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err := js.ObjectStore(ctx, putObjectCfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	client := http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(putObjectCfg.URL)
	if err != nil {
		t.Fatalf("Error getting content: %v", err)
	}
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("Error reading content: %v", err)
	}
	defer resp.Body.Close()
	if _, err := os.Put(ctx, putObjectCfg.Config, bytes.NewBuffer(data)); err != nil {
		t.Fatalf("Error putting object: %v", err)
	}
	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateTestResult(t, sub)
}

func TestCompatibilityObjectStoreUpdateMetadata(t *testing.T) {
	t.Parallel()

	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	ctx := context.Background()

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.update-metadata.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	// Update object metadata
	var putObjectCfg objectStepConfig[jetstream.ObjectMeta]
	if err := json.Unmarshal(msg.Data, &putObjectCfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err := js.ObjectStore(ctx, putObjectCfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	if err := os.UpdateMeta(ctx, putObjectCfg.Object, putObjectCfg.Config); err != nil {
		t.Fatalf("Error putting object: %v", err)
	}
	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateTestResult(t, sub)
}

func TestCompatibilityObjectStoreWatch(t *testing.T) {
	t.Parallel()

	type config struct {
		Bucket string `json:"bucket"`
		Object string `json:"object"`
	}

	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	ctx := context.Background()

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.watch.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	// Watch object
	var cfg config
	if err := json.Unmarshal(msg.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err := js.ObjectStore(ctx, cfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	watcher, err := os.Watch(ctx)
	if err != nil {
		t.Fatalf("Error getting watcher: %v", err)
	}
	var digests []string
	var info *jetstream.ObjectInfo

	// get the initial value
	select {
	case info = <-watcher.Updates():
		digests = append(digests, info.Digest)
	case <-time.After(30 * time.Second):
		t.Fatalf("Timeout waiting for object update")
	}

	// init done, should receive nil
	select {
	case info = <-watcher.Updates():
		if info != nil {
			t.Fatalf("Expected nil, got: %v", info)
		}
	case <-time.After(30 * time.Second):
		t.Fatalf("Timeout waiting for object update")
	}

	// get the updated value
	select {
	case info = <-watcher.Updates():
		digests = append(digests, info.Digest)
	case <-time.After(30 * time.Second):
		t.Fatalf("Timeout waiting for object update")
	}

	if err := msg.Respond([]byte(strings.Join(digests, ","))); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateTestResult(t, sub)
}

func TestCompatibilityObjectStoreWatchUpdates(t *testing.T) {
	t.Parallel()

	type config struct {
		Bucket string `json:"bucket"`
		Object string `json:"object"`
	}

	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	ctx := context.Background()

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.watch-updates.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	// Watch object
	var cfg config
	if err := json.Unmarshal(msg.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err := js.ObjectStore(ctx, cfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	watcher, err := os.Watch(ctx, jetstream.UpdatesOnly())
	if err != nil {
		t.Fatalf("Error getting watcher: %v", err)
	}
	var info *jetstream.ObjectInfo
	select {
	case info = <-watcher.Updates():
	case <-time.After(30 * time.Second):
		t.Fatalf("Timeout waiting for object update")
	}

	if err := msg.Respond([]byte(info.Digest)); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateTestResult(t, sub)
}

func TestCompatibilityObjectStoreGetLink(t *testing.T) {
	t.Parallel()

	type config struct {
		Bucket string `json:"bucket"`
		Object string `json:"object"`
	}

	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	ctx := context.Background()
	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.get-link.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	// Watch object
	var cfg config
	if err := json.Unmarshal(msg.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err := js.ObjectStore(ctx, cfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	obj, err := os.Get(ctx, cfg.Object)
	if err != nil {
		t.Fatalf("Error getting object: %v", err)
	}

	data, err := io.ReadAll(obj)
	if err != nil {
		t.Fatalf("Error reading object: %v", err)
	}
	// calculate sha256 of the object
	h := sha256.New()
	h.Write(data)
	sha := h.Sum(nil)

	if err := msg.Respond(sha); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}

	validateTestResult(t, sub)
}

func TestCompatibilityObjectStorePutLink(t *testing.T) {
	t.Parallel()

	type config struct {
		Bucket   string `json:"bucket"`
		Object   string `json:"object"`
		LinkName string `json:"link_name"`
	}

	nc := connect(t)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	ctx := context.Background()
	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object-store.put-link.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	// Watch object
	var cfg config
	if err := json.Unmarshal(msg.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err := js.ObjectStore(ctx, cfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	sourceObj, err := os.GetInfo(ctx, cfg.Object)
	if err != nil {
		t.Fatalf("Error getting object: %v", err)
	}
	_, err = os.AddLink(ctx, cfg.LinkName, sourceObj)
	if err != nil {
		t.Fatalf("Error adding link: %v", err)
	}

	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}

	validateTestResult(t, sub)
}

func validateTestResult(t *testing.T, sub *nats.Subscription) {
	t.Helper()
	stepEnd, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	if strings.Contains(string(stepEnd.Subject), "fail") {
		t.Fatalf("Test step failed: %v", string(stepEnd.Subject))
	}
}

func connect(t *testing.T) *nats.Conn {
	t.Helper()
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL, nats.Timeout(1*time.Hour))
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	return nc
}

func connectJS(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
	nc := connect(t)
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Error getting JetStream context: %v", err)
	}
	return nc, js
}

type serviceStepConfig[T any] struct {
	Suite   string `json:"suite"`
	Test    string `json:"test"`
	Command string `json:"command"`
	Config  T      `json:"config"`
}

func TestTestCompatibilityService(t *testing.T) {
	t.Parallel()
	nc := connect(t)
	defer nc.Close()

	type groupConfig struct {
		Name       string `json:"name"`
		QueueGroup string `json:"queue_group"`
	}

	type endpointConfig struct {
		micro.EndpointConfig
		Name  string `json:"name"`
		Group string `json:"group"`
	}

	type config struct {
		micro.Config
		Groups    []groupConfig    `json:"groups"`
		Endpoints []endpointConfig `json:"endpoints"`
	}

	echoHandler := micro.HandlerFunc(func(req micro.Request) {
		req.Respond(req.Data())
	})

	errHandler := micro.HandlerFunc(func(req micro.Request) {
		req.Error("500", "handler error", nil)
	})

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.service.core.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	// 1. Get service and endpoint configs
	msg, err := sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	var cfg serviceStepConfig[*config]
	if err := json.Unmarshal(msg.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}

	var services []micro.Service
	svcCfg := cfg.Config
	svcCfg.StatsHandler = func(e *micro.Endpoint) any {
		return map[string]string{"endpoint": e.Name}
	}
	svc, err := micro.AddService(nc, svcCfg.Config)
	if err != nil {
		t.Fatalf("Error adding service: %v", err)
	}
	groups := make(map[string]micro.Group)
	for _, groupCfg := range svcCfg.Groups {
		opts := []micro.GroupOpt{}
		if groupCfg.QueueGroup != "" {
			opts = append(opts, micro.WithGroupQueueGroup(groupCfg.QueueGroup))
		}
		groups[groupCfg.Name] = svc.AddGroup(groupCfg.Name, opts...)
	}
	for _, endpointCfg := range svcCfg.Endpoints {
		opts := []micro.EndpointOpt{
			micro.WithEndpointSubject(endpointCfg.Subject),
		}
		if endpointCfg.QueueGroup != "" {
			opts = append(opts, micro.WithEndpointQueueGroup(endpointCfg.QueueGroup))
		}
		if endpointCfg.Metadata != nil {
			opts = append(opts, micro.WithEndpointMetadata(endpointCfg.Metadata))
		}
		handler := echoHandler
		if endpointCfg.Name == "faulty" {
			handler = errHandler
		}
		if endpointCfg.Group != "" {
			g := groups[endpointCfg.Group]
			if g == nil {
				t.Fatalf("Group %q not found", endpointCfg.Group)
			}
			if err := g.AddEndpoint(endpointCfg.Name, handler, opts...); err != nil {
				t.Fatalf("Error adding endpoint: %v", err)
			}
		} else {
			if err := svc.AddEndpoint(endpointCfg.Name, handler, opts...); err != nil {
				t.Fatalf("Error adding endpoint: %v", err)
			}
		}
	}
	services = append(services, svc)

	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}

	// 2. Stop services
	msg, err = sub.NextMsg(1 * time.Hour)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	for _, svc := range services {
		svc.Stop()
	}
	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}

	validateTestResult(t, sub)
}
