package test

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
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
	nc, js := connect(t)
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

	_, err = js.CreateObjectStore(&nats.ObjectStoreConfig{
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
	nc, js := connect(t)
	defer nc.Close()

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
	var cfg objectStepConfig[*nats.ObjectStoreConfig]
	if err := json.Unmarshal(msg.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}

	_, err = js.CreateObjectStore(cfg.Config)
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

	nc, js := connect(t)
	defer nc.Close()

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
	os, err := js.ObjectStore(cfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	obj, err := os.Get(cfg.Object)
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

	nc, js := connect(t)
	defer nc.Close()

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
	var putObjectCfg objectStepConfig[*nats.ObjectMeta]
	if err := json.Unmarshal(msg.Data, &putObjectCfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err := js.ObjectStore(putObjectCfg.Bucket)
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
	if _, err := os.Put(putObjectCfg.Config, bytes.NewBuffer(data)); err != nil {
		t.Fatalf("Error putting object: %v", err)
	}
	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateTestResult(t, sub)
}

func TestCompatibilityObjectStoreUpdateMetadata(t *testing.T) {
	t.Parallel()

	nc, js := connect(t)
	defer nc.Close()

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
	var putObjectCfg objectStepConfig[*nats.ObjectMeta]
	if err := json.Unmarshal(msg.Data, &putObjectCfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err := js.ObjectStore(putObjectCfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	if err := os.UpdateMeta(putObjectCfg.Object, putObjectCfg.Config); err != nil {
		t.Fatalf("Error putting object: %v", err)
	}
	if err := msg.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateTestResult(t, sub)
}

func TestCompatibilityObjectStoreWatch(t *testing.T) {
	t.Skip("Skipping test until watch behavior is sorted out in compatibility-tests")
	t.Parallel()

	type config struct {
		Bucket string `json:"bucket"`
		Object string `json:"object"`
	}

	nc, js := connect(t)
	defer nc.Close()

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
	os, err := js.ObjectStore(cfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	time.Sleep(5 * time.Second)
	watcher, err := os.Watch()
	if err != nil {
		t.Fatalf("Error getting watcher: %v", err)
	}
	var info *nats.ObjectInfo
	select {
	case info = <-watcher.Updates():
		fmt.Println(info.Digest)
	case <-time.After(30 * time.Second):
		t.Fatalf("Timeout waiting for object update")
	}

	if err := msg.Respond([]byte(info.Digest)); err != nil {
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

func connect(t *testing.T) (*nats.Conn, nats.JetStreamContext) {
	t.Helper()
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = nats.DefaultURL
	}
	nc, err := nats.Connect(natsURL, nats.Timeout(1*time.Hour))
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Error getting JetStream context: %v", err)
	}
	return nc, js
}
