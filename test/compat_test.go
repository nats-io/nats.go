//go:build compat
// +build compat

package test

import (
	"encoding/json"
	"net/http"
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
	Config  T      `json:"config"`
}

func TestObjectStoreCompatibility(t *testing.T) {
	nc, err := nats.Connect("demo.nats.io")
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()

	// setup subscription on which tester will be sending requests
	sub, err := nc.SubscribeSync("tests.object_store.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()

	// 1. Create default bucket
	init, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		t.Fatalf("Error getting JetStream context: %v", err)
	}
	os, err := js.CreateObjectStore(&nats.ObjectStoreConfig{
		Bucket: "test",
	})
	if err != nil {
		t.Fatalf("Error creating object store: %v", err)
	}
	// send empty response to indicate client is done
	if err := init.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateStepResult(t, sub)

	// 2. Create bucket with custom config
	custom, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	var cfg objectStepConfig[*nats.ObjectStoreConfig]
	if err := json.Unmarshal(custom.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}

	os, err = js.CreateObjectStore(cfg.Config)
	if err != nil {
		t.Fatalf("Error creating object store: %v", err)
	}
	if err := custom.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateStepResult(t, sub)

	// 3. Put object
	objReq, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	var putObjectCfg objectStepConfig[*nats.ObjectMeta]
	if err := json.Unmarshal(objReq.Data, &putObjectCfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err = js.ObjectStore(putObjectCfg.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	client := http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(putObjectCfg.URL)
	if err != nil {
		t.Fatalf("Error getting content: %v", err)
	}
	defer resp.Body.Close()
	if _, err := os.Put(putObjectCfg.Config, resp.Body); err != nil {
		t.Fatalf("Error putting object: %v", err)
	}
	if err := objReq.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	validateStepResult(t, sub)
}

func validateStepResult(t *testing.T, sub *nats.Subscription) {
	t.Helper()
	stepEnd, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	if len(stepEnd.Header["STATUS"]) > 0 {
		t.Fatalf("Test step failed: %v", stepEnd.Header["STATUS"][0])
	}
}
