//go:build compat
// +build compat

package test

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
)

type objectRequest struct {
	Config *nats.ObjectMeta `json:"config"`
	URL    string           `json:"url"`
	Bucket string           `json:"bucket"`
}

func TestObjectStoreCompatibility(t *testing.T) {
	nc, err := nats.Connect("demo.nats.io")
	if err != nil {
		t.Fatalf("Error connecting to NATS: %v", err)
	}
	defer nc.Close()
	sub, err := nc.SubscribeSync("tests.object_store.>")
	if err != nil {
		t.Fatalf("Error subscribing to test subject: %v", err)
	}
	defer sub.Unsubscribe()
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
	if err := init.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
	custom, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	var cfg nats.ObjectStoreConfig
	if err := json.Unmarshal(custom.Data, &cfg); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	fmt.Printf("%+v\n", cfg)

	os, err = js.CreateObjectStore(&cfg)
	if err != nil {
		t.Fatalf("Error creating object store: %v", err)
	}
	if err := custom.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}

	objReq, err := sub.NextMsg(5 * time.Second)
	if err != nil {
		t.Fatalf("Error getting message: %v", err)
	}
	var req objectRequest
	if err := json.Unmarshal(objReq.Data, &req); err != nil {
		t.Fatalf("Error unmarshalling message: %v", err)
	}
	os, err = js.ObjectStore(req.Bucket)
	if err != nil {
		t.Fatalf("Error getting object store: %v", err)
	}
	client := http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{DisableKeepAlives: true}}
	resp, err := client.Get(req.URL)
	if err != nil {
		t.Fatalf("Error getting content: %v", err)
	}
	defer resp.Body.Close()
	if _, err := os.Put(req.Config, resp.Body); err != nil {
		t.Fatalf("Error putting object: %v", err)
	}
	if err := objReq.Respond(nil); err != nil {
		t.Fatalf("Error responding to message: %v", err)
	}
}
