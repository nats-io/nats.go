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
	"strings"
	"testing"
	"time"
)

func TestOrderedPushConsumerConfig(t *testing.T) {
	t.Run("defaults applied on first create", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{NamePrefix: "p"})
		c.deliverSubject = "inbox.x"
		cfg := c.consumerConfig()

		if cfg.Name != "p_1" {
			t.Errorf("expected Name=p_1, got %q", cfg.Name)
		}
		if cfg.AckPolicy != AckNonePolicy {
			t.Errorf("expected AckPolicy=AckNone, got %v", cfg.AckPolicy)
		}
		if cfg.MaxDeliver != 1 {
			t.Errorf("expected MaxDeliver=1, got %d", cfg.MaxDeliver)
		}
		if !cfg.MemoryStorage {
			t.Error("expected MemoryStorage=true")
		}
		if cfg.Replicas != 1 {
			t.Errorf("expected Replicas=1, got %d", cfg.Replicas)
		}
		if !cfg.FlowControl {
			t.Error("expected FlowControl=true")
		}
		if cfg.IdleHeartbeat != defaultOrderedPushHeartbeat {
			t.Errorf("expected default IdleHeartbeat=%v, got %v", defaultOrderedPushHeartbeat, cfg.IdleHeartbeat)
		}
		if cfg.DeliverSubject != "inbox.x" {
			t.Errorf("expected DeliverSubject=inbox.x, got %q", cfg.DeliverSubject)
		}
		if cfg.InactiveThreshold != 5*time.Minute {
			t.Errorf("expected default InactiveThreshold=5m, got %v", cfg.InactiveThreshold)
		}
		// First create with DeliverPolicy zero-value (DeliverAllPolicy) should
		// honor it and clear OptStartSeq.
		if cfg.DeliverPolicy != DeliverAllPolicy {
			t.Errorf("expected DeliverAllPolicy on first create, got %v", cfg.DeliverPolicy)
		}
		if cfg.OptStartSeq != 0 {
			t.Errorf("expected OptStartSeq=0 with DeliverAllPolicy, got %d", cfg.OptStartSeq)
		}
	})

	t.Run("NamePrefix produces sequenced names", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{NamePrefix: "watcher"})
		c.deliverSubject = "x"
		for i, want := range []string{"watcher_1", "watcher_2", "watcher_3"} {
			cfg := c.consumerConfig()
			if cfg.Name != want {
				t.Errorf("iteration %d: expected Name=%s, got %s", i, want, cfg.Name)
			}
		}
	})

	t.Run("missing NamePrefix uses NUID", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{})
		c.deliverSubject = "x"
		cfg := c.consumerConfig()
		// NUID is 22 chars; full name "<22>_1" is 24 chars.
		if !strings.HasSuffix(cfg.Name, "_1") || len(cfg.Name) != 24 {
			t.Errorf("expected NUID_1 naming, got %q (len=%d)", cfg.Name, len(cfg.Name))
		}
	})

	t.Run("FilterSubjects pass-through", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{
			NamePrefix:     "p",
			FilterSubjects: []string{"a.>", "b.>"},
		})
		c.deliverSubject = "x"
		cfg := c.consumerConfig()
		if cfg.FilterSubject != "" {
			t.Errorf("expected empty FilterSubject for multi-subject, got %q", cfg.FilterSubject)
		}
		if len(cfg.FilterSubjects) != 2 || cfg.FilterSubjects[0] != "a.>" || cfg.FilterSubjects[1] != "b.>" {
			t.Errorf("expected FilterSubjects=[a.> b.>], got %v", cfg.FilterSubjects)
		}
	})

	t.Run("single FilterSubject collapses to FilterSubject", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{
			NamePrefix:     "p",
			FilterSubjects: []string{"only.one"},
		})
		c.deliverSubject = "x"
		cfg := c.consumerConfig()
		if cfg.FilterSubject != "only.one" {
			t.Errorf("expected FilterSubject=only.one, got %q", cfg.FilterSubject)
		}
		if len(cfg.FilterSubjects) != 0 {
			t.Errorf("expected empty FilterSubjects, got %v", cfg.FilterSubjects)
		}
	})

	t.Run("cursor advance overrides DeliverPolicy on recreate", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{
			NamePrefix:    "p",
			DeliverPolicy: DeliverNewPolicy,
		})
		c.deliverSubject = "x"
		c.cursor.streamSeq = 42
		cfg := c.consumerConfig()
		if cfg.DeliverPolicy != DeliverByStartSequencePolicy {
			t.Errorf("expected DeliverByStartSequencePolicy on recreate, got %v", cfg.DeliverPolicy)
		}
		if cfg.OptStartSeq != 43 {
			t.Errorf("expected OptStartSeq=43, got %d", cfg.OptStartSeq)
		}
	})

	t.Run("OptStartSeq honored on first create with sequence policy", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{
			NamePrefix:    "p",
			DeliverPolicy: DeliverByStartSequencePolicy,
			OptStartSeq:   100,
		})
		c.deliverSubject = "x"
		cfg := c.consumerConfig()
		if cfg.OptStartSeq != 100 {
			t.Errorf("expected OptStartSeq=100 on first create, got %d", cfg.OptStartSeq)
		}
	})

	t.Run("custom IdleHeartbeat honored", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{
			NamePrefix:    "p",
			IdleHeartbeat: 250 * time.Millisecond,
		})
		c.deliverSubject = "x"
		cfg := c.consumerConfig()
		if cfg.IdleHeartbeat != 250*time.Millisecond {
			t.Errorf("expected IdleHeartbeat=250ms, got %v", cfg.IdleHeartbeat)
		}
	})

	t.Run("custom InactiveThreshold honored", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{
			NamePrefix:        "p",
			InactiveThreshold: 30 * time.Second,
		})
		c.deliverSubject = "x"
		cfg := c.consumerConfig()
		if cfg.InactiveThreshold != 30*time.Second {
			t.Errorf("expected InactiveThreshold=30s, got %v", cfg.InactiveThreshold)
		}
	})

	t.Run("DeliverLastPerSubject without filter sets filter to >", func(t *testing.T) {
		c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{
			NamePrefix:    "p",
			DeliverPolicy: DeliverLastPerSubjectPolicy,
		})
		c.deliverSubject = "x"
		cfg := c.consumerConfig()
		if len(cfg.FilterSubjects) != 1 || cfg.FilterSubjects[0] != ">" {
			t.Errorf("expected FilterSubjects=[>], got %v", cfg.FilterSubjects)
		}
	})
}

func TestOrderedPushConsumerValidateConfig(t *testing.T) {
	c := newOrderedPushConsumer(nil, "S", OrderedPushConsumerConfig{
		OptStartSeq:  10,
		OptStartTime: timePtr(),
	})
	if err := c.validateConfig(); err == nil {
		t.Fatal("expected error for OptStartSeq+OptStartTime, got nil")
	}
}

func timePtr() *time.Time {
	t := time.Now()
	return &t
}
