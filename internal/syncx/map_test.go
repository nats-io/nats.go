// Copyright 2024 The NATS Authors
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

package syncx

import (
	"testing"
)

func TestMapLoad(t *testing.T) {
	var m Map[int, string]
	m.Store(1, "one")

	v, ok := m.Load(1)
	if !ok || v != "one" {
		t.Errorf("Load(1) = %v, %v; want 'one', true", v, ok)
	}

	v, ok = m.Load(2)
	if ok || v != "" {
		t.Errorf("Load(2) = %v, %v; want '', false", v, ok)
	}
}

func TestMapStore(t *testing.T) {
	var m Map[int, string]
	m.Store(1, "one")

	v, ok := m.Load(1)
	if !ok || v != "one" {
		t.Errorf("Load(1) after Store(1, 'one') = %v, %v; want 'one', true", v, ok)
	}
}

func TestMapDelete(t *testing.T) {
	var m Map[int, string]
	m.Store(1, "one")
	m.Delete(1)

	v, ok := m.Load(1)
	if ok || v != "" {
		t.Errorf("Load(1) after Delete(1) = %v, %v; want '', false", v, ok)
	}
}

func TestMapRange(t *testing.T) {
	var m Map[int, string]
	m.Store(1, "one")
	m.Store(2, "two")

	var keys []int
	var values []string
	m.Range(func(key int, value string) bool {
		keys = append(keys, key)
		values = append(values, value)
		return true
	})

	if len(keys) != 2 || len(values) != 2 {
		t.Errorf("Range() keys = %v, values = %v; want 2 keys and 2 values", keys, values)
	}
}

func TestMapLoadOrStore(t *testing.T) {
	var m Map[int, string]

	v, loaded := m.LoadOrStore(1, "one")
	if loaded || v != "one" {
		t.Errorf("LoadOrStore(1, 'one') = %v, %v; want 'one', false", v, loaded)
	}

	v, loaded = m.LoadOrStore(1, "uno")
	if !loaded || v != "one" {
		t.Errorf("LoadOrStore(1, 'uno') = %v, %v; want 'one', true", v, loaded)
	}
}

func TestMapLoadAndDelete(t *testing.T) {
	var m Map[int, string]
	m.Store(1, "one")

	v, ok := m.LoadAndDelete(1)
	if !ok || v != "one" {
		t.Errorf("LoadAndDelete(1) = %v, %v; want 'one', true", v, ok)
	}

	v, ok = m.Load(1)
	if ok || v != "" {
		t.Errorf("Load(1) after LoadAndDelete(1) = %v, %v; want '', false", v, ok)
	}

	// Test that LoadAndDelete on a missing key returns the zero value.
	v, ok = m.LoadAndDelete(2)
	if ok || v != "" {
		t.Errorf("LoadAndDelete(2) = %v, %v; want '', false", v, ok)
	}
}

func TestMapCompareAndSwap(t *testing.T) {
	var m Map[int, string]
	m.Store(1, "one")

	ok := m.CompareAndSwap(1, "one", "uno")
	if !ok {
		t.Errorf("CompareAndSwap(1, 'one', 'uno') = false; want true")
	}

	v, _ := m.Load(1)
	if v != "uno" {
		t.Errorf("Load(1) after CompareAndSwap = %v; want 'uno'", v)
	}
}

func TestMapCompareAndDelete(t *testing.T) {
	var m Map[int, string]
	m.Store(1, "one")

	ok := m.CompareAndDelete(1, "one")
	if !ok {
		t.Errorf("CompareAndDelete(1, 'one') = false; want true")
	}

	v, _ := m.Load(1)
	if v != "" {
		t.Errorf("Load(1) after CompareAndDelete = %v; want ''", v)
	}
}

func TestMapSwap(t *testing.T) {
	var m Map[int, string]
	m.Store(1, "one")

	v, loaded := m.Swap(1, "uno")
	if !loaded || v != "one" {
		t.Errorf("Swap(1, 'uno') = %v, %v; want 'one', true", v, loaded)
	}

	v, _ = m.Load(1)
	if v != "uno" {
		t.Errorf("Load(1) after Swap = %v; want 'uno'", v)
	}
}
