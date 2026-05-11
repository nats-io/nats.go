// Copyright 2012-2025 The NATS Authors
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

//go:build tinygo

package nats

import (
	"sync"
	"time"
)

// natsRand is a minimal LCG pseudo-random number generator.
// Uses Knuth's multiplicative hashing constants.
type natsRand struct {
	mu    sync.Mutex
	state uint64
}

func natsNewRand() *natsRand {
	return &natsRand{state: uint64(time.Now().UnixNano())}
}

func (r *natsRand) next() uint64 {
	r.mu.Lock()
	r.state = r.state*6364136223846793005 + 1442695040888963407
	v := r.state
	r.mu.Unlock()
	return v
}

// Int63 returns a non-negative pseudo-random int64.
func (r *natsRand) Int63() int64 {
	return int64(r.next() >> 1)
}

// Intn returns a non-negative pseudo-random int in [0,n).
func (r *natsRand) Intn(n int) int {
	if n <= 0 {
		return 0
	}
	return int(r.next() % uint64(n))
}

var _globalRand = natsNewRand()

func natsRandShuffle(n int, swap func(i, j int)) {
	for i := n - 1; i > 0; i-- {
		j := _globalRand.Intn(i + 1)
		swap(i, j)
	}
}

func natsRandInt63n(n int64) int64 {
	if n <= 0 {
		return 0
	}
	return int64(_globalRand.next() % uint64(n))
}
