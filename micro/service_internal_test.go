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

package micro

import "testing"

func TestMatchEndpointSubject(t *testing.T) {
	for _, test := range []struct {
		name            string
		endpointSubject string
		literalSubject  string
		match           bool
	}{
		{"exact", "foo.bar", "foo.bar", true},
		{"exact single token", "foo", "foo", true},
		{"single wildcard", "foo.*", "foo.bar", true},
		{"single wildcard mismatched literal", "foo.*", "foo.bar.baz", false},
		{"full wildcard", "foo.>", "foo.bar.baz", true},
		{"full wildcard one token", "foo.>", "foo.bar", true},
		{"literal mismatch", "foo.bar", "foo.baz", false},
		{"endpoint longer than subject", "foo.bar", "foo", false},
		// A shorter literal endpoint must not match a longer subject.
		{"prefix over-match single token", "foo", "foo.bar", false},
		{"prefix over-match multi token", "foo.bar", "foo.bar.baz", false},
		{"wildcard prefix over-match", "foo.*", "foo.bar.baz", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := matchEndpointSubject(test.endpointSubject, test.literalSubject); got != test.match {
				t.Errorf("matchEndpointSubject(%q, %q) = %v; want %v",
					test.endpointSubject, test.literalSubject, got, test.match)
			}
		})
	}
}
