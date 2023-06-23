// Copyright 2020-2022 The NATS Authors
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

package parser

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestParseNum(t *testing.T) {
	tests := []struct {
		name     string
		given    string
		expected uint64
	}{
		{
			name:     "parse 191817",
			given:    "191817",
			expected: 191817,
		},
		{
			name:     "parse 0",
			given:    "0",
			expected: 0,
		},
		{
			name:     "parse 00",
			given:    "00",
			expected: 0,
		},
		{
			name:     "empty string",
			given:    "",
			expected: 0,
		},
		{
			name:     "negative number",
			given:    "-123",
			expected: 0,
		},
		{
			name:     "not a number",
			given:    "abc",
			expected: 0,
		},
		{
			name:     "max uit64",
			given:    strconv.FormatUint(math.MaxUint64, 10),
			expected: math.MaxUint64,
		},
		{
			name:     "max uit64 + 2, overflow",
			given:    "18446744073709551617",
			expected: 1,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res := ParseNum(test.given)
			if res != test.expected {
				t.Fatalf("Invalid result; want: %d; got: %d", test.expected, res)
			}
		})
	}
}

func FuzzParseNum(f *testing.F) {
	testcases := []string{"191817", " ", "-123", "abc"}
	for _, tc := range testcases {
		f.Add(tc)
	}

	f.Fuzz(func(t *testing.T, given string) {
		given = strings.TrimLeft(given, "+")
		res := ParseNum(given)
		parsed, err := strconv.ParseUint(given, 10, 64)
		if err != nil && !errors.Is(err, strconv.ErrRange) {
			if res != 0 {
				t.Errorf("given: %s; expected: -1; got: %d; err: %v", given, res, err)
			}
		} else if err == nil && res != parsed {
			t.Errorf("given: %s; expected: %d; got: %d", given, parsed, res)
		}
	})
}

func TestGetMetadataFields(t *testing.T) {
	tests := []struct {
		name      string
		subject   string
		expected  []string
		withError error
	}{
		{
			name:     "parse v2 tokens",
			subject:  "$JS.ACK.domain.hash-123.stream.cons.100.200.150.123456789.100.token",
			expected: []string{"$JS", "ACK", "domain", "hash-123", "stream", "cons", "100", "200", "150", "123456789", "100", "token"},
		},
		{
			name:     "parse v2 tokens with underscore domain",
			subject:  "$JS.ACK._.hash-123.stream.cons.100.200.150.123456789.100.token",
			expected: []string{"$JS", "ACK", "", "hash-123", "stream", "cons", "100", "200", "150", "123456789", "100", "token"},
		},
		{
			name:     "parse v1 tokens",
			subject:  "$JS.ACK.stream.cons.100.200.150.123456789.100",
			expected: []string{"$JS", "ACK", "", "", "stream", "cons", "100", "200", "150", "123456789", "100"},
		},
		{
			name:      "invalid start of subject",
			subject:   "$ABC.123.stream.cons.100.200.150.123456789.100",
			withError: ErrInvalidSubjectFormat,
		},
		{
			name:      "invalid subject length (10)",
			subject:   "$JS.ACK.stream.cons.100.200.150.123456789.100.ABC",
			withError: ErrInvalidSubjectFormat,
		},
		{
			name:      "invalid subject length (5)",
			subject:   "$JS.ACK.stream.cons.100",
			withError: ErrInvalidSubjectFormat,
		},
		{
			name:      "invalid subject ",
			subject:   "",
			withError: ErrInvalidSubjectFormat,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			res, err := GetMetadataFields(test.subject)
			if test.withError != nil {
				if err == nil || !errors.Is(err, test.withError) {
					t.Fatalf("Expected error: %v; got: %v", test.withError, err)
				}
				return
			}
			if !reflect.DeepEqual(test.expected, res) {
				t.Fatalf("Invalid result; want: %v; got: %v", test.expected, res)
			}
		})
	}
}
