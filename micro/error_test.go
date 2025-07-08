// Copyright 2025 The NATS Authors
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

import (
	"errors"
	"testing"
)

func TestNATSErrorUnwrap(t *testing.T) {
	baseErr := errors.New("base error")
	natsErr := &NATSError{
		Subject:     "test.subject",
		Description: "test description",
		err:         baseErr,
	}

	unwrapped := errors.Unwrap(natsErr)
	if unwrapped != baseErr {
		t.Errorf("Unwrap() = %v, want %v", unwrapped, baseErr)
	}

	// Test with nil underlying error
	natsErr2 := &NATSError{
		Subject:     "test.subject",
		Description: "test description",
	}
	if errors.Unwrap(natsErr2) != nil {
		t.Errorf("Unwrap() with nil err should return nil")
	}
}

func TestNATSErrorIs(t *testing.T) {
	natsErr1 := &NATSError{
		Subject:     "test.subject",
		Description: "test description",
	}

	natsErr2 := &NATSError{
		Subject:     "test.subject",
		Description: "test description",
	}

	natsErr3 := &NATSError{
		Subject:     "different.subject",
		Description: "test description",
	}

	natsErr4 := &NATSError{
		Subject:     "test.subject",
		Description: "different description",
	}

	// Test equality
	if !errors.Is(natsErr1, natsErr2) {
		t.Errorf("Is() = false, want true for equal errors")
	}

	// different subject
	if errors.Is(natsErr1, natsErr3) {
		t.Errorf("Is() = true, want false for different subjects")
	}

	// different description
	if errors.Is(natsErr1, natsErr4) {
		t.Errorf("Is() = true, want false for different descriptions")
	}

	// non NATSError
	otherErr := errors.New("other error")
	if errors.Is(natsErr1, otherErr) {
		t.Errorf("Is() = true, want false for non-NATSError")
	}
}

func TestNATSErrorWrapping(t *testing.T) {
	// Test error chain
	baseErr := errors.New("connection failed")
	natsErr := &NATSError{
		Subject:     "test.subject",
		Description: "service error: connection failed",
		err:         baseErr,
	}

	// Check that we can find the base error in the chain
	if !errors.Is(natsErr, baseErr) {
		t.Errorf("errors.Is() should find base error in chain")
	}

	// Test error message
	expectedMsg := `"test.subject": service error: connection failed`
	if natsErr.Error() != expectedMsg {
		t.Errorf("Error() = %q, want %q", natsErr.Error(), expectedMsg)
	}
}
