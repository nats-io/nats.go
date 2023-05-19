// Copyright 2022-2023 The NATS Authors
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

package test

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

func TestJetStreamErrors(t *testing.T) {
	t.Run("API error", func(t *testing.T) {
		conf := createConfFile(t, []byte(`
			listen: 127.0.0.1:-1
			no_auth_user: rip
			jetstream: {max_mem_store: 64GB, max_file_store: 10TB}
			accounts: {
				JS: {
					jetstream: enabled
					users: [ {user: dlc, password: foo} ]
				},
				IU: {
					users: [ {user: rip, password: bar} ]
				},
			}
		`))
		defer os.Remove(conf)

		s, _ := RunServerWithConfig(conf)
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		_, err = js.AccountInfo(ctx)
		// check directly to var (backwards compatible)
		if err != jetstream.ErrJetStreamNotEnabledForAccount {
			t.Fatalf("Did not get the proper error, got %v", err)
		}

		// matching via errors.Is
		if ok := errors.Is(err, jetstream.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected jetstream.ErrJetStreamNotEnabledForAccount")
		}

		// matching wrapped via error.Is
		err2 := fmt.Errorf("custom error: %w", jetstream.ErrJetStreamNotEnabledForAccount)
		if ok := errors.Is(err2, jetstream.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected wrapped ErrJetStreamNotEnabled")
		}

		// via classic type assertion.
		jserr, ok := err.(jetstream.JetStreamError)
		if !ok {
			t.Fatal("Expected a jetstream.JetStreamError")
		}
		expected := jetstream.JSErrCodeJetStreamNotEnabledForAccount
		if jserr.APIError().ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, jserr.APIError().ErrorCode)
		}
		if jserr.APIError() == nil {
			t.Fatal("Expected APIError")
		}

		// matching to interface via errors.As(...)
		var apierr jetstream.JetStreamError
		ok = errors.As(err, &apierr)
		if !ok {
			t.Fatal("Expected a jetstream.JetStreamError")
		}
		if apierr.APIError() == nil {
			t.Fatal("Expected APIError")
		}
		if apierr.APIError().ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, apierr.APIError().ErrorCode)
		}
		expectedMessage := "nats: API error: code=503 err_code=10039 description=jetstream not enabled for account"
		if apierr.Error() != expectedMessage {
			t.Fatalf("Expected: %v, got: %v", expectedMessage, apierr.Error())
		}

		// an APIError also implements the jetstream.JetStreamError interface.
		var _ jetstream.JetStreamError = &jetstream.APIError{}

		// matching arbitrary custom error via errors.Is(...)
		customErr := &jetstream.APIError{ErrorCode: expected}
		if ok := errors.Is(customErr, jetstream.ErrJetStreamNotEnabledForAccount); !ok {
			t.Fatal("Expected wrapped jetstream.ErrJetStreamNotEnabledForAccount")
		}
		customErr = &jetstream.APIError{ErrorCode: 1}
		if ok := errors.Is(customErr, jetstream.ErrJetStreamNotEnabledForAccount); ok {
			t.Fatal("Expected to not match ErrJetStreamNotEnabled")
		}
		var cerr jetstream.JetStreamError
		if ok := errors.As(customErr, &cerr); !ok {
			t.Fatal("Expected custom error to be a jetstream.JetStreamError")
		}

		// matching to concrete type via errors.As(...)
		var aerr *jetstream.APIError
		ok = errors.As(err, &aerr)
		if !ok {
			t.Fatal("Expected an APIError")
		}
		if aerr.ErrorCode != expected {
			t.Fatalf("Expected: %v, got: %v", expected, aerr.ErrorCode)
		}
		expectedMessage = "nats: API error: code=503 err_code=10039 description=jetstream not enabled for account"
		if aerr.Error() != expectedMessage {
			t.Fatalf("Expected: %v, got: %v", expectedMessage, apierr.Error())
		}
	})

	t.Run("test non-api error", func(t *testing.T) {
		s := RunBasicJetStreamServer()
		defer shutdownJSServerAndRemoveStorage(t, s)

		nc, err := nats.Connect(s.ClientURL())
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		js, err := jetstream.New(nc)
		if err != nil {
			t.Fatalf("Unexpected error: %v", err)
		}
		defer nc.Close()

		// stream with empty name
		_, err = js.CreateStream(ctx, jetstream.StreamConfig{})
		if err == nil {
			t.Fatalf("Expected error, got nil")
		}

		// check directly to var (backwards compatible)
		if err != jetstream.ErrStreamNameRequired {
			t.Fatalf("Expected: %v; got: %v", jetstream.ErrInvalidStreamName, err)
		}

		// matching via errors.Is
		if ok := errors.Is(err, jetstream.ErrStreamNameRequired); !ok {
			t.Fatalf("Expected: %v; got: %v", jetstream.ErrStreamNameRequired, err)
		}

		// matching wrapped via error.Is
		err2 := fmt.Errorf("custom error: %w", jetstream.ErrStreamNameRequired)
		if ok := errors.Is(err2, jetstream.ErrStreamNameRequired); !ok {
			t.Fatal("Expected wrapped ErrStreamNameRequired")
		}

		// via classic type assertion.
		jserr, ok := err.(jetstream.JetStreamError)
		if !ok {
			t.Fatal("Expected a jetstream.JetStreamError")
		}
		if jserr.APIError() != nil {
			t.Fatalf("Expected: empty APIError; got: %v", jserr.APIError())
		}

		// matching to interface via errors.As(...)
		var jserr2 jetstream.JetStreamError
		ok = errors.As(err, &jserr2)
		if !ok {
			t.Fatal("Expected a jetstream.JetStreamError")
		}
		if jserr2.APIError() != nil {
			t.Fatalf("Expected: empty APIError; got: %v", jserr2.APIError())
		}
		expectedMessage := "nats: stream name is required"
		if jserr2.Error() != expectedMessage {
			t.Fatalf("Expected: %v, got: %v", expectedMessage, jserr2.Error())
		}

		// matching to concrete type via errors.As(...)
		var aerr *jetstream.APIError
		ok = errors.As(err, &aerr)
		if ok {
			t.Fatal("Expected ErrStreamNameRequired not to map to APIError")
		}
	})

}
