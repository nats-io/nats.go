// Copyright 2012 Apcera Inc. All rights reserved.

package nats

import (
	"encoding/json"
)

// A JSON Encoder implementation for EncodedConn
// This encoder will use the builtin encoding/json to Marshal
// and Unmarshal most types, including structs.
type JsonEncoder struct {
	// Empty
}

func (je *JsonEncoder) Encode(subject string, v interface{}) ([]byte, error) {
	b, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (je *JsonEncoder) Decode(subject string, data []byte, vPtr interface{}) error {
	return json.Unmarshal(data, vPtr)
}
