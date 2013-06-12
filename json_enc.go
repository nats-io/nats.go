// Copyright 2012-2013 Apcera Inc. All rights reserved.

package nats

import (
	"encoding/json"
	"strings"
	"unsafe"
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

func (je *JsonEncoder) Decode(subject string, data []byte, vPtr interface{}) (err error) {
	switch arg := vPtr.(type) {
	case *string:
		// If they want a string and it is a JSON string, strip quotes
		// This allows someone to send a struct but receive as a plain string if
		// need be..
		str := *(*string)(unsafe.Pointer(&data))
		if strings.HasPrefix(str, `"`) && strings.HasSuffix(str, `"`) {
			*arg = str[1 : len(str)-1]
		} else {
			*arg = str
		}
	case *[]byte:
		*arg = data
	default:
		err = json.Unmarshal(data, arg)
	}
	return
}
