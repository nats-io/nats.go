// Copyright 2012-2014 Apcera Inc. All rights reserved.

package nats

import (
	"github.com/golang/protobuf/proto"
)

// A JSON Encoder implementation for EncodedConn
// This encoder will use the builtin encoding/json to Marshal
// and Unmarshal most types, including structs.
type ProtoEncoder struct {
	// Empty
}

func (pb *ProtoEncoder) Encode(subject string, v interface{}) ([]byte, error) {
	b, err := proto.Marshal(v.(proto.Message))
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (pb *ProtoEncoder) Decode(subject string, data []byte, vPtr interface{}) (error) {
	
	i := vPtr.(proto.Message)
	err := proto.Unmarshal(data, i)
	if err != nil {
		return err
	}
	return nil
}
