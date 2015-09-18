// Copyright 2014 Antonio Antelo Vazquez All rights reserved.

package nats

import (
	"errors"

	"github.com/golang/protobuf/proto"
)

// A protobuf Encoder implementation for EncodedConn
// This encoder will use the builtin protobuf lib to Marshal
// and Unmarshal structs.
type ProtoEncoder struct {
	// Empty
}

func (pb *ProtoEncoder) Encode(subject string, v interface{}) ([]byte, error) {
	i, found := v.(proto.Message)
	if !found {
		return nil, errors.New("Invalid proto.Message object to encode")
	}

	b, err := proto.Marshal(i)
	if err != nil {
		return nil, err
	}
	return b, nil
}

func (pb *ProtoEncoder) Decode(subject string, data []byte, vPtr interface{}) error {
	i, found := vPtr.(proto.Message)
	if !found {
		return errors.New("Invalid proto.Message object to decode")
	}

	err := proto.Unmarshal(data, i)
	if err != nil {
		return err
	}
	return nil
}
