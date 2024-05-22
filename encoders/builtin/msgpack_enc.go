package builtin

import (
	"bytes"

	"github.com/vmihailenco/msgpack/v5"
)

// MsgpackEncoder is a Msgpack Encoder implementation for EncodedConn.
// This encoder will use the github.com/vmihailenco/msgpack/v5 to Marshal
// and Unmarshal most types, including structs.
type MsgpackEncoder struct{}

// Encode
func (me *MsgpackEncoder) Encode(subject string, v any) ([]byte, error) {
	b, err := msgpack.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

// Decode
func (me *MsgpackEncoder) Decode(subject string, data []byte, vPtr any) (err error) {
	dec := msgpack.NewDecoder(bytes.NewBuffer(data))
	err = dec.Decode(vPtr)
	return
}
