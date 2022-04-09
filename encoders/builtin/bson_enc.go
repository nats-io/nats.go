package builtin

import (
	"go.mongodb.org/mongo-driver/bson"
	"strings"
)

// BsonEncoder is a BSON Encoder implementation for EncodedConn.
// This encoder will use the mongo bson encoder to Marshal
// and Unmarshal most types, including structs.
type BsonEncoder struct{}

//Encode encode v to bson
func (be *BsonEncoder) Encode(subject string, v interface{}) ([]byte, error) {
	b, err := bson.Marshal(v)
	if err != nil {
		return nil, err
	}
	return b, nil
}

//Decode decode data to bson
func (be *BsonEncoder) Decode(subject string, data []byte, vPtr interface{}) (err error) {
	switch arg := vPtr.(type) {
	case *string:
		str := string(data)
		if strings.HasPrefix(str, `"`) && strings.HasSuffix(str, `"`) {
			*arg = str[1 : len(str)-1]
		} else {
			*arg = str
		}
	case *[]byte:
		*arg = data
	default:
		err = bson.Unmarshal(data, arg)
	}
	return
}
