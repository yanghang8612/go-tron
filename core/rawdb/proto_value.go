package rawdb

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"google.golang.org/protobuf/proto"
)

// protoValueWriter is implemented by Pebble batches that can reserve a final
// value record and let the serializer fill it in place. Other databases keep
// the ordinary defensive Put path.
type protoValueWriter interface {
	PutValueFunc(key []byte, valueLen int, fill func([]byte) error) error
}

func putProtoValue(db ethdb.KeyValueWriter, key []byte, message proto.Message) error {
	if writer, ok := db.(protoValueWriter); ok {
		size := proto.Size(message)
		return writer.PutValueFunc(key, size, func(dst []byte) error {
			encoded, err := proto.MarshalOptions{}.MarshalAppend(dst[:0], message)
			if err != nil {
				return err
			}
			if len(encoded) != len(dst) {
				return fmt.Errorf("rawdb: protobuf size changed during batch encode: got %d want %d", len(encoded), len(dst))
			}
			// MarshalAppend normally uses dst's exact reserved capacity. Retain a
			// defensive copy for an implementation that unexpectedly returns a
			// different backing slice without changing the stored bytes.
			if len(dst) != 0 && &encoded[0] != &dst[0] {
				copy(dst, encoded)
			}
			return nil
		})
	}
	data, err := proto.Marshal(message)
	if err != nil {
		return err
	}
	return db.Put(key, data)
}
