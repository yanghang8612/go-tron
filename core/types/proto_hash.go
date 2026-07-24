package types

import (
	"crypto/sha256"
	"sync"

	"github.com/tronprotocol/go-tron/common"
	"google.golang.org/protobuf/proto"
)

const maxPooledProtoHashBuffer = 1 << 20

var protoHashBufferPool = sync.Pool{
	New: func() any {
		buf := make([]byte, 0, 512)
		return &buf
	},
}

// hashProtoMessage hashes the canonical protobuf encoding while reusing the
// short-lived marshal destination. The encoded bytes are consumed before the
// buffer is returned and never escape this function. Oversized transactions
// are deliberately not retained by the process-wide pool.
func hashProtoMessage(message proto.Message) (common.Hash, error) {
	bufPtr := protoHashBufferPool.Get().(*[]byte)
	scratch := (*bufPtr)[:0]
	encoded, err := proto.MarshalOptions{}.MarshalAppend(scratch, message)
	if err != nil {
		if cap(scratch) <= maxPooledProtoHashBuffer {
			*bufPtr = scratch[:0]
			protoHashBufferPool.Put(bufPtr)
		}
		return common.Hash{}, err
	}
	hash := common.Hash(sha256.Sum256(encoded))
	if cap(encoded) <= maxPooledProtoHashBuffer {
		*bufPtr = encoded[:0]
		protoHashBufferPool.Put(bufPtr)
	}
	return hash, nil
}
