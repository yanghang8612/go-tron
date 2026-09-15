package rawdb

// Frozen from51482c56582fa0b93ccbf5b22bb9f1c80aff96c2 state_changeset_shared.go.
// Only the function name changed; ordinary Has/Get, full chunk and pack SHA
// remain independent of the new read helper and cache branch.
import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"github.com/ethereum/go-ethereum/ethdb"
)

func frozen5148DecodeStateHistorySharedPack(db ethdb.KeyValueReader, data []byte, blockNum uint64) ([]byte, error) {
	if db == nil {
		return nil, fmt.Errorf("rawdb: shared history requires a coherent database reader")
	}
	refs, length, count, digest, err := sharedStateHistoryPackHeader(data, blockNum)
	if err != nil {
		return nil, err
	}
	bucket := stateHistoryChunkBucket(blockNum)
	decoded := make([]byte, length)
	offset := 0
	for i := 0; i < count; i++ {
		size, n := binary.Uvarint(refs)
		var hash [32]byte
		copy(hash[:], refs[n:n+32])
		refs = refs[n+32:]
		data, exists, err := readPresentValue(db, stateHistoryChunkKey(bucket, hash), "shared history chunk")
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, fmt.Errorf("rawdb: missing shared history chunk in bucket %d", bucket)
		}
		end := offset + int(size)
		_, err = decodeStateHistorySharedChunkInto(decoded[offset:end:end], data, int(size), hash)
		if err != nil {
			return nil, err
		}
		offset = end
	}
	if sha256.Sum256(decoded) != digest {
		return nil, fmt.Errorf("rawdb: shared history pack hash mismatch")
	}
	return decoded, nil
}
