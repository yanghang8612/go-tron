package rawdb

import (
	"encoding/binary"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"golang.org/x/crypto/sha3"
)

const LatestDomainCommitmentScheme = "state-kv-latest-v1"

type StateCommitmentCheckpoint struct {
	BlockNum  uint64
	BlockHash common.Hash
	Root      common.Hash
	Scheme    string
}

func WriteStateCommitmentCheckpoint(db ethdb.KeyValueWriter, checkpoint *StateCommitmentCheckpoint) error {
	if checkpoint == nil {
		return nil
	}
	data, err := rlp.EncodeToBytes(checkpoint)
	if err != nil {
		return err
	}
	return db.Put(stateCommitmentCheckpointKey(checkpoint.BlockNum), data)
}

func ReadStateCommitmentCheckpoint(db ethdb.KeyValueReader, blockNum uint64) (*StateCommitmentCheckpoint, bool, error) {
	data, err := db.Get(stateCommitmentCheckpointKey(blockNum))
	if err != nil {
		return nil, false, nil
	}
	var checkpoint StateCommitmentCheckpoint
	if err := rlp.DecodeBytes(data, &checkpoint); err != nil {
		return nil, false, err
	}
	return &checkpoint, true, nil
}

func DeleteStateCommitmentCheckpoint(db ethdb.KeyValueWriter, blockNum uint64) error {
	return db.Delete(stateCommitmentCheckpointKey(blockNum))
}

func IterateStateCommitmentCheckpoints(db ethdb.Iteratee, fn func(*StateCommitmentCheckpoint) (bool, error)) error {
	it := db.NewIterator(stateCommitmentPrefix, nil)
	defer it.Release()
	for it.Next() {
		var checkpoint StateCommitmentCheckpoint
		if err := rlp.DecodeBytes(it.Value(), &checkpoint); err != nil {
			return err
		}
		cont, err := fn(&checkpoint)
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

// ComputeLatestDomainRoot computes a deterministic debug commitment over the
// physical latest generic-domain tables. It intentionally excludes
// content-addressed code blobs because the final commitment must include only
// code hashes selected by account envelopes; code-domain retention may contain
// orphan immutable blobs.
func ComputeLatestDomainRoot(db ethdb.Iteratee) (common.Hash, error) {
	h := sha3.NewLegacyKeccak256()
	for _, prefix := range [][]byte{stateKVGenerationPrefix, stateKVLatestPrefix} {
		if err := hashPrefix(h, db, prefix); err != nil {
			return common.Hash{}, err
		}
	}
	var out common.Hash
	h.Sum(out[:0])
	return out, nil
}

type byteWriter interface {
	Write([]byte) (int, error)
}

func hashPrefix(h byteWriter, db ethdb.Iteratee, prefix []byte) error {
	it := db.NewIterator(prefix, nil)
	defer it.Release()
	for it.Next() {
		writeLenPrefixed(h, it.Key())
		writeLenPrefixed(h, it.Value())
	}
	return it.Error()
}

func writeLenPrefixed(h byteWriter, data []byte) {
	var lenBuf [8]byte
	binary.BigEndian.PutUint64(lenBuf[:], uint64(len(data)))
	_, _ = h.Write(lenBuf[:])
	_, _ = h.Write(data)
}
