package rawdb

import (
	"bytes"
	"encoding/binary"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/rlp"
	"github.com/tronprotocol/go-tron/common"
	"golang.org/x/crypto/sha3"
)

const LatestDomainCommitmentScheme = "state-flat-latest-v1"

var (
	latestDomainCommitmentRootKey       = []byte("latest-root")
	latestStateCommitmentCheckpointKey  = []byte("latest-checkpoint")
	stateCommitmentCheckpointLogicalPfx = []byte("checkpoint/")
)

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
	data, err := EncodeStateCommitmentCheckpointValue(checkpoint)
	if err != nil {
		return err
	}
	if err := WriteStateCommitmentDomain(db, stateCommitmentCheckpointLogicalKey(checkpoint.BlockNum), data); err != nil {
		return err
	}
	if reader, ok := db.(ethdb.KeyValueReader); ok {
		latest, ok, err := ReadLatestStateCommitmentCheckpoint(reader)
		if err != nil {
			return err
		}
		if ok && latest.BlockNum > checkpoint.BlockNum {
			return nil
		}
		if !ok {
			if iter, ok := db.(ethdb.Iteratee); ok {
				newest, err := latestStateCommitmentCheckpointByIteration(iter)
				if err != nil {
					return err
				}
				if newest != nil && newest.BlockNum > checkpoint.BlockNum {
					return writeLatestStateCommitmentCheckpoint(db, newest)
				}
			}
		}
	}
	return writeLatestStateCommitmentCheckpoint(db, checkpoint)
}

func ReadStateCommitmentCheckpoint(db ethdb.KeyValueReader, blockNum uint64) (*StateCommitmentCheckpoint, bool, error) {
	data, ok, err := ReadStateCommitmentDomain(db, stateCommitmentCheckpointLogicalKey(blockNum))
	if err != nil || !ok {
		return nil, ok, err
	}
	return decodeStateCommitmentCheckpoint(data)
}

func DeleteStateCommitmentCheckpoint(db ethdb.KeyValueWriter, blockNum uint64) error {
	if err := DeleteStateCommitmentDomain(db, stateCommitmentCheckpointLogicalKey(blockNum)); err != nil {
		return err
	}
	reader, ok := db.(ethdb.KeyValueReader)
	if !ok {
		return nil
	}
	latest, latestOK, err := ReadLatestStateCommitmentCheckpoint(reader)
	if err != nil || !latestOK || latest.BlockNum != blockNum {
		return err
	}
	if err := DeleteStateCommitmentDomain(db, latestStateCommitmentCheckpointKey); err != nil {
		return err
	}
	iter, ok := db.(ethdb.Iteratee)
	if !ok {
		return nil
	}
	newest, err := latestStateCommitmentCheckpointByIteration(iter)
	if err != nil {
		return err
	}
	if newest == nil {
		return nil
	}
	return writeLatestStateCommitmentCheckpoint(db, newest)
}

func IterateStateCommitmentCheckpoints(db ethdb.Iteratee, fn func(*StateCommitmentCheckpoint) (bool, error)) error {
	return IterateStateCommitmentDomain(db, stateCommitmentCheckpointLogicalPfx, func(_ []byte, value []byte) (bool, error) {
		checkpoint, ok, err := decodeStateCommitmentCheckpoint(value)
		if err != nil || !ok {
			return false, err
		}
		cont, err := fn(checkpoint)
		if err != nil {
			return false, err
		}
		if !cont {
			return false, nil
		}
		return true, nil
	})
}

func ReadLatestStateCommitmentCheckpoint(db ethdb.KeyValueReader) (*StateCommitmentCheckpoint, bool, error) {
	data, ok, err := ReadStateCommitmentDomain(db, latestStateCommitmentCheckpointKey)
	if err != nil || !ok {
		return nil, ok, err
	}
	return decodeStateCommitmentCheckpoint(data)
}

func writeLatestStateCommitmentCheckpoint(db ethdb.KeyValueWriter, checkpoint *StateCommitmentCheckpoint) error {
	if checkpoint == nil {
		return nil
	}
	data, err := EncodeStateCommitmentCheckpointValue(checkpoint)
	if err != nil {
		return err
	}
	return WriteStateCommitmentDomain(db, latestStateCommitmentCheckpointKey, data)
}

func latestStateCommitmentCheckpointByIteration(db ethdb.Iteratee) (*StateCommitmentCheckpoint, error) {
	var newest *StateCommitmentCheckpoint
	if err := IterateStateCommitmentCheckpoints(db, func(checkpoint *StateCommitmentCheckpoint) (bool, error) {
		if newest == nil || checkpoint.BlockNum > newest.BlockNum {
			cp := *checkpoint
			newest = &cp
		}
		return true, nil
	}); err != nil {
		return nil, err
	}
	return newest, nil
}

func decodeStateCommitmentCheckpoint(data []byte) (*StateCommitmentCheckpoint, bool, error) {
	checkpoint, err := DecodeStateCommitmentCheckpointValue(data)
	if err != nil {
		return nil, false, err
	}
	return checkpoint, true, nil
}

func EncodeStateCommitmentCheckpointValue(checkpoint *StateCommitmentCheckpoint) ([]byte, error) {
	if checkpoint == nil {
		return nil, nil
	}
	return rlp.EncodeToBytes(checkpoint)
}

func DecodeStateCommitmentCheckpointValue(data []byte) (*StateCommitmentCheckpoint, error) {
	var checkpoint StateCommitmentCheckpoint
	if err := rlp.DecodeBytes(data, &checkpoint); err != nil {
		return nil, err
	}
	return &checkpoint, nil
}

// ComputeLatestDomainRoot computes a deterministic commitment over the
// physical flat latest-state tables. It intentionally excludes content-
// addressed code blobs because account envelopes select code by hash; code
// retention may contain orphan immutable blobs.
func ComputeLatestDomainRoot(db ethdb.Iteratee) (common.Hash, error) {
	h := sha3.NewLegacyKeccak256()
	for _, prefix := range [][]byte{stateAccountLatestPrefix, stateKVGenerationPrefix, stateKVLatestPrefix} {
		if err := hashPrefix(h, db, prefix); err != nil {
			return common.Hash{}, err
		}
	}
	var out common.Hash
	h.Sum(out[:0])
	return out, nil
}

func WriteLatestDomainCommitmentRoot(db ethdb.KeyValueWriter, root common.Hash) error {
	return WriteStateCommitmentDomain(db, latestDomainCommitmentRootKey, root.Bytes())
}

func ReadLatestDomainCommitmentRoot(db ethdb.KeyValueReader) (common.Hash, bool, error) {
	value, ok, err := ReadStateCommitmentDomain(db, latestDomainCommitmentRootKey)
	if err != nil || !ok {
		return common.Hash{}, ok, err
	}
	if len(value) != common.HashLength {
		return common.Hash{}, false, fmt.Errorf("state commitment root: bad length %d", len(value))
	}
	return common.BytesToHash(value), true, nil
}

func LatestDomainCommitmentRootLogicalKey() []byte {
	return append([]byte(nil), latestDomainCommitmentRootKey...)
}

func LatestStateCommitmentCheckpointLogicalKey() []byte {
	return append([]byte(nil), latestStateCommitmentCheckpointKey...)
}

func StateCommitmentCheckpointLogicalPrefix() []byte {
	return append([]byte(nil), stateCommitmentCheckpointLogicalPfx...)
}

func stateCommitmentCheckpointLogicalKey(blockNum uint64) []byte {
	key := make([]byte, len(stateCommitmentCheckpointLogicalPfx)+8)
	copy(key, stateCommitmentCheckpointLogicalPfx)
	binary.BigEndian.PutUint64(key[len(stateCommitmentCheckpointLogicalPfx):], blockNum)
	return key
}

func LatestDomainCommitmentNodeLogicalPrefix() []byte {
	return append([]byte(nil), commitmentNodePrefix...)
}

func IsLatestDomainCommitmentRootLogicalKey(logicalKey []byte) bool {
	return bytes.Equal(logicalKey, latestDomainCommitmentRootKey)
}

func IsLatestStateCommitmentCheckpointLogicalKey(logicalKey []byte) bool {
	return bytes.Equal(logicalKey, latestStateCommitmentCheckpointKey)
}

func IsLatestDomainCommitmentNodeLogicalKey(logicalKey []byte) bool {
	return bytes.HasPrefix(logicalKey, commitmentNodePrefix)
}

func IsStateCommitmentCheckpointLogicalKey(logicalKey []byte) bool {
	return bytes.HasPrefix(logicalKey, stateCommitmentCheckpointLogicalPfx) &&
		len(logicalKey) == len(stateCommitmentCheckpointLogicalPfx)+8
}

type latestDomainCommitmentStore interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
	ethdb.Iteratee
}

func ComputeAndWriteLatestDomainRoot(db latestDomainCommitmentStore) (common.Hash, error) {
	return RebuildLatestDomainCommitment(db)
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
