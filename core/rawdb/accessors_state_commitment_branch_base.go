package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const (
	commitmentBranchBaseVersion     = byte(1)
	commitmentBranchBaseSize        = 1 + 8 + 8 + common.HashLength
	commitmentBranchRotationVersion = byte(1)
	commitmentBranchRotationSize    = 1 + 8 + 8 + common.HashLength + 8 + common.HashLength
)

// CommitmentBranchBase binds one immutable branch snapshot to the generation
// that contains all later updates and tombstones. A marker is valid only when
// the referenced snapshot's root matches Root exactly.
type CommitmentBranchBase struct {
	Generation    uint64
	SnapshotTxNum uint64
	Root          common.Hash
}

// CommitmentBranchRotation records the short crash-safe interval in which new
// writes already target Generation while the complete legacy branch table is
// frozen as the baseline being built. Root and SnapshotTxNum bind the eventual
// immutable snapshot. Completion atomically replaces this marker with a base.
type CommitmentBranchRotation struct {
	Generation    uint64
	SnapshotTxNum uint64
	Root          common.Hash
	BlockNum      uint64
	BlockHash     common.Hash
}

func EncodeCommitmentBranchBase(base CommitmentBranchBase) ([]byte, error) {
	if base.Generation == 0 {
		return nil, errors.New("rawdb: zero commitment branch base generation")
	}
	encoded := make([]byte, commitmentBranchBaseSize)
	encoded[0] = commitmentBranchBaseVersion
	binary.BigEndian.PutUint64(encoded[1:9], base.Generation)
	binary.BigEndian.PutUint64(encoded[9:17], base.SnapshotTxNum)
	copy(encoded[17:], base.Root[:])
	return encoded, nil
}

func DecodeCommitmentBranchBase(encoded []byte) (CommitmentBranchBase, error) {
	if len(encoded) != commitmentBranchBaseSize {
		return CommitmentBranchBase{}, fmt.Errorf("rawdb: commitment branch base bad length %d", len(encoded))
	}
	if encoded[0] != commitmentBranchBaseVersion {
		return CommitmentBranchBase{}, fmt.Errorf("rawdb: commitment branch base unsupported version %d", encoded[0])
	}
	base := CommitmentBranchBase{
		Generation:    binary.BigEndian.Uint64(encoded[1:9]),
		SnapshotTxNum: binary.BigEndian.Uint64(encoded[9:17]),
	}
	if base.Generation == 0 {
		return CommitmentBranchBase{}, errors.New("rawdb: zero commitment branch base generation")
	}
	copy(base.Root[:], encoded[17:])
	return base, nil
}

func WriteCommitmentBranchBase(db ethdb.KeyValueWriter, base CommitmentBranchBase) error {
	encoded, err := EncodeCommitmentBranchBase(base)
	if err != nil {
		return err
	}
	return db.Put(stateCommitmentBranchBaseKey, encoded)
}

func ReadCommitmentBranchBase(db ethdb.KeyValueReader) (CommitmentBranchBase, bool, error) {
	encoded, ok, err := readPresentValue(db, stateCommitmentBranchBaseKey, "commitment branch base")
	if err != nil || !ok {
		return CommitmentBranchBase{}, ok, err
	}
	base, err := DecodeCommitmentBranchBase(encoded)
	if err != nil {
		return CommitmentBranchBase{}, false, err
	}
	return base, true, nil
}

func DeleteCommitmentBranchBase(db ethdb.KeyValueWriter) error {
	return db.Delete(stateCommitmentBranchBaseKey)
}

func WriteCommitmentBranchRotation(db ethdb.KeyValueWriter, rotation CommitmentBranchRotation) error {
	encoded, err := EncodeCommitmentBranchRotation(rotation)
	if err != nil {
		return err
	}
	return db.Put(stateCommitmentBranchRotationKey, encoded)
}

func EncodeCommitmentBranchRotation(rotation CommitmentBranchRotation) ([]byte, error) {
	if rotation.Generation == 0 {
		return nil, errors.New("rawdb: zero commitment branch rotation generation")
	}
	if rotation.BlockHash == (common.Hash{}) {
		return nil, errors.New("rawdb: zero commitment branch rotation block hash")
	}
	encoded := make([]byte, commitmentBranchRotationSize)
	encoded[0] = commitmentBranchRotationVersion
	binary.BigEndian.PutUint64(encoded[1:9], rotation.Generation)
	binary.BigEndian.PutUint64(encoded[9:17], rotation.SnapshotTxNum)
	copy(encoded[17:17+common.HashLength], rotation.Root[:])
	offset := 17 + common.HashLength
	binary.BigEndian.PutUint64(encoded[offset:offset+8], rotation.BlockNum)
	copy(encoded[offset+8:], rotation.BlockHash[:])
	return encoded, nil
}

func DecodeCommitmentBranchRotation(encoded []byte) (CommitmentBranchRotation, error) {
	if len(encoded) != commitmentBranchRotationSize {
		return CommitmentBranchRotation{}, fmt.Errorf("rawdb: commitment branch rotation bad length %d", len(encoded))
	}
	if encoded[0] != commitmentBranchRotationVersion {
		return CommitmentBranchRotation{}, fmt.Errorf("rawdb: commitment branch rotation unsupported version %d", encoded[0])
	}
	rotation := CommitmentBranchRotation{
		Generation:    binary.BigEndian.Uint64(encoded[1:9]),
		SnapshotTxNum: binary.BigEndian.Uint64(encoded[9:17]),
	}
	if rotation.Generation == 0 {
		return CommitmentBranchRotation{}, errors.New("rawdb: zero commitment branch rotation generation")
	}
	copy(rotation.Root[:], encoded[17:17+common.HashLength])
	offset := 17 + common.HashLength
	rotation.BlockNum = binary.BigEndian.Uint64(encoded[offset : offset+8])
	copy(rotation.BlockHash[:], encoded[offset+8:])
	if rotation.BlockHash == (common.Hash{}) {
		return CommitmentBranchRotation{}, errors.New("rawdb: zero commitment branch rotation block hash")
	}
	return rotation, nil
}

func ReadCommitmentBranchRotation(db ethdb.KeyValueReader) (CommitmentBranchRotation, bool, error) {
	encoded, ok, err := readPresentValue(db, stateCommitmentBranchRotationKey, "commitment branch rotation")
	if err != nil || !ok {
		return CommitmentBranchRotation{}, ok, err
	}
	rotation, err := DecodeCommitmentBranchRotation(encoded)
	if err != nil {
		return CommitmentBranchRotation{}, false, err
	}
	return rotation, true, nil
}

func DeleteCommitmentBranchRotation(db ethdb.KeyValueWriter) error {
	return db.Delete(stateCommitmentBranchRotationKey)
}
