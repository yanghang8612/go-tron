package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

const (
	commitmentBranchBaseVersion = byte(1)
	commitmentBranchBaseSize    = 1 + 8 + 8 + common.HashLength
)

// CommitmentBranchBase binds one immutable branch snapshot to the generation
// that contains all later updates and tombstones. A marker is valid only when
// the referenced snapshot's root matches Root exactly.
type CommitmentBranchBase struct {
	Generation    uint64
	SnapshotTxNum uint64
	Root          common.Hash
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
