package rawdb

import (
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
)

type StageID string

const (
	StageHeaders       StageID = "Headers"
	StageBodies        StageID = "Bodies"
	StageExecution     StageID = "Execution"
	StageCommitment    StageID = "Commitment"
	StageFinish        StageID = "Finish"
	StageSnapshotBuild StageID = "SnapshotBuild"
	StageSnapshotPrune StageID = "SnapshotPrune"

	StageSnapshotLatest          StageID = "SnapshotLatest"
	StageSnapshotHistory         StageID = "SnapshotHistory"
	StageSnapshotAccessor        StageID = "SnapshotAccessor"
	StageSnapshotCommitmentFlush StageID = "SnapshotCommitmentFlush"
	StageSnapshotHotPrune        StageID = "SnapshotHotPrune"
)

type StageProgress struct {
	Stage        StageID
	BlockNum     uint64
	BlockHash    common.Hash
	HasBlockHash bool
}

func CanonicalExecutionStages() []StageID {
	return []StageID{
		StageHeaders,
		StageBodies,
		StageExecution,
		StageCommitment,
		StageFinish,
	}
}

func WriteStageProgress(db ethdb.KeyValueWriter, stage StageID, blockNum uint64) error {
	if db == nil {
		return errors.New("rawdb: nil stage progress writer")
	}
	if stage == "" {
		return errors.New("rawdb: empty stage id")
	}
	return db.Put(stageProgressKey(stage), encodeStageProgress(blockNum, common.Hash{}, false))
}

func WriteStageProgressWithHash(db ethdb.KeyValueWriter, stage StageID, blockNum uint64, blockHash common.Hash) error {
	if db == nil {
		return errors.New("rawdb: nil stage progress writer")
	}
	if stage == "" {
		return errors.New("rawdb: empty stage id")
	}
	return db.Put(stageProgressKey(stage), encodeStageProgress(blockNum, blockHash, true))
}

func WriteCanonicalStageProgress(db ethdb.KeyValueWriter, blockNum uint64) error {
	for _, stage := range CanonicalExecutionStages() {
		if err := WriteStageProgress(db, stage, blockNum); err != nil {
			return err
		}
	}
	return nil
}

func WriteCanonicalStageProgressWithHash(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash) error {
	for _, stage := range CanonicalExecutionStages() {
		if err := WriteStageProgressWithHash(db, stage, blockNum, blockHash); err != nil {
			return err
		}
	}
	return nil
}

func RewindStageProgress(db ethdb.KeyValueWriter, stage StageID, blockNum uint64) error {
	if db == nil {
		return errors.New("rawdb: nil stage progress writer")
	}
	if stage == "" {
		return errors.New("rawdb: empty stage id")
	}
	return WriteStageProgress(db, stage, blockNum)
}

func RewindCanonicalStageProgress(db ethdb.KeyValueWriter, blockNum uint64) error {
	for _, stage := range CanonicalExecutionStages() {
		if err := RewindStageProgress(db, stage, blockNum); err != nil {
			return err
		}
	}
	return nil
}

func RewindCanonicalStageProgressWithHash(db ethdb.KeyValueWriter, blockNum uint64, blockHash common.Hash) error {
	for _, stage := range CanonicalExecutionStages() {
		if err := WriteStageProgressWithHash(db, stage, blockNum, blockHash); err != nil {
			return err
		}
	}
	return nil
}

func ReadStageProgress(db ethdb.KeyValueReader, stage StageID) (uint64, bool, error) {
	row, ok, err := ReadStageProgressRow(db, stage)
	if err != nil || !ok {
		return 0, ok, err
	}
	return row.BlockNum, true, nil
}

func ReadStageProgressRow(db ethdb.KeyValueReader, stage StageID) (StageProgress, bool, error) {
	if db == nil || stage == "" {
		return StageProgress{}, false, nil
	}
	data, err := db.Get(stageProgressKey(stage))
	if err != nil {
		return StageProgress{}, false, nil
	}
	row, err := decodeStageProgress(stage, data)
	if err != nil {
		return StageProgress{}, false, err
	}
	return row, true, nil
}

func DeleteStageProgress(db ethdb.KeyValueWriter, stage StageID) error {
	if db == nil || stage == "" {
		return nil
	}
	return db.Delete(stageProgressKey(stage))
}

func IterateStageProgress(db ethdb.Iteratee, fn func(StageProgress) (bool, error)) error {
	if db == nil || fn == nil {
		return nil
	}
	it := db.NewIterator(stageProgressPrefix, nil)
	defer it.Release()
	for it.Next() {
		stage := StageID(string(it.Key()[len(stageProgressPrefix):]))
		if stage == "" {
			continue
		}
		row, err := decodeStageProgress(stage, it.Value())
		if err != nil {
			return err
		}
		cont, err := fn(StageProgress{
			Stage:        stage,
			BlockNum:     row.BlockNum,
			BlockHash:    row.BlockHash,
			HasBlockHash: row.HasBlockHash,
		})
		if err != nil {
			return err
		}
		if !cont {
			return nil
		}
	}
	return it.Error()
}

func encodeStageProgress(blockNum uint64, blockHash common.Hash, withHash bool) []byte {
	if !withHash {
		var buf [8]byte
		binary.BigEndian.PutUint64(buf[:], blockNum)
		return append([]byte(nil), buf[:]...)
	}
	buf := make([]byte, 8+common.HashLength)
	binary.BigEndian.PutUint64(buf[:8], blockNum)
	copy(buf[8:], blockHash[:])
	return buf
}

func decodeStageProgress(stage StageID, data []byte) (StageProgress, error) {
	switch len(data) {
	case 8:
		return StageProgress{Stage: stage, BlockNum: binary.BigEndian.Uint64(data)}, nil
	case 8 + common.HashLength:
		var hash common.Hash
		copy(hash[:], data[8:])
		return StageProgress{
			Stage:        stage,
			BlockNum:     binary.BigEndian.Uint64(data[:8]),
			BlockHash:    hash,
			HasBlockHash: true,
		}, nil
	default:
		return StageProgress{}, fmt.Errorf("rawdb: stage progress %q has length %d", stage, len(data))
	}
}
