package pruning

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var errNilStageProgressStore = errors.New("pruning: nil stage progress store")

type stageProgressStore interface {
	Write(stage rawdb.StageID, blockNum uint64) error
	WriteWithHash(stage rawdb.StageID, blockNum uint64, blockHash common.Hash) error
	Read(stage rawdb.StageID) (rawdb.StageProgress, bool, error)
}

type rawDBStageProgressStore struct {
	reader ethdb.KeyValueReader
	writer ethdb.KeyValueWriter
}

func newRawDBStageProgressStore(db interface {
	ethdb.KeyValueReader
	ethdb.KeyValueWriter
}) stageProgressStore {
	return rawDBStageProgressStore{reader: db, writer: db}
}

func newRawDBStageProgressReader(reader ethdb.KeyValueReader) stageProgressStore {
	return rawDBStageProgressStore{reader: reader}
}

func (s rawDBStageProgressStore) Write(stage rawdb.StageID, blockNum uint64) error {
	if s.writer == nil {
		return errNilStageProgressStore
	}
	return rawdb.WriteStageProgress(s.writer, stage, blockNum)
}

func (s rawDBStageProgressStore) WriteWithHash(stage rawdb.StageID, blockNum uint64, blockHash common.Hash) error {
	if s.writer == nil {
		return errNilStageProgressStore
	}
	return rawdb.WriteStageProgressWithHash(s.writer, stage, blockNum, blockHash)
}

func (s rawDBStageProgressStore) Read(stage rawdb.StageID) (rawdb.StageProgress, bool, error) {
	if s.reader == nil {
		return rawdb.StageProgress{}, false, errNilStageProgressStore
	}
	return rawdb.ReadStageProgressRow(s.reader, stage)
}
