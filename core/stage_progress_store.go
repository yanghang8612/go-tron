package core

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var errNilStageProgressStore = errors.New("canonical stage pipeline: nil stage progress store")

type stageProgressStore interface {
	WriteWithHash(stage rawdb.StageID, blockNum uint64, hash common.Hash) error
	RewindCanonicalWithHash(blockNum uint64, hash common.Hash) error
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

func newRawDBStageProgressWriter(writer ethdb.KeyValueWriter) stageProgressStore {
	return rawDBStageProgressStore{writer: writer}
}

func newRawDBStageProgressReader(reader ethdb.KeyValueReader) stageProgressStore {
	return rawDBStageProgressStore{reader: reader}
}

func (s rawDBStageProgressStore) WriteWithHash(stage rawdb.StageID, blockNum uint64, hash common.Hash) error {
	if s.writer == nil {
		return errNilStageProgressStore
	}
	return rawdb.WriteStageProgressWithHash(s.writer, stage, blockNum, hash)
}

func (s rawDBStageProgressStore) RewindCanonicalWithHash(blockNum uint64, hash common.Hash) error {
	if s.writer == nil {
		return errNilStageProgressStore
	}
	return rawdb.RewindCanonicalStageProgressWithHash(s.writer, blockNum, hash)
}

func (s rawDBStageProgressStore) Read(stage rawdb.StageID) (rawdb.StageProgress, bool, error) {
	if s.reader == nil {
		return rawdb.StageProgress{}, false, errNilStageProgressStore
	}
	return rawdb.ReadStageProgressRow(s.reader, stage)
}
