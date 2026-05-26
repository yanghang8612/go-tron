package snapshots

import (
	"errors"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var errNilStageProgressStore = errors.New("snapshots: nil stage progress store")

type stageProgressStore interface {
	Write(stage rawdb.StageID, blockNum uint64) error
}

type rawDBStageProgressStore struct {
	writer ethdb.KeyValueWriter
}

func newRawDBStageProgressStore(writer ethdb.KeyValueWriter) stageProgressStore {
	return rawDBStageProgressStore{writer: writer}
}

func (s rawDBStageProgressStore) Write(stage rawdb.StageID, blockNum uint64) error {
	if s.writer == nil {
		return errNilStageProgressStore
	}
	return rawdb.WriteStageProgress(s.writer, stage, blockNum)
}
