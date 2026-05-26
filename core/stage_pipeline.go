package core

import (
	"fmt"

	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// canonicalStagePipeline is the block-local execution stage writer. It keeps
// canonical stage progress tied to one block hash and enforces the execution
// order expected by rewind/prune consumers.
type canonicalStagePipeline struct {
	progress stageProgressStore
	blockNum uint64
	hash     tcommon.Hash
	last     int
}

func newCanonicalStagePipeline(writer ethdb.KeyValueWriter, blockNum uint64, hash tcommon.Hash) *canonicalStagePipeline {
	return &canonicalStagePipeline{
		progress: newRawDBStageProgressWriter(writer),
		blockNum: blockNum,
		hash:     hash,
		last:     -1,
	}
}

func (p *canonicalStagePipeline) Advance(stages ...rawdb.StageID) error {
	if p == nil {
		return fmt.Errorf("canonical stage pipeline: nil pipeline")
	}
	if p.progress == nil {
		return errNilStageProgressStore
	}
	for _, stage := range stages {
		ord, ok := canonicalExecutionStageOrdinal(stage)
		if !ok {
			return fmt.Errorf("canonical stage pipeline: %s is not a canonical execution stage", stage)
		}
		if ord != p.last+1 {
			return fmt.Errorf("canonical stage pipeline: stage %s out of order after ordinal %d", stage, p.last)
		}
		if err := p.progress.WriteWithHash(stage, p.blockNum, p.hash); err != nil {
			return fmt.Errorf("write %s stage progress: %w", stage, err)
		}
		p.last = ord
	}
	return nil
}

func rewindCanonicalStagePipeline(writer ethdb.KeyValueWriter, blockNum uint64, hash tcommon.Hash) error {
	if err := newRawDBStageProgressWriter(writer).RewindCanonicalWithHash(blockNum, hash); err != nil {
		return fmt.Errorf("rewind canonical stage progress to %d: %w", blockNum, err)
	}
	return nil
}

func verifyCanonicalStagePipelineHead(reader ethdb.KeyValueReader, blockNum uint64, hash tcommon.Hash) error {
	if reader == nil {
		return fmt.Errorf("canonical stage pipeline: nil reader")
	}
	progress := newRawDBStageProgressReader(reader)
	for _, stage := range rawdb.CanonicalExecutionStages() {
		row, ok, err := progress.Read(stage)
		if err != nil {
			return fmt.Errorf("read %s stage progress: %w", stage, err)
		}
		if !ok {
			return fmt.Errorf("missing %s stage progress", stage)
		}
		if row.BlockNum != blockNum {
			return fmt.Errorf("%s stage progress at block %d, want %d", stage, row.BlockNum, blockNum)
		}
		if !row.HasBlockHash {
			return fmt.Errorf("%s stage progress at block %d is not hash-bound", stage, row.BlockNum)
		}
		if row.BlockHash != hash {
			return fmt.Errorf("%s stage progress at block %d has hash %x, want %x", stage, row.BlockNum, row.BlockHash, hash)
		}
	}
	return nil
}

func canonicalExecutionStageOrdinal(stage rawdb.StageID) (int, bool) {
	switch stage {
	case rawdb.StageHeaders:
		return 0, true
	case rawdb.StageBodies:
		return 1, true
	case rawdb.StageExecution:
		return 2, true
	case rawdb.StageCommitment:
		return 3, true
	case rawdb.StageFinish:
		return 4, true
	default:
		return 0, false
	}
}
