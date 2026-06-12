package net

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	syncdl "github.com/tronprotocol/go-tron/net/sync/downloader"
)

func assertSyncPipelineProgress(t *testing.T, db ethdb.KeyValueReader, block *types.Block) {
	t.Helper()
	for _, stage := range syncdl.SyncPipelineProgressStages() {
		row, ok, err := rawdb.ReadStageProgressRow(db, stage)
		if err != nil || !ok || row.BlockNum != block.Number() || !row.HasBlockHash || row.BlockHash != block.Hash() {
			t.Fatalf("%s progress = %+v ok=%v err=%v, want block %d hash %x", stage, row, ok, err, block.Number(), block.Hash())
		}
	}
}
