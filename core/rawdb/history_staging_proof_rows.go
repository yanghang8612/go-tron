package rawdb

import (
	"context"
)

// ReadBucketBlockProofs returns the retained hot tx-range/canonical hash rows
// for one complete bucket. The caller pins its cold manifest and holds the
// maintenance writer guard while using them to authenticate a rebind.
func (m *HistoryStagingManager) ReadBucketBlockProofs(ctx context.Context, bucket uint64) ([]HistoryStagingBlockProof, error) {
	if m == nil || ctx == nil {
		return nil, ErrHistoryStagingConflict
	}
	first, last, err := StateHistoryChunkBucketBounds(bucket)
	if err != nil {
		return nil, err
	}
	blocks := make([]HistoryStagingBlockProof, 0, StateHistoryChunkBucketBlocks)
	for block := first; block <= last; block++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		row, present, err := ReadStateTxRange(m.hot, block)
		if err != nil || !present || row.BlockNum != block || row.BlockHash == ([32]byte{}) || row.EndTxNum < row.BeginTxNum {
			return nil, ErrHistoryStagingIncomplete
		}
		if len(blocks) > 0 && (blocks[len(blocks)-1].EndTxNum == ^uint64(0) || row.BeginTxNum != blocks[len(blocks)-1].EndTxNum+1) {
			return nil, ErrHistoryStagingConflict
		}
		blocks = append(blocks, HistoryStagingBlockProof{Number: block, Hash: row.BlockHash, BeginTxNum: row.BeginTxNum, EndTxNum: row.EndTxNum})
	}
	return blocks, nil
}
