package downloader

import (
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
)

// StagedBodyIterator streams persisted sync-staged body rows in ascending
// block-number order starting at start.
type StagedBodyIterator func(start uint64, fn func(rawdb.SyncStagedBlockRow) (bool, error)) error

// StagedBodyRestoreResult reports the contiguous range recovered from the
// persisted downloader body stage into the in-memory drain buffer.
type StagedBodyRestoreResult struct {
	Restored         int
	TargetHead       uint64
	NextExpected     uint64
	LastRestoredNum  uint64
	LastRestoredHash tcommon.Hash
	HaveLastRestored bool
	NeedPruneTail    bool
	PruneFrom        uint64
	ReadError        error
}

// RestoreStagedBodies restores a contiguous run of persisted downloader body
// rows into buffer, honoring any already-buffered contiguous entries first. A
// gap, path conflict, or read error asks the caller to prune the stale persisted
// tail from PruneFrom; the caller decides whether startup repair is enabled.
func RestoreStagedBodies(start uint64, limit int, targetHead uint64, buffer map[uint64]BufferedBlock, bufferedHashes map[tcommon.Hash]struct{}, path *BlockPath, iterate StagedBodyIterator) StagedBodyRestoreResult {
	result := StagedBodyRestoreResult{
		TargetHead:   targetHead,
		NextExpected: start,
	}
	if limit <= 0 || buffer == nil {
		return result
	}
	expected := start
	markRestored := func(num uint64, hash tcommon.Hash) {
		if num > result.TargetHead {
			result.TargetHead = num
		}
		result.LastRestoredNum = num
		result.LastRestoredHash = hash
		result.HaveLastRestored = true
		result.Restored++
		expected++
		result.NextExpected = expected
	}
	requestPrune := func(from uint64) {
		if !result.NeedPruneTail {
			result.NeedPruneTail = true
			result.PruneFrom = from
		}
		result.NextExpected = from
	}
	consumeBuffered := func() bool {
		for result.Restored < limit {
			buffered, ok := buffer[expected]
			if !ok {
				result.NextExpected = expected
				return true
			}
			markRestored(buffered.Num, buffered.Hash)
		}
		return false
	}
	if !consumeBuffered() || iterate == nil {
		return result
	}
	err := iterate(start, func(row rawdb.SyncStagedBlockRow) (bool, error) {
		for row.Number > expected {
			if !consumeBuffered() {
				return false, nil
			}
			if row.Number > expected {
				requestPrune(expected)
				return false, nil
			}
		}
		if row.Number < expected {
			return true, nil
		}
		bid := types.BlockID{Hash: row.Hash, Num: row.Number}
		if path != nil {
			nextPath, ok := (*path).Reserve(bid)
			if !ok {
				requestPrune(row.Number)
				return false, nil
			}
			*path = nextPath
		}
		buffer[row.Number] = BufferedBlock{
			Raw:  row.Raw,
			Hash: row.Hash,
			Num:  row.Number,
		}
		if bufferedHashes != nil {
			bufferedHashes[bid.Hash] = struct{}{}
		}
		markRestored(row.Number, row.Hash)
		return result.Restored < limit, nil
	})
	if err != nil {
		result.ReadError = err
		requestPrune(expected)
		return result
	}
	if result.Restored < limit && consumeBuffered() {
		requestPrune(expected)
	}
	return result
}
