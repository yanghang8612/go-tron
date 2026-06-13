package downloader

import (
	"errors"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/p2p"
)

// BufferedBlock holds an out-of-order sync block awaiting contiguous drain.
// It stores the raw wire bytes plus light metadata rather than the decoded
// *types.Block: a decoded block pins its full proto tree and can balloon the
// GC mark set when many blocks are waiting on a gap.
type BufferedBlock struct {
	Raw  []byte
	Hash tcommon.Hash
	Num  uint64
	Peer *p2p.Peer
}

// NewBufferedBlock returns a buffered sync block with self-owned wire bytes.
func NewBufferedBlock(peer *p2p.Peer, block *types.Block, raw []byte) BufferedBlock {
	return BufferedBlock{
		Raw:  RawBlockBytes(block, raw),
		Hash: block.Hash(),
		Num:  block.Number(),
		Peer: peer,
	}
}

// RawBlockBytes returns a self-owned copy of the block's wire bytes for the
// sync buffer. raw is the exact payload received off the wire; callers without
// it pass nil and the bytes are re-marshaled from the decoded block.
func RawBlockBytes(block *types.Block, raw []byte) []byte {
	if len(raw) == 0 {
		b, _ := block.Marshal()
		return b
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

// BufferedBatch is the contiguous raw block run popped for local import.
type BufferedBatch struct {
	Blocks      []*types.Block
	Buffered    []BufferedBlock
	BufferWaits []time.Duration
}

// AppliedBatchSummary is the service-neutral accounting for the prefix of a
// buffered batch that canonical insertion accepted.
type AppliedBatchSummary struct {
	OK       bool
	Applied  int
	TxCount  int
	Last     BufferedBlock
	HasStage bool
}

// SummarizeAppliedBatch derives stats and the stage-progress boundary from an
// applied prefix of a buffered batch. Callers still own DB deletes, stage
// writes, logging, and sync stats emission.
func SummarizeAppliedBatch(batch BufferedBatch, applied int) AppliedBatchSummary {
	if applied <= 0 || applied > len(batch.Buffered) {
		return AppliedBatchSummary{}
	}
	summary := AppliedBatchSummary{
		OK:      true,
		Applied: applied,
		Last:    batch.Buffered[applied-1],
	}
	for i := 0; i < applied && i < len(batch.Blocks); i++ {
		if block := batch.Blocks[i]; block != nil {
			summary.TxCount += len(block.Transactions())
		}
	}
	summary.HasStage = summary.Last.Num > 0
	return summary
}

// ImportFailureResolution describes which prefix was applied before a staged
// import failed and which buffered block should be used to pause sync.
type ImportFailureResolution struct {
	OK          bool
	Applied     int
	FailedIndex int
	Failed      BufferedBlock
	FailedNum   uint64
}

// ResolveImportFailure maps a canonical range-insert error back onto the
// buffered downloader batch. InsertBlocksError.Index names the first failed
// block; generic errors conservatively pause at the first buffered block.
func ResolveImportFailure(batch BufferedBatch, insertErr error) ImportFailureResolution {
	if insertErr == nil || len(batch.Buffered) == 0 {
		return ImportFailureResolution{}
	}
	failed := 0
	var rangeErr *core.InsertBlocksError
	if errors.As(insertErr, &rangeErr) && rangeErr.Index >= 0 && rangeErr.Index < len(batch.Buffered) {
		failed = rangeErr.Index
	}
	failedBlock := batch.Buffered[failed]
	failedNum := failedBlock.Num
	if failedNum == 0 && rangeErr != nil {
		failedNum = rangeErr.BlockNumber
	}
	return ImportFailureResolution{
		OK:          true,
		Applied:     failed,
		FailedIndex: failed,
		Failed:      failedBlock,
		FailedNum:   failedNum,
	}
}

// StagedBodyDrainLimit clamps one local staged-body drain chunk to the
// hash-verified SyncBodiesReady frontier. The returned bool is false when the
// ready frontier is behind the next needed block, so callers should not drain
// from the local buffer.
func StagedBodyDrainLimit(next uint64, max int, readyLimit uint64, hasReadyLimit bool) (int, bool) {
	if max <= 0 {
		return 0, false
	}
	if !hasReadyLimit {
		return max, true
	}
	if readyLimit < next {
		return 0, false
	}
	if span := readyLimit - next + 1; span < uint64(max) {
		return int(span), true
	}
	return max, true
}

// PopBufferedBatch removes the contiguous run starting at next from the local
// raw block buffer. Popping also releases the session path reservation and hash
// de-dup entry because canonical import, or a sticky pause on failure, owns the
// block number after this point.
func PopBufferedBatch(buffer map[uint64]BufferedBlock, bufferedHashes map[tcommon.Hash]struct{}, path BlockPath, wait *BufferWaitTracker, next uint64, limit int, now time.Time) BufferedBatch {
	var batch BufferedBatch
	if limit <= 0 {
		return batch
	}
	for len(batch.Buffered) < limit {
		buffered, ok := buffer[next]
		if !ok {
			break
		}
		batch.BufferWaits = append(batch.BufferWaits, wait.End(next, now))
		delete(buffer, next)
		path.Release(next)
		delete(bufferedHashes, buffered.Hash)
		batch.Buffered = append(batch.Buffered, buffered)
		next++
	}
	return batch
}

// DecodeBlocks decodes raw buffered entries into Blocks. It preserves the
// successfully decoded prefix and reports the first undecodable entry; callers
// can import the prefix and refetch the dropped suffix.
func (b *BufferedBatch) DecodeBlocks() (BufferedBlock, error) {
	if b == nil {
		return BufferedBlock{}, nil
	}
	b.Blocks = make([]*types.Block, 0, len(b.Buffered))
	for _, buffered := range b.Buffered {
		block, err := types.UnmarshalBlock(buffered.Raw)
		if err != nil {
			return buffered, err
		}
		b.Blocks = append(b.Blocks, block)
	}
	return BufferedBlock{}, nil
}
