package downloader

import (
	"errors"
	"time"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
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

// BufferedBatchDecodeAction tells the local drain loop what to do after
// decoding a raw buffered batch.
type BufferedBatchDecodeAction uint8

const (
	BufferedBatchDecodeContinue BufferedBatchDecodeAction = iota
	BufferedBatchDecodeImport
)

// BufferedBatchDecodeResult records the decoded-prefix decision for one local
// import chunk.
type BufferedBatchDecodeResult struct {
	Action  BufferedBatchDecodeAction
	Dropped BufferedBlock
	Err     error
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

// AppliedStagedBlockDeletes returns the staged body rows covered by an applied
// prefix of a buffered batch.
func AppliedStagedBlockDeletes(batch BufferedBatch, applied int) []rawdb.SyncStagedBlockDelete {
	if applied <= 0 || len(batch.Buffered) == 0 {
		return nil
	}
	if applied > len(batch.Buffered) {
		applied = len(batch.Buffered)
	}
	deletes := make([]rawdb.SyncStagedBlockDelete, 0, applied)
	for i := 0; i < applied; i++ {
		deletes = append(deletes, rawdb.SyncStagedBlockDelete{
			Number: batch.Buffered[i].Num,
			Hash:   batch.Buffered[i].Hash,
		})
	}
	return deletes
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

// ImportOutcome describes how the local drain loop should settle a canonical
// insert attempt for one decoded buffered batch.
type ImportOutcome struct {
	Applied       int
	RecordApplied bool
	Pause         bool
	PausePeer     *p2p.Peer
	PauseNum      uint64
	StopDrain     bool
}

// PlanImportOutcome maps a canonical insert result into service actions:
// record the applied prefix, pause on failure, and decide whether the local
// drain loop should stop.
func PlanImportOutcome(batch BufferedBatch, insertErr error) ImportOutcome {
	if insertErr == nil {
		applied := len(batch.Blocks)
		return ImportOutcome{
			Applied:       applied,
			RecordApplied: applied > 0,
		}
	}
	outcome := ImportOutcome{
		Pause:     true,
		StopDrain: true,
	}
	failure := ResolveImportFailure(batch, insertErr)
	if !failure.OK {
		return outcome
	}
	outcome.Applied = failure.Applied
	outcome.RecordApplied = failure.Applied > 0
	outcome.PausePeer = failure.Failed.Peer
	outcome.PauseNum = failure.FailedNum
	return outcome
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

// StagedBodyDrainPlan is the local import chunk decision derived from the
// persisted ready frontier and the operator's import-batch limit.
type StagedBodyDrainPlan struct {
	RestoreLimit  int
	CanDrain      bool
	ReadyLimit    uint64
	HasReadyLimit bool
	RefreshReady  bool
	Steps         []StagedBodyDrainStep
}

// StagedBodyDrainStepAction names one ordered local operation required to drain
// a staged-body prefix into canonical import.
type StagedBodyDrainStepAction uint8

const (
	StagedBodyDrainRefreshReady StagedBodyDrainStepAction = iota
	StagedBodyDrainRestoreBodies
	StagedBodyDrainPopBuffer
)

// StagedBodyDrainStep is one downloader-owned step in a local staged-body drain.
type StagedBodyDrainStep struct {
	Action         StagedBodyDrainStepAction
	From           uint64
	Next           uint64
	Limit          int
	PruneStaleTail bool
}

// StagedBodyDrainPlanApplier performs the persistence/runtime operations named
// by a staged-body drain plan. SyncService owns DB handles and in-memory maps;
// downloader owns the ordered local import preparation.
type StagedBodyDrainPlanApplier interface {
	RefreshSyncBodiesReady()
	RestoreStagedBodies(from uint64, limit int, pruneStaleTail bool)
	PopBufferedBatch(next uint64, limit int) BufferedBatch
}

// PlanStagedBodyDrain decides how many staged bodies the local importer may
// restore and pop. Only a valid SyncBodiesReady row clamps the chunk; invalid
// or missing rows are treated as uncapped while their diagnostics are handled by
// the caller.
func PlanStagedBodyDrain(next uint64, max int, ready StagedBodyReadyLimit) StagedBodyDrainPlan {
	var plan StagedBodyDrainPlan
	if ready.Valid() {
		plan.ReadyLimit = ready.Limit
		plan.HasReadyLimit = true
	}
	if ready.Status == StagedBodyReadyLimitStale {
		plan.RefreshReady = true
	}
	plan.RestoreLimit, plan.CanDrain = StagedBodyDrainLimit(next, max, plan.ReadyLimit, plan.HasReadyLimit)
	return plan.withSteps(next)
}

func (p StagedBodyDrainPlan) withSteps(next uint64) StagedBodyDrainPlan {
	if p.RefreshReady {
		p.Steps = append(p.Steps, StagedBodyDrainStep{Action: StagedBodyDrainRefreshReady})
	}
	if p.CanDrain {
		p.Steps = append(p.Steps,
			StagedBodyDrainStep{Action: StagedBodyDrainRestoreBodies, From: next, Limit: p.RestoreLimit},
			StagedBodyDrainStep{Action: StagedBodyDrainPopBuffer, Next: next, Limit: p.RestoreLimit},
		)
	}
	return p
}

// ApplyStagedBodyDrainPlan executes the downloader-owned local drain schedule.
func ApplyStagedBodyDrainPlan(plan StagedBodyDrainPlan, applier StagedBodyDrainPlanApplier) BufferedBatch {
	if applier == nil {
		return BufferedBatch{}
	}
	var batch BufferedBatch
	for _, step := range plan.Steps {
		switch step.Action {
		case StagedBodyDrainRefreshReady:
			applier.RefreshSyncBodiesReady()
		case StagedBodyDrainRestoreBodies:
			applier.RestoreStagedBodies(step.From, step.Limit, step.PruneStaleTail)
		case StagedBodyDrainPopBuffer:
			batch = applier.PopBufferedBatch(step.Next, step.Limit)
		}
	}
	return batch
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

// DecodeBufferedBatch decodes a local import chunk and returns the next drain
// action. A decode error after a non-empty prefix still imports that prefix;
// an error on the first entry asks the caller to continue the drain loop.
func DecodeBufferedBatch(batch *BufferedBatch) BufferedBatchDecodeResult {
	dropped, err := batch.DecodeBlocks()
	result := BufferedBatchDecodeResult{
		Dropped: dropped,
		Err:     err,
	}
	if batch != nil && len(batch.Blocks) > 0 {
		result.Action = BufferedBatchDecodeImport
	}
	return result
}
