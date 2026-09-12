package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var (
	// ErrStateChangePostingPruneDeferred leaves the caller's in-memory traversal
	// unchanged while the index builder owns its lock or the required prefix is unavailable.
	ErrStateChangePostingPruneDeferred = errors.New("state-change posting prune deferred")
	// ErrStateChangePostingPruneBoundaryChanged invalidates the current traversal
	// and its hot-prune permission. A fresh permission is required before retrying.
	ErrStateChangePostingPruneBoundaryChanged = errors.New("state-change posting prune boundary changed")
)

// StateChangePostingPruneTimings records elapsed phases even when a call fails.
// ChainHeld includes admission, proof, scan and write; GateHeld overlaps it.
// Zero means that phase was not entered (or was below clock resolution).
type StateChangePostingPruneTimings struct {
	ChainWait, ChainHeld, Admission, Proof, GateHeld time.Duration
}

// StateChangePostingPruneAdmission runs synchronously with both chain/index
// locks held. It must not acquire either lock, perform device/proc reads or wait
// for a maintenance lease. On success it returns a non-nil release function;
// the core wrapper releases the lease on every exit before unlocking the chain.
type StateChangePostingPruneAdmission func() (release func(), admitted bool)

// PruneStateChangePostingChunk deletes one bounded set of wholly stale frames.
// prunedThrough must come from a successful hot-history prune in this process;
// proofHead/proofHash are that prune's previously verified canonical boundary.
// They must remain fixed throughout the cursor traversal. A manifest watermark
// alone is not this permission: offline restore/reset can rewrite old history.
//
// A zero anchor starts a traversal. Successful calls return the canonical hash
// at prunedThrough, which must accompany subsequent cursors. All progress is in
// memory; callers must not persist a cursor ahead of these unsynced deletions.
// No directory rows, stage watermarks or manifest progress are modified.
//
// The lock order matches the index builder and Close. chainmu also excludes
// canonical rewinds, which do not acquire stateHistoryIndexMu. Both locks stay
// held through the chunk's batch write. Existing async commits/flushes may keep
// writing strictly beyond the verified durable prefix; those immutable frame
// keys cannot replace the <= prunedThrough frames considered for deletion.
// The index lock is acquired opportunistically. The chain lock queues behind
// existing holders, so per-block import can hand it off without a maintenance
// timer having to hit the tiny unlock/relock interval. Waiting has no hard
// timeout: cancellation is checked immediately after acquisition, and shutdown
// may need to wait for an existing holder to finish. Limits bound work between
// rows, not lock-wait time or the latency of an individual DB call.
func (bc *BlockChain) PruneStateChangePostingChunk(ctx context.Context, prunedThrough, proofHead uint64, proofHash, anchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits) (rawdb.StateChangePostingPruneChunkResult, common.Hash, error) {
	result, nextAnchor, _, err := bc.pruneStateChangePostingChunk(ctx, prunedThrough, proofHead, proofHash, anchor, cursor, limits, nil)
	return result, nextAnchor, err
}

// PruneStateChangePostingChunkWithAdmission acquires resource admission only
// after queuing for chainmu. No maintenance lease is retained while waiting.
// Admission is mandatory here; the original entry point remains compatible for
// callers that already coordinate their own resource budget.
func (bc *BlockChain) PruneStateChangePostingChunkWithAdmission(ctx context.Context, prunedThrough, proofHead uint64, proofHash, anchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits, admit StateChangePostingPruneAdmission) (rawdb.StateChangePostingPruneChunkResult, common.Hash, StateChangePostingPruneTimings, error) {
	if admit == nil {
		return rawdb.StateChangePostingPruneChunkResult{NextCursor: bytes.Clone(cursor)}, anchor, StateChangePostingPruneTimings{}, errors.New("state-change posting prune: missing resource admission")
	}
	return bc.pruneStateChangePostingChunk(ctx, prunedThrough, proofHead, proofHash, anchor, cursor, limits, admit)
}

func (bc *BlockChain) pruneStateChangePostingChunk(ctx context.Context, prunedThrough, proofHead uint64, proofHash, anchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits, admit StateChangePostingPruneAdmission) (result rawdb.StateChangePostingPruneChunkResult, nextAnchor common.Hash, timing StateChangePostingPruneTimings, retErr error) {
	result = rawdb.StateChangePostingPruneChunkResult{NextCursor: bytes.Clone(cursor)}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, anchor, timing, err
	}
	if bc == nil || bc.db == nil || bc.chaindb == nil {
		return result, anchor, timing, errors.New("state-change posting prune: unavailable blockchain database")
	}
	if prunedThrough == 0 || proofHead < prunedThrough || proofHash == (common.Hash{}) {
		return result, anchor, timing, errors.New("state-change posting prune: invalid hot-prune proof")
	}
	if len(cursor) != 0 && anchor == (common.Hash{}) {
		return result, anchor, timing, errors.New("state-change posting prune: resumed cursor has no canonical anchor")
	}
	if !bc.stateHistoryIndexMu.TryLock() {
		return result, anchor, timing, ErrStateChangePostingPruneDeferred
	}
	defer bc.stateHistoryIndexMu.Unlock()
	waitStarted := time.Now()
	bc.chainmu.Lock()
	acquiredAt := time.Now()
	timing.ChainWait = acquiredAt.Sub(waitStarted)
	defer func() {
		timing.ChainHeld = time.Since(acquiredAt)
		bc.chainmu.Unlock()
	}()
	if err := ctx.Err(); err != nil {
		return result, anchor, timing, err
	}
	if bc.closed.Load() || bc.config == nil || !bc.config.HistoryEnabled {
		return result, anchor, timing, ErrStateChangePostingPruneDeferred
	}
	head := bc.CurrentBlock()
	if head == nil || head.Number() < proofHead {
		return result, anchor, timing, ErrStateChangePostingPruneDeferred
	}
	// Read only the published scalar, without copying the DP maps or falling
	// back to a state/commitment lookup under this maintenance critical section.
	solidified := int64(-1)
	bc.dynPropsCacheMu.RLock()
	if bc.dynPropsCache != nil {
		solidified = bc.dynPropsCache.LatestSolidifiedBlockNum()
	}
	bc.dynPropsCacheMu.RUnlock()
	if solidified < 0 || uint64(solidified) < prunedThrough {
		return result, anchor, timing, ErrStateChangePostingPruneDeferred
	}

	if admit != nil {
		started := time.Now()
		release, admitted := admit()
		timing.Admission = time.Since(started)
		// Defensively release an acquired lease even if a callback rejects.
		if release != nil {
			leaseStarted := time.Now()
			defer func() {
				release()
				timing.GateHeld = time.Since(leaseStarted)
			}()
		}
		if !admitted {
			return result, anchor, timing, ErrStateChangePostingPruneDeferred
		}
		if release == nil {
			return result, anchor, timing, errors.New("state-change posting prune: admission has no lease")
		}
		if err := ctx.Err(); err != nil {
			return result, anchor, timing, err
		}
	}
	proofStarted := time.Now()
	verifying := true
	defer func() {
		if verifying {
			timing.Proof = time.Since(proofStarted)
		}
	}()

	// At most four canonical heights are read: proof, Finish, index and H.
	// Reuse coincident heights without allocating a map. The existing strict
	// lookup decodes a canonical block, so its individual I/O is not time-bounded.
	var heights [4]uint64
	var hashes [4]common.Hash
	used := 0
	lookup := func(height uint64) (common.Hash, bool, error) {
		for i := 0; i < used; i++ {
			if heights[i] == height {
				return hashes[i], true, nil
			}
		}
		hash, ok, err := bc.readCanonicalHashStrict(height)
		if err == nil && ok && hash != (common.Hash{}) {
			heights[used], hashes[used] = height, hash
			used++
		}
		return hash, ok, err
	}
	canonicalProof, ok, err := lookup(proofHead)
	if err != nil {
		return result, anchor, timing, fmt.Errorf("state-change posting prune: read proof block %d: %w", proofHead, err)
	}
	if !ok || canonicalProof == (common.Hash{}) {
		return result, anchor, timing, fmt.Errorf("state-change posting prune: missing canonical proof block %d", proofHead)
	}
	if canonicalProof != proofHash {
		return result, anchor, timing, ErrStateChangePostingPruneBoundaryChanged
	}
	for _, requirement := range [...]struct {
		stage rawdb.StageID
		block uint64
	}{{rawdb.StageFinish, proofHead}, {rawdb.StageStateHistoryIndex, prunedThrough}} {
		block, exists, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, requirement.stage, lookup)
		if err != nil {
			return result, anchor, timing, fmt.Errorf("state-change posting prune: verify %s: %w", requirement.stage, err)
		}
		if !exists || block < requirement.block {
			return result, anchor, timing, ErrStateChangePostingPruneDeferred
		}
	}
	canonicalAnchor, ok, err := lookup(prunedThrough)
	if err != nil {
		return result, anchor, timing, fmt.Errorf("state-change posting prune: read boundary %d: %w", prunedThrough, err)
	}
	if !ok || canonicalAnchor == (common.Hash{}) {
		return result, anchor, timing, fmt.Errorf("state-change posting prune: missing canonical boundary %d", prunedThrough)
	}
	if anchor != (common.Hash{}) && anchor != canonicalAnchor {
		return result, anchor, timing, ErrStateChangePostingPruneBoundaryChanged
	}
	timing.Proof = time.Since(proofStarted)
	verifying = false
	result, err = rawdb.PruneStaleStateChangePostingChunkContext(ctx, bc.db, prunedThrough, cursor, limits)
	if err != nil {
		return result, anchor, timing, err
	}
	return result, canonicalAnchor, timing, nil
}
