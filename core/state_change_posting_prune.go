package core

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

var (
	// ErrStateChangePostingPruneDeferred leaves the caller's in-memory traversal
	// unchanged while a writer owns the locks or the required prefix is unavailable.
	ErrStateChangePostingPruneDeferred = errors.New("state-change posting prune deferred")
	// ErrStateChangePostingPruneBoundaryChanged invalidates the current traversal
	// and its hot-prune permission. A fresh permission is required before retrying.
	ErrStateChangePostingPruneBoundaryChanged = errors.New("state-change posting prune boundary changed")
)

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
// Limits bound work between rows, not the latency of an individual DB call.
func (bc *BlockChain) PruneStateChangePostingChunk(ctx context.Context, prunedThrough, proofHead uint64, proofHash, anchor common.Hash, cursor []byte, limits rawdb.StateChangePostingPruneLimits) (rawdb.StateChangePostingPruneChunkResult, common.Hash, error) {
	result := rawdb.StateChangePostingPruneChunkResult{NextCursor: bytes.Clone(cursor)}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return result, anchor, err
	}
	if bc == nil || bc.db == nil || bc.chaindb == nil {
		return result, anchor, errors.New("state-change posting prune: unavailable blockchain database")
	}
	if prunedThrough == 0 || proofHead < prunedThrough || proofHash == (common.Hash{}) {
		return result, anchor, errors.New("state-change posting prune: invalid hot-prune proof")
	}
	if len(cursor) != 0 && anchor == (common.Hash{}) {
		return result, anchor, errors.New("state-change posting prune: resumed cursor has no canonical anchor")
	}
	if !bc.stateHistoryIndexMu.TryLock() {
		return result, anchor, ErrStateChangePostingPruneDeferred
	}
	defer bc.stateHistoryIndexMu.Unlock()
	if !bc.chainmu.TryLock() {
		return result, anchor, ErrStateChangePostingPruneDeferred
	}
	defer bc.chainmu.Unlock()
	if err := ctx.Err(); err != nil {
		return result, anchor, err
	}
	if bc.closed.Load() || bc.config == nil || !bc.config.HistoryEnabled {
		return result, anchor, ErrStateChangePostingPruneDeferred
	}
	head := bc.CurrentBlock()
	if head == nil || head.Number() < proofHead {
		return result, anchor, ErrStateChangePostingPruneDeferred
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
		return result, anchor, ErrStateChangePostingPruneDeferred
	}

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
		return result, anchor, fmt.Errorf("state-change posting prune: read proof block %d: %w", proofHead, err)
	}
	if !ok || canonicalProof == (common.Hash{}) {
		return result, anchor, fmt.Errorf("state-change posting prune: missing canonical proof block %d", proofHead)
	}
	if canonicalProof != proofHash {
		return result, anchor, ErrStateChangePostingPruneBoundaryChanged
	}
	for _, requirement := range [...]struct {
		stage rawdb.StageID
		block uint64
	}{{rawdb.StageFinish, proofHead}, {rawdb.StageStateHistoryIndex, prunedThrough}} {
		block, exists, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, requirement.stage, lookup)
		if err != nil {
			return result, anchor, fmt.Errorf("state-change posting prune: verify %s: %w", requirement.stage, err)
		}
		if !exists || block < requirement.block {
			return result, anchor, ErrStateChangePostingPruneDeferred
		}
	}
	canonicalAnchor, ok, err := lookup(prunedThrough)
	if err != nil {
		return result, anchor, fmt.Errorf("state-change posting prune: read boundary %d: %w", prunedThrough, err)
	}
	if !ok || canonicalAnchor == (common.Hash{}) {
		return result, anchor, fmt.Errorf("state-change posting prune: missing canonical boundary %d", prunedThrough)
	}
	if anchor != (common.Hash{}) && anchor != canonicalAnchor {
		return result, anchor, ErrStateChangePostingPruneBoundaryChanged
	}
	result, err = rawdb.PruneStaleStateChangePostingChunkContext(ctx, bc.db, prunedThrough, cursor, limits)
	if err != nil {
		return result, anchor, err
	}
	return result, canonicalAnchor, nil
}
