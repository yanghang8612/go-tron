package core

import (
	"context"
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
)

// TryWithStateDomainChangePruneGuard protects one bounded hot-history range
// deletion. The caller must first select cold-covered blocks <= through and
// capture proofHead/proofHash before that selection. This guard does not prove
// cold coverage itself. work must synchronously scan, delete and flush its batch
// before returning; it must not acquire chain/index locks or wait for a lease.
//
// Only lock contention returns (false, nil). Invalid or unavailable proof is an
// error, so it must not authorize an unguarded fallback. A caller may retain its
// existing point-delete behavior on contention, but that fallback receives none
// of this guard's serialization or proof guarantees. Once work runs, the return
// value is true even when work fails.
//
// The lock order matches the history index builder and Close. Both acquisitions
// are opportunistic: a caller already holding a maintenance lease never queues
// for the importer. Both locks remain held through work's final batch flush.
// Existing async flushes need not be drained. A hash-verified durable Finish
// together with a settled buffer prefix excludes an old group still retained
// for writing or retry. Subsequent groups and new writers target later immutable
// block keys. This does not permit
// concurrent offline repair/restore or writers that rewrite the old prefix.
// Block and stage reads can wait for storage; there is no hard I/O timeout.
func (bc *BlockChain) TryWithStateDomainChangePruneGuard(ctx context.Context, through, proofHead uint64, proofHash common.Hash, work func() error) (bool, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if bc == nil || bc.db == nil || bc.chaindb == nil {
		return false, errors.New("state domain change prune: unavailable blockchain database")
	}
	if through == 0 || proofHead < through || proofHash == (common.Hash{}) || work == nil {
		return false, errors.New("state domain change prune: invalid proof or work callback")
	}
	if !bc.stateHistoryIndexMu.TryLock() {
		return false, nil
	}
	defer bc.stateHistoryIndexMu.Unlock()
	if !bc.chainmu.TryLock() {
		return false, nil
	}
	defer bc.chainmu.Unlock()
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if bc.closed.Load() || bc.config == nil || !bc.config.HistoryEnabled {
		return false, errors.New("state domain change prune: blockchain closed or history disabled")
	}
	if err := bc.commitErr.Load(); err != nil {
		return false, fmt.Errorf("state domain change prune: async commit failed: %w", *err)
	}
	if err := bc.flushErr.Load(); err != nil {
		return false, fmt.Errorf("state domain change prune: async flush failed: %w", *err)
	}
	head := bc.CurrentBlock()
	if head == nil || head.Number() < proofHead {
		return false, errors.New("state domain change prune: current head behind proof")
	}
	solidified := int64(-1)
	bc.dynPropsCacheMu.RLock()
	if bc.dynPropsCache != nil {
		solidified = bc.dynPropsCache.LatestSolidifiedBlockNum()
	}
	bc.dynPropsCacheMu.RUnlock()
	if solidified < 0 || uint64(solidified) < through {
		return false, errors.New("state domain change prune: solidified head behind selected blocks")
	}
	// Strict canonical lookups decode whole blocks. Cache coincident heights to
	// keep verification bounded to proof, Finish, index and through.
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
		return false, fmt.Errorf("state domain change prune: read proof block %d: %w", proofHead, err)
	}
	if !ok || canonicalProof == (common.Hash{}) || canonicalProof != proofHash {
		return false, errors.New("state domain change prune: canonical proof changed or unavailable")
	}
	for _, requirement := range [...]struct {
		stage rawdb.StageID
		block uint64
	}{{rawdb.StageFinish, proofHead}, {rawdb.StageStateHistoryIndex, through}} {
		block, exists, err := rawdb.ReadVerifiedStageProgressBlockWithHashLookup(bc.db, requirement.stage, lookup)
		if err != nil {
			return false, fmt.Errorf("state domain change prune: verify %s: %w", requirement.stage, err)
		}
		if !exists || block < requirement.block {
			return false, fmt.Errorf("state domain change prune: %s does not cover required block %d", requirement.stage, requirement.block)
		}
	}
	boundary, ok, err := lookup(through)
	if err != nil {
		return false, fmt.Errorf("state domain change prune: read selected boundary %d: %w", through, err)
	}
	if !ok || boundary == (common.Hash{}) {
		return false, errors.New("state domain change prune: selected canonical boundary unavailable")
	}
	// Finish can already be visible while a flush group is still retained, or
	// after an ambiguous batch failure. Reject old buffered layers so a later
	// write/retry cannot reintroduce rows or create an unscanned interior key.
	// chainmu excludes new old-height layers; async workers only advance the
	// existing later layers after this observation. Do not wait for flushMu.
	if bc.buffer == nil || !bc.buffer.HistoryPrefixSettled(through) {
		return false, errors.New("state domain change prune: selected buffer prefix is not settled")
	}
	if err := ctx.Err(); err != nil {
		return false, err
	}
	return true, work()
}
