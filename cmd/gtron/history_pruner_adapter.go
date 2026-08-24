package main

import (
	"time"

	"github.com/ethereum/go-ethereum/ethdb"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/rawdb"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	tnet "github.com/tronprotocol/go-tron/net"
	"github.com/tronprotocol/go-tron/params"
)

// prunerChainSource adapts *core.BlockChain to the narrow domain-state
// pruning interface. The pruner only needs the disk KV store handle and the
// most-recently-solidified block number.
type prunerChainSource struct {
	chain *core.BlockChain
}

type domainPrunerChainSource struct {
	*prunerChainSource
	sync *tnet.SyncService
}

type stateSnapshotChainSource struct {
	chain *core.BlockChain
}

func newDomainPrunerChainSource(chain *core.BlockChain, syncService *tnet.SyncService) statepruning.ChainSource {
	return &domainPrunerChainSource{
		prunerChainSource: &prunerChainSource{chain: chain},
		sync:              syncService,
	}
}

func newStateSnapshotChainSource(chain *core.BlockChain) statesnapshots.ChainSource {
	return &stateSnapshotChainSource{chain: chain}
}

func (a *prunerChainSource) DB() ethdb.KeyValueStore {
	return a.chain.DB()
}

func (a *prunerChainSource) EventLogDB() *rawdb.ChainDB {
	if a == nil || a.chain == nil {
		return nil
	}
	return a.chain.ChainDB()
}

func (a *prunerChainSource) LatestSolidifiedBlockNum() int64 {
	// DynProps reads through the in-memory applyBlock buffer; the
	// solidified counter is consensus-derived and rarely lags by more
	// than one block under steady-state. Reading it once per prune pass
	// is bounded by the pass's Interval (default 1 minute), so allocator
	// pressure is negligible.
	return a.chain.DynProps().LatestSolidifiedBlockNum()
}

func (a *prunerChainSource) CanonicalBlockHash(blockNum uint64) (common.Hash, bool) {
	hash, ok, err := a.CanonicalBlockHashStrict(blockNum)
	if err != nil {
		return common.Hash{}, false
	}
	return hash, ok
}

func (a *prunerChainSource) CanonicalBlockHashStrict(blockNum uint64) (common.Hash, bool, error) {
	if a == nil || a.chain == nil {
		return common.Hash{}, false, nil
	}
	return rawdb.ReadBlockHashByNumberStrict(a.chain.ChainDB(), blockNum)
}

func (a *stateSnapshotChainSource) DB() statesnapshots.AggregatorDB {
	return a.chain.DB()
}

func (a *stateSnapshotChainSource) LatestSolidifiedBlockNum() int64 {
	return a.chain.DynProps().LatestSolidifiedBlockNum()
}

func (a *stateSnapshotChainSource) CanonicalBlockHash(blockNum uint64) (common.Hash, bool) {
	hash, ok, err := a.CanonicalBlockHashStrict(blockNum)
	if err != nil {
		return common.Hash{}, false
	}
	return hash, ok
}

func (a *stateSnapshotChainSource) CanonicalBlockHashStrict(blockNum uint64) (common.Hash, bool, error) {
	if a == nil || a.chain == nil {
		return common.Hash{}, false, nil
	}
	return rawdb.ReadBlockHashByNumberStrict(a.chain.ChainDB(), blockNum)
}

func (a *domainPrunerChainSource) SyncRemainingBlocks() (uint64, bool) {
	if a == nil {
		return 0, false
	}
	if a.sync != nil {
		if remaining, ok := a.sync.SyncRemainingBlocks(); ok && remaining > 0 {
			return uint64(remaining), true
		}
	}
	// SyncService sessions are in-memory and are re-established only after a
	// post-restart peer handshake. Preserve the deep-sync maintenance gate in
	// that startup window by falling back to the durable highest explicit
	// inventory tip. Normal completion leaves the tip at or behind canonical
	// head, so it cannot suppress the sync-complete catch-up pass.
	if a.chain == nil || a.chain.CurrentBlock() == nil {
		return 0, false
	}
	head := a.chain.CurrentBlock().Number()
	return persistedSyncRemainingBlocksBounded(a.chain.DB(), head, plausibleSyncTarget(a.chain, time.Now()))
}

func plausibleSyncTarget(chain *core.BlockChain, now time.Time) uint64 {
	if chain == nil {
		return 0
	}
	head := chain.CurrentBlock()
	if head == nil || head.Timestamp() < 0 {
		return 0
	}
	projectTo := now.UnixMilli() + time.Hour.Milliseconds()
	if head.Timestamp() >= projectTo {
		return head.Number()
	}
	elapsed := uint64(projectTo - head.Timestamp())
	additional := elapsed / uint64(params.BlockProducedInterval)
	if additional > ^uint64(0)-head.Number() {
		return ^uint64(0)
	}
	return head.Number() + additional
}

func persistedSyncRemainingBlocks(db ethdb.KeyValueReader, head uint64) (uint64, bool) {
	return persistedSyncRemainingBlocksBounded(db, head, 0)
}

func persistedSyncRemainingBlocksBounded(db ethdb.KeyValueReader, head, maxTarget uint64) (uint64, bool) {
	if db == nil {
		return 0, false
	}
	target, ok, err := rawdb.ReadStageProgress(db, rawdb.StageSyncInventory)
	if err != nil || !ok || target <= head || (maxTarget > 0 && target > maxTarget) {
		return 0, false
	}
	return target - head, true
}

func (a *domainPrunerChainSource) BeginCommitmentBranchRotation() (rawdb.CommitmentBranchRotation, bool, error) {
	if a == nil || a.chain == nil {
		return rawdb.CommitmentBranchRotation{}, false, nil
	}
	return a.chain.BeginCommitmentBranchRotation()
}

func (a *domainPrunerChainSource) CompleteCommitmentBranchRotation(rotation rawdb.CommitmentBranchRotation, mgr *statesnapshots.Manager) error {
	if a == nil || a.chain == nil {
		return nil
	}
	return a.chain.CompleteCommitmentBranchRotation(rotation, mgr)
}
