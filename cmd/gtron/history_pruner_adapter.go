package main

import (
	"github.com/ethereum/go-ethereum/ethdb"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	tnet "github.com/tronprotocol/go-tron/net"
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

func (a *prunerChainSource) LatestSolidifiedBlockNum() int64 {
	// DynProps reads through the in-memory applyBlock buffer; the
	// solidified counter is consensus-derived and rarely lags by more
	// than one block under steady-state. Reading it once per prune pass
	// is bounded by the pass's Interval (default 1 minute), so allocator
	// pressure is negligible.
	return a.chain.DynProps().LatestSolidifiedBlockNum()
}

func (a *prunerChainSource) CanonicalBlockHash(blockNum uint64) (common.Hash, bool) {
	block := a.chain.GetBlockByNumber(blockNum)
	if block == nil {
		return common.Hash{}, false
	}
	return block.Hash(), true
}

func (a *stateSnapshotChainSource) DB() statesnapshots.AggregatorDB {
	return a.chain.DB()
}

func (a *stateSnapshotChainSource) LatestSolidifiedBlockNum() int64 {
	return a.chain.DynProps().LatestSolidifiedBlockNum()
}

func (a *domainPrunerChainSource) SyncRemainingBlocks() (uint64, bool) {
	if a == nil || a.sync == nil {
		return 0, false
	}
	remaining, ok := a.sync.SyncRemainingBlocks()
	if !ok || remaining <= 0 {
		return 0, false
	}
	return uint64(remaining), true
}
