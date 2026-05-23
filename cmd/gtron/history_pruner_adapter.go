package main

import (
	"github.com/ethereum/go-ethereum/ethdb"

	"github.com/tronprotocol/go-tron/core"
	"github.com/tronprotocol/go-tron/core/historyprune"
	statepruning "github.com/tronprotocol/go-tron/core/state/pruning"
)

// prunerChainSource adapts *core.BlockChain to the narrow
// historyprune.ChainSource interface. The pruner only needs the disk KV
// store handle and the most-recently-solidified block number; an
// adapter keeps the pruner's test surface unchanged and lets a future
// rework of BlockChain accessors flow through this single shim.
type prunerChainSource struct {
	chain *core.BlockChain
}

func newPrunerChainSource(chain *core.BlockChain) historyprune.ChainSource {
	return &prunerChainSource{chain: chain}
}

func newDomainPrunerChainSource(chain *core.BlockChain) statepruning.ChainSource {
	return &prunerChainSource{chain: chain}
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
