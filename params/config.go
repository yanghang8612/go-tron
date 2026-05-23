package params

import "github.com/tronprotocol/go-tron/common"

type ChainConfig struct {
	ChainID     int64
	P2PVersion  int32
	GenesisHash common.Hash
	P2PPort     int
	HTTPPort    int
	GRPCPort    int
	JSONRPCPort int
	// Java-tron config key: enery.limit.block.num.
	// A nil pointer means the java-tron default.
	BlockNumForEnergyLimit *int64
	// HistoryEnabled toggles the State History Index (SHI) capture path.
	// false (the default) leaves applyBlock and StateDB on the zero-overhead
	// fast path — no per-mutation accounting, no per-block flush. Archive
	// operators opt in via node config; the gate is independent of any
	// java-tron proposal, so flipping it never affects consensus.
	HistoryEnabled bool
	// HistoryMode is the retention policy for State History Index rows
	// captured by applyBlock. "full" prunes rows older than HistoryPruneWindow
	// blocks (the default — recent-tip-only archive coverage); "archive"
	// keeps every row forever. Slice 5's background pruner consults this
	// field at construction time and skips registration entirely in archive
	// mode so history grows linearly with chain length.
	//
	// Ignored when HistoryEnabled is false (no rows to prune, no archive to
	// keep).
	HistoryMode string
	// HistoryPruneWindow is the number of recent blocks of state history
	// retained in "full" mode. Rows for blocks below (solidified - window)
	// become eligible for deletion by the background pruner. The default
	// (HistoryDefaultPruneWindow) is sized at 27 maintenance rounds × ~1K
	// slots per round, generous enough to cover the reorg horizon and a
	// day-or-two wallet-tx grace window. Ignored in archive mode.
	HistoryPruneWindow uint64
	// StateCommitmentCheckpoints enables the transitional Erigon-style domain
	// commitment checkpoint writer. It computes a deterministic debug root over
	// physical latest-domain rows after each block. The computation is
	// intentionally opt-in until the incremental commitment domain replaces
	// nested-MPT root building.
	StateCommitmentCheckpoints bool
}

const DefaultBlockNumForEnergyLimit int64 = 4_727_890

// History retention modes for ChainConfig.HistoryMode. "full" prunes; any
// other value (typically "archive") disables the pruner and keeps every
// row. The CLI / TOML loaders normalise unknown values upstream.
const (
	HistoryModeFull    = "full"
	HistoryModeArchive = "archive"
)

// HistoryDefaultPruneWindow is the default retention window for "full"
// mode. 27_648 ≈ 27 maintenance rounds × 1024 slots per round — covers the
// reorg horizon with a generous wallet-tx grace window. See the spec's
// Pruning section for the sizing rationale.
const HistoryDefaultPruneWindow uint64 = 27 * 1024

func chainConfigInt64(v int64) *int64 { return &v }

func (c *ChainConfig) EnergyLimitForkBlockNum() int64 {
	if c != nil && c.BlockNumForEnergyLimit != nil {
		return *c.BlockNumForEnergyLimit
	}
	return DefaultBlockNumForEnergyLimit
}

// EffectiveHistoryMode returns the resolved retention mode: blank /
// unrecognised values normalise to HistoryModeFull so the pruner is
// always wired in by default. Archive operators must opt in explicitly
// via "archive".
func (c *ChainConfig) EffectiveHistoryMode() string {
	if c == nil || c.HistoryMode == "" {
		return HistoryModeFull
	}
	switch c.HistoryMode {
	case HistoryModeArchive:
		return HistoryModeArchive
	default:
		return HistoryModeFull
	}
}

// EffectiveHistoryPruneWindow returns the active full-mode retention
// window. Zero (the field default for unconfigured chains) falls back to
// HistoryDefaultPruneWindow so test fixtures and dev chains get the same
// safety margin without per-test boilerplate.
func (c *ChainConfig) EffectiveHistoryPruneWindow() uint64 {
	if c == nil || c.HistoryPruneWindow == 0 {
		return HistoryDefaultPruneWindow
	}
	return c.HistoryPruneWindow
}

var MainnetChainConfig = &ChainConfig{
	ChainID:                1,
	P2PVersion:             11111,
	P2PPort:                18888,
	HTTPPort:               8090,
	GRPCPort:               50051,
	JSONRPCPort:            8545,
	BlockNumForEnergyLimit: chainConfigInt64(DefaultBlockNumForEnergyLimit),
}

var NileChainConfig = &ChainConfig{
	ChainID:                3448148188,
	GenesisHash:            NileGenesisHash,
	P2PVersion:             201910292,
	P2PPort:                18888,
	HTTPPort:               8090,
	GRPCPort:               50051,
	JSONRPCPort:            8545,
	BlockNumForEnergyLimit: chainConfigInt64(DefaultBlockNumForEnergyLimit),
}
