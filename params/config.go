package params

import "github.com/tronprotocol/go-tron/common"

type ChainConfig struct {
	ChainID     int64
	P2PVersion  int32
	GenesisHash common.Hash
	// BlockVersion is written by witnesses into BlockHeader.raw.version.
	// Zero selects params.BlockVersion. It is chain-configurable because Nile
	// may advertise testnet-only software versions that mainnet must not claim.
	BlockVersion int32
	P2PPort      int
	HTTPPort     int
	GRPCPort     int
	JSONRPCPort  int
	// Java-tron config key: enery.limit.block.num.
	// A nil pointer means the java-tron default.
	BlockNumForEnergyLimit *int64
	// HistoryEnabled toggles flat temporal StateDomainChange capture. false
	// (the default) leaves applyBlock and StateDB on the zero-overhead fast
	// path — no per-mutation accounting, no per-block temporal flush. Explicit
	// prune modes and archive/snap defaults turn it on via node config; the gate
	// is independent of any java-tron proposal, so flipping it never affects
	// consensus.
	HistoryEnabled bool
	// HistoryMode is the retention policy for StateDomainChange/StateTxRange
	// rows captured by applyBlock. "full", "blocks", and "minimal" prune rows
	// older than their effective history window (recent-tip-only archive
	// coverage); "snap" prunes only after cold snapshot coverage; "archive"
	// retains every logical history row while allowing a verified immutable
	// cold copy to replace duplicate hot rows outside the recent window.
	//
	// Ignored when HistoryEnabled is false (no rows to prune, no archive to
	// keep).
	HistoryMode string
	// HistoryPruneWindow is the operator override for the number of recent
	// blocks of state history retained in finite prune modes. Temporal rows for
	// blocks below (solidified - window) become eligible for deletion by the
	// background pruner. When unset, defaults are mode-specific to match
	// Erigon's current retention policy. In archive mode this is the mutable hot
	// tail; it never limits historical query availability.
	HistoryPruneWindow uint64
	// BalanceTraceEnabled enables java-tron-specific BlockBalanceTrace and
	// AccountTrace capture. It is deliberately independent of flat temporal
	// state history: Ethereum-compatible historical state queries use
	// StateDomainChange rows and do not need these denormalized TRON traces.
	// Production chain configs leave this disabled; the offline balance-trace
	// replay tool enables it explicitly when rebuilding legacy trace data.
	BalanceTraceEnabled bool
}

const DefaultBlockNumForEnergyLimit int64 = 4_727_890

// History retention modes for ChainConfig.HistoryMode. "full", "blocks", and
// "minimal" prune hot history below the local window; "snap" and "archive"
// prune duplicate hot history only after cold snapshot coverage exists.
const (
	HistoryModeFull    = "full"
	HistoryModeSnap    = "snap"
	HistoryModeArchive = "archive"
	HistoryModeBlocks  = "blocks"
	HistoryModeMinimal = "minimal"
)

// HistoryDefaultPruneWindow is the default hot state-history window for
// full/blocks/snap/archive modes. Archive retains older logical history in the
// verified cold layer. It follows Erigon 3.5's EIP-8252 retention window for
// full and blocks mode: 262,144 blocks.
const HistoryDefaultPruneWindow uint64 = 262_144

// HistoryColdDefaultPruneWindow is the fresh-genesis snap/archive hot duplicate
// window. Verified cold history remains permanent, so retaining more than one
// 65,536-block immutable segment in Pebble only duplicates data. At TRON's
// three-second cadence this still leaves roughly 2.3 days of local history and
// is far wider than the reorg safety window.
const HistoryColdDefaultPruneWindow uint64 = 65_536

// HistoryMinimalDefaultPruneWindow is the default finite-prune state history
// window for minimal mode. Erigon 3.5 kept minimal at 100,000 blocks while
// widening full and blocks mode.
const HistoryMinimalDefaultPruneWindow uint64 = 100_000

func chainConfigInt64(v int64) *int64 { return &v }

func (c *ChainConfig) EnergyLimitForkBlockNum() int64 {
	if c != nil && c.BlockNumForEnergyLimit != nil {
		return *c.BlockNumForEnergyLimit
	}
	return DefaultBlockNumForEnergyLimit
}

func (c *ChainConfig) EffectiveBlockVersion() int32 {
	if c != nil && c.BlockVersion != 0 {
		return c.BlockVersion
	}
	return BlockVersion
}

// EffectiveHistoryMode returns the resolved retention mode: blank /
// unrecognised values normalise to HistoryModeFull so the pruner is always
// conservative by default. Archive/snap operators must opt in explicitly.
func (c *ChainConfig) EffectiveHistoryMode() string {
	if c == nil || c.HistoryMode == "" {
		return HistoryModeFull
	}
	switch c.HistoryMode {
	case HistoryModeArchive:
		return HistoryModeArchive
	case HistoryModeSnap:
		return HistoryModeSnap
	case HistoryModeBlocks:
		return HistoryModeBlocks
	case HistoryModeMinimal:
		return HistoryModeMinimal
	default:
		return HistoryModeFull
	}
}

// DefaultHistoryPruneWindowForMode returns the zero-config hot retention
// window. Unknown modes intentionally fall back to full mode's default,
// matching EffectiveHistoryMode's conservative normalisation.
func DefaultHistoryPruneWindowForMode(mode string) uint64 {
	if mode == HistoryModeMinimal {
		return HistoryMinimalDefaultPruneWindow
	}
	if mode == HistoryModeSnap || mode == HistoryModeArchive {
		return HistoryColdDefaultPruneWindow
	}
	return HistoryDefaultPruneWindow
}

// EffectiveHistoryPruneWindow returns the active hot retention window. Zero
// (the field default for unconfigured chains) falls back to the mode-specific
// default so test fixtures and dev chains get the same safety margin without
// per-test boilerplate.
func (c *ChainConfig) EffectiveHistoryPruneWindow() uint64 {
	if c == nil || c.HistoryPruneWindow == 0 {
		mode := HistoryModeFull
		if c != nil {
			mode = c.EffectiveHistoryMode()
		}
		return DefaultHistoryPruneWindowForMode(mode)
	}
	return c.HistoryPruneWindow
}

var MainnetChainConfig = &ChainConfig{
	ChainID:                1,
	BlockVersion:           BlockVersion,
	P2PVersion:             11111,
	P2PPort:                18888,
	HTTPPort:               8090,
	GRPCPort:               50051,
	JSONRPCPort:            8545,
	BlockNumForEnergyLimit: chainConfigInt64(DefaultBlockNumForEnergyLimit),
}

var NileChainConfig = &ChainConfig{
	ChainID: 3448148188,
	// Nile's PQ1 build advertises VERSION_4_8_2_PQ1 while mainnet remains on
	// VERSION_4_8_2. Keep this chain-specific: version 37 and its PQ proposals
	// are Nile-only until java-tron deploys them elsewhere.
	BlockVersion:           37,
	GenesisHash:            NileGenesisHash,
	P2PVersion:             201910292,
	P2PPort:                18888,
	HTTPPort:               8090,
	GRPCPort:               50051,
	JSONRPCPort:            8545,
	BlockNumForEnergyLimit: chainConfigInt64(DefaultBlockNumForEnergyLimit),
}
