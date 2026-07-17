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
	// path — no per-mutation accounting, no per-block temporal flush. Archive
	// operators opt in via node config; the gate is independent of any
	// java-tron proposal, so flipping it never affects consensus.
	HistoryEnabled bool
	// HistoryMode is the retention policy for StateDomainChange/StateTxRange
	// rows captured by applyBlock. "full" prunes rows older than
	// HistoryPruneWindow blocks (the default — recent-tip-only archive
	// coverage); "archive" keeps every row forever.
	//
	// Ignored when HistoryEnabled is false (no rows to prune, no archive to
	// keep).
	HistoryMode string
	// HistoryPruneWindow is the number of recent blocks of state history
	// retained in "full" mode. Temporal rows for blocks below (solidified -
	// window) become eligible for deletion by the background pruner. The default
	// (HistoryDefaultPruneWindow) is sized at 27 maintenance rounds × ~1K
	// slots per round, generous enough to cover the reorg horizon and a
	// day-or-two wallet-tx grace window. Ignored in archive mode.
	HistoryPruneWindow uint64
	// StateCommitmentCheckpoints enables optional latest-domain commitment
	// checkpoint rows after each block. The block state root itself is already
	// the CommitmentDomain root.
	StateCommitmentCheckpoints bool
	// StateCommitmentMode is retained as an operator-facing label. Fresh nodes
	// always use "latest": flat latest domains plus CommitmentDomain root.
	StateCommitmentMode string
}

const DefaultBlockNumForEnergyLimit int64 = 4_727_890

// History retention modes for ChainConfig.HistoryMode. "full" prunes hot
// history below the local window; "snap" prunes hot history only after cold
// snapshot coverage exists; "archive" keeps every row.
const (
	HistoryModeFull    = "full"
	HistoryModeSnap    = "snap"
	HistoryModeArchive = "archive"
)

// State commitment modes. Latest is the only fresh-node state layout:
// flat latest domains plus CommitmentDomain root.
const (
	StateCommitmentModeLatest = "latest"
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

// EffectiveStateCommitmentMode returns the active commitment mode. Blank and
// unrecognised values normalise to latest; new databases do not preserve the
// old per-block MPT materialisation path by default.
func (c *ChainConfig) EffectiveStateCommitmentMode() string {
	return StateCommitmentModeLatest
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
