package consensus

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
)

type Engine interface {
	VerifyHeader(chain ChainReader, header *types.Block) error
	// VerifyHeaderWithDynProps verifies a block header against the given
	// dynamic-properties snapshot. The caller (typically applyBlock) is
	// responsible for loading dp from the buffer-overlay reader; this avoids
	// the duplicate LoadDynamicProperties that the chain.DynProps() fallback
	// would otherwise perform. Engines that don't take a dp shortcut may
	// delegate to VerifyHeader internally.
	VerifyHeaderWithDynProps(chain ChainReader, header *types.Block, dp *state.DynamicProperties) error
	GetScheduledWitness(slot int64) (common.Address, error)
	IsInMaintenance(timestamp int64) bool
	DoMaintenance(chain ChainHeaderWriter) error
	PayBlockReward(chain ChainHeaderWriter, witness common.Address)
}

type ChainReader interface {
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) *types.Block
	GenesisTimestamp() int64
	ActiveWitnesses() []common.Address
	NextMaintenanceTime() int64
	DynProps() *state.DynamicProperties
}

type ChainHeaderWriter interface {
	GetWitness(addr common.Address) *types.Witness
	PutWitness(w *types.Witness)
	AddWitnessVoteCount(addr common.Address, delta int64)
	AddAllowance(addr common.Address, amount int64)
	NextMaintenanceTime() int64
	SetNextMaintenanceTime(t int64)
	WitnessPayPerBlock() int64
	WitnessStandbyAllowance() int64
	MaintenanceTimeInterval() int64
	// ChangeDelegation reports whether the "new reward algorithm" switch
	// is on. When true, maintenance skips the legacy IncentiveManager.reward
	// path (per-block payStandbyWitness handles standby pay instead).
	ChangeDelegation() bool

	// GenesisWitnesses returns the immutable list of {address, initial
	// vote count} captured at genesis setup. Used by tryRemoveThePowerOfTheGr
	// to subtract each GR's initial 100M vote stake when the corresponding
	// DP flag fires.
	GenesisWitnesses() []GenesisWitnessInfo
	RemoveThePowerOfTheGr() int64
	SetRemoveThePowerOfTheGr(v int64)
}

// GenesisWitnessInfo carries an immutable genesis witness record into the
// consensus layer. Kept structurally identical to rawdb.GenesisWitness so the
// adapter can forward without a translation step, while avoiding an import
// cycle (rawdb depends on common; consensus stays primitive-only).
type GenesisWitnessInfo struct {
	Address   common.Address
	VoteCount int64
}
