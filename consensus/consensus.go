package consensus

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

type Engine interface {
	VerifyHeader(chain ChainReader, header *types.Block) error
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
}

type ChainHeaderWriter interface {
	GetWitness(addr common.Address) *types.Witness
	PutWitness(w *types.Witness)
	AddAllowance(addr common.Address, amount int64)
	NextMaintenanceTime() int64
	SetNextMaintenanceTime(t int64)
	WitnessPayPerBlock() int64
	WitnessStandbyAllowance() int64
	MaintenanceTimeInterval() int64
}
