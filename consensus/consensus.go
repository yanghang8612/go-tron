package consensus

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
)

type Engine interface {
	VerifyHeader(chain ChainReader, header *types.Block) error
	GetScheduledWitness(slot int64) (common.Address, error)
	IsInMaintenance(timestamp int64) bool
}

type ChainReader interface {
	CurrentBlock() *types.Block
	GetBlockByNumber(number uint64) *types.Block
	GenesisTimestamp() int64
	ActiveWitnesses() []common.Address
	NextMaintenanceTime() int64
}
