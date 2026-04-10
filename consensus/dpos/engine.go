package dpos

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

var ErrNoActiveWitnesses = errors.New("no active witnesses")

type DPoS struct {
	chain consensus.ChainReader
}

func New(chain consensus.ChainReader) *DPoS {
	return &DPoS{chain: chain}
}

func (d *DPoS) VerifyHeader(chain consensus.ChainReader, block *types.Block) error {
	return VerifyHeader(chain, block)
}

func (d *DPoS) GetScheduledWitness(slot int64) (common.Address, error) {
	witnesses := d.chain.ActiveWitnesses()
	if len(witnesses) == 0 {
		return common.Address{}, ErrNoActiveWitnesses
	}
	head := d.chain.CurrentBlock()
	addr := GetScheduledWitness(slot, head.Timestamp(), d.chain.GenesisTimestamp(), witnesses,
		d.IsInMaintenance(head.Timestamp()), params.MaintenanceSkipSlots)
	return addr, nil
}

func (d *DPoS) IsInMaintenance(timestamp int64) bool {
	maintTime := d.chain.NextMaintenanceTime()
	if maintTime <= 0 {
		return false
	}
	return timestamp >= maintTime
}

func (d *DPoS) DoMaintenance(chain consensus.ChainHeaderWriter) error {
	return nil
}

func (d *DPoS) PayBlockReward(chain consensus.ChainHeaderWriter, witness common.Address) {
	PayBlockReward(chain, witness)
}
