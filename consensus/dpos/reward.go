package dpos

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
)

func PayBlockReward(chain consensus.ChainHeaderWriter, witness common.Address) {
	chain.AddAllowance(witness, chain.WitnessPayPerBlock())
}
