package dpos

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/consensus"
	"github.com/tronprotocol/go-tron/core/state"
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

// PrewarmHeaderSignature warms the block's witness-recovery memo, performing the
// (otherwise serial) SR-signature ECDSA recovery ahead of VerifyHeaderWithDynProps.
// It makes no accept/reject decision — VerifyHeaderWithDynProps still owns every
// header check and re-reads the recovered signer from the same memo. Errors are
// captured in the memo and surfaced (identically) by verification. Used by the
// parallel pre-verification pass in core; satisfies the optional
// headerSignaturePrewarmer interface there via duck typing.
func (d *DPoS) PrewarmHeaderSignature(block *types.Block) {
	if block != nil && block.Proto().GetBlockHeader().GetPqAuthSig() != nil {
		return
	}
	_, _ = block.CachedRecoveredWitness(recoverWitness)
}

// VerifyHeaderWithDynProps lets hot-path callers (applyBlock) thread an
// already-loaded dynamic-properties snapshot through verification, avoiding
// the redundant LoadDynamicProperties that the chain.DynProps() fallback in
// VerifyHeader would perform.
func (d *DPoS) VerifyHeaderWithDynProps(chain consensus.ChainReader, block *types.Block, dp *state.DynamicProperties) error {
	return VerifyHeaderWithDynProps(chain, block, dp)
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
