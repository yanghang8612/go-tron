package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type ProposalCreateActuator struct{}

func (a *ProposalCreateActuator) getContract(ctx *Context) (*contractpb.ProposalCreateContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ProposalCreateContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ProposalCreateContract")
	}
	return c, nil
}

func (a *ProposalCreateActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !witnessExists(ctx, ownerAddr) {
		return errors.New("owner is not a witness")
	}
	if len(c.Parameters) == 0 {
		return errors.New("proposal parameters are empty")
	}
	for id, value := range c.Parameters {
		if err := validateProposalParameter(ctx, id, value); err != nil {
			return err
		}
	}
	return nil
}

func (a *ProposalCreateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}

	proposalID := ctx.DynProps.LatestProposalNum() + 1
	ctx.DynProps.SetLatestProposalNum(proposalID)

	// Expiration aligns to the first maintenance boundary strictly after
	// (prevBlockTime + proposalExpireTime), matching java-tron
	// ProposalCreateActuator. java reads getLatestBlockHeaderTimestamp()
	// here, which during processTransaction still holds the previous
	// block's timestamp (Manager.applyBlock runs updateDynamicProperties
	// only *after* processTransaction).
	now3 := ctx.PrevBlockTime + ctx.DynProps.ProposalExpireTime()
	nextMaintenance := ctx.DynProps.NextMaintenanceTime()
	interval := ctx.DynProps.MaintenanceTimeInterval()
	var expirationTime int64
	if interval > 0 {
		round := (now3 - nextMaintenance) / interval
		expirationTime = nextMaintenance + (round+1)*interval
	} else {
		expirationTime = now3
	}

	proposal := &rawdb.Proposal{
		ID:             proposalID,
		Proposer:       ownerAddr,
		Parameters:     c.Parameters,
		CreateTime:     ctx.PrevBlockTime,
		ExpirationTime: expirationTime,
		State:          rawdb.ProposalStatePending,
	}

	// Stage the proposal record + index entry into the rooted system-KV
	// (Phase 3d). The same *StateDB is read by approve/delete/maintenance
	// later this block, the write is journaled (rolls back if this tx
	// reverts), and it rewinds with the state root on a fork rewind.
	if err := ctx.State.WriteProposal(proposalID, proposal); err != nil {
		return nil, err
	}
	if err := ctx.State.AppendProposalIndex(proposalID); err != nil {
		return nil, err
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
