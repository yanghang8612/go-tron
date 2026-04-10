package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

const proposalExpirationMs = 259200000 // 3 days in ms

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
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if !isActiveWitness(ownerAddr, ctx.ActiveWitnesses) {
		return errors.New("owner is not an active witness")
	}
	if len(c.Parameters) == 0 {
		return errors.New("proposal parameters are empty")
	}
	return nil
}

func (a *ProposalCreateActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)

	proposalID := ctx.DynProps.NextProposalID()
	ctx.DynProps.SetNextProposalID(proposalID + 1)

	proposal := &rawdb.Proposal{
		ID:             proposalID,
		Proposer:       ownerAddr,
		Parameters:     c.Parameters,
		CreateTime:     ctx.BlockTime,
		ExpirationTime: ctx.BlockTime + proposalExpirationMs,
		State:          rawdb.ProposalStatePending,
	}

	if ctx.DB != nil {
		if err := rawdb.WriteProposal(ctx.DB, proposalID, proposal); err != nil {
			return nil, err
		}
		index := rawdb.ReadProposalIndex(ctx.DB)
		index = append(index, proposalID)
		if err := rawdb.WriteProposalIndex(ctx.DB, index); err != nil {
			return nil, err
		}
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}

func isActiveWitness(addr common.Address, actives []common.Address) bool {
	for _, a := range actives {
		if a == addr {
			return true
		}
	}
	return false
}
