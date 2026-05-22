package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type ProposalDeleteActuator struct{}

func (a *ProposalDeleteActuator) getContract(ctx *Context) (*contractpb.ProposalDeleteContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ProposalDeleteContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ProposalDeleteContract")
	}
	return c, nil
}

func (a *ProposalDeleteActuator) Validate(ctx *Context) error {
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
	if c.ProposalId > ctx.DynProps.LatestProposalNum() {
		return errors.New("proposal not found")
	}
	proposal := ctx.State.ReadProposal(c.ProposalId)
	if proposal == nil {
		return errors.New("proposal not found")
	}
	if proposal.State == rawdb.ProposalStateCanceled {
		return errors.New("proposal is canceled")
	}
	if ctx.PrevBlockTime >= proposal.ExpirationTime {
		return errors.New("proposal has expired")
	}
	if proposal.Proposer != ownerAddr {
		return errors.New("only the proposer can delete the proposal")
	}
	return nil
}

func (a *ProposalDeleteActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	proposal := ctx.State.ReadProposal(c.ProposalId)
	if proposal == nil {
		return nil, errors.New("proposal not found")
	}
	proposal.State = rawdb.ProposalStateCanceled
	if err := ctx.State.WriteProposal(c.ProposalId, proposal); err != nil {
		return nil, err
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}
