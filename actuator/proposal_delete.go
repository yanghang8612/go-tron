package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
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
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	if ctx.DB == nil {
		return errors.New("database not available")
	}
	proposal := rawdb.ReadProposal(ctx.DB, c.ProposalId)
	if proposal == nil {
		return errors.New("proposal not found")
	}
	if proposal.State != rawdb.ProposalStatePending {
		return errors.New("proposal is not pending")
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
	if ctx.DB == nil {
		return nil, errors.New("database not available")
	}
	proposal := rawdb.ReadProposal(ctx.DB, c.ProposalId)
	if proposal == nil {
		return nil, errors.New("proposal not found")
	}
	proposal.State = rawdb.ProposalStateCanceled
	if err := rawdb.WriteProposal(ctx.DB, c.ProposalId, proposal); err != nil {
		return nil, err
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}
