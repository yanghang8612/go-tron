package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type ProposalApproveActuator struct{}

func (a *ProposalApproveActuator) getContract(ctx *Context) (*contractpb.ProposalApproveContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ProposalApproveContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ProposalApproveContract")
	}
	return c, nil
}

func (a *ProposalApproveActuator) Validate(ctx *Context) error {
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
	if ctx.DB == nil {
		return errors.New("database not available")
	}
	if c.ProposalId >= ctx.DynProps.NextProposalID() {
		return errors.New("proposal not found")
	}
	proposal := rawdb.ReadProposal(ctx.DB, c.ProposalId)
	if proposal == nil {
		return errors.New("proposal not found")
	}
	if proposal.State != rawdb.ProposalStatePending {
		return errors.New("proposal is not pending")
	}
	if proposal.ExpirationTime <= ctx.BlockTime {
		return errors.New("proposal has expired")
	}
	hasApproved := containsAddress(proposal.Approvals, ownerAddr)
	if c.IsAddApproval && hasApproved {
		return errors.New("already approved")
	}
	if !c.IsAddApproval && !hasApproved {
		return errors.New("not yet approved, cannot revoke")
	}
	return nil
}

func (a *ProposalApproveActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr := common.BytesToAddress(c.OwnerAddress)
	if ctx.DB == nil {
		return nil, errors.New("database not available")
	}
	proposal := rawdb.ReadProposal(ctx.DB, c.ProposalId)
	if proposal == nil {
		return nil, errors.New("proposal not found")
	}

	if c.IsAddApproval {
		proposal.Approvals = append(proposal.Approvals, ownerAddr)
	} else {
		proposal.Approvals = removeAddress(proposal.Approvals, ownerAddr)
	}

	if err := rawdb.WriteProposal(ctx.DB, c.ProposalId, proposal); err != nil {
		return nil, err
	}
	return &Result{Fee: 0, ContractRet: 1}, nil
}

func containsAddress(addrs []common.Address, target common.Address) bool {
	for _, a := range addrs {
		if a == target {
			return true
		}
	}
	return false
}

func removeAddress(addrs []common.Address, target common.Address) []common.Address {
	result := make([]common.Address, 0, len(addrs))
	for _, a := range addrs {
		if a != target {
			result = append(result, a)
		}
	}
	return result
}
