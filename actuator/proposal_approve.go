package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// proposalParamKey maps governance proposal parameter IDs to DynamicProperties keys.
var proposalParamKey = map[int64]string{
	// Numeric parameters
	0:  "maintenance_time_interval",
	5:  "total_energy_current_limit",
	7:  "energy_fee",
	8:  "max_cpu_time_of_one_tx",
	9:  "free_net_limit",
	13: "witness_pay_per_block",
	14: "witness_standby_allowance",
	19: "total_net_limit",
	// Allow flags
	1:  "allow_multi_sign",
	3:  "allow_same_token_name",
	4:  "allow_delegate_resource",
	6:  "allow_adaptive_energy_limit",
	15: "allow_tvm_transfer_trc10",
	16: "allow_change_delegation",
	18: "allow_new_resource_model",
	30: "allow_tvm_constantinople",
	32: "allow_tvm_solidity059",
	33: "allow_tvm_freeze",
	35: "allow_tvm_shielded_token",
	40: "allow_pbft",
	41: "allow_tvm_istanbul",
	45: "allow_market_transaction",
	48: "allow_tvm_compatibility",
	52: "allow_account_history",
	57: "allow_tvm_vote",
	65: "allow_tvm_london",
	66: "allow_energy_adjustment",
	70: "allow_dynamic_energy",
	74: "allow_staking_v2",
	78: "allow_tvm_big_integer",
	82: "allow_tvm_cancun",
	83: "allow_tvm_blob",
}

// applyProposal applies all parameters from an approved proposal to DynamicProperties.
func applyProposal(ctx *Context, p *rawdb.Proposal) {
	for paramID, value := range p.Parameters {
		key, ok := proposalParamKey[paramID]
		if !ok {
			continue
		}
		ctx.DynProps.Set(key, value)
	}
}

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

	// Check if approval threshold reached (>2/3 of active witnesses)
	if c.IsAddApproval && len(ctx.ActiveWitnesses) > 0 {
		threshold := len(ctx.ActiveWitnesses)*2/3 + 1
		if len(proposal.Approvals) >= threshold {
			applyProposal(ctx, proposal)
			proposal.State = rawdb.ProposalStateApproved
		}
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
