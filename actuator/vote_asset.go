package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// VoteAssetActuator handles the deprecated VoteAsset transaction (contract type 3).
// VoteAsset had no lasting on-chain effect in modern TRON. This implementation
// validates the owner exists and returns success with no state changes.
type VoteAssetActuator struct{}

func (a *VoteAssetActuator) getContract(ctx *Context) (*contractpb.VoteAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.VoteAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal VoteAssetContract")
	}
	return c, nil
}

func (a *VoteAssetActuator) Validate(ctx *Context) error {
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(owner) {
		return errors.New("owner account does not exist")
	}
	return nil
}

func (a *VoteAssetActuator) Execute(ctx *Context) (*Result, error) {
	if _, err := a.getContract(ctx); err != nil {
		return nil, err
	}
	// VoteAsset is deprecated — no state changes.
	return &Result{Fee: 0, ContractRet: 1}, nil
}
