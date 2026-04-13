package actuator

import (
	"errors"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// TransferAssetActuator handles TRC10 token transfers (contract type 2).
type TransferAssetActuator struct{}

func (a *TransferAssetActuator) getContract(ctx *Context) (*contractpb.TransferAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.TransferAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal TransferAssetContract")
	}
	return c, nil
}

func (a *TransferAssetActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	tokenID, err := strconv.ParseInt(string(c.AssetName), 10, 64)
	if err != nil {
		return errors.New("invalid token ID in asset_name")
	}
	if rawdb.ReadAssetIssue(ctx.DB, tokenID) == nil {
		return errors.New("token not found")
	}
	if c.Amount <= 0 {
		return errors.New("transfer amount must be positive")
	}
	from := common.BytesToAddress(c.OwnerAddress)
	to := common.BytesToAddress(c.ToAddress)
	if from == to {
		return errors.New("cannot transfer to self")
	}
	if ctx.State.GetTRC10Balance(from, tokenID) < c.Amount {
		return errors.New("insufficient TRC10 balance")
	}
	if ctx.DynProps.ForbidTransferToContract() && ctx.State.AccountExists(to) {
		if len(ctx.State.GetCode(to)) > 0 {
			return errors.New("cannot transfer TRC10 to a smart contract")
		}
	}
	return nil
}

func (a *TransferAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	tokenID, _ := strconv.ParseInt(string(c.AssetName), 10, 64)
	from := common.BytesToAddress(c.OwnerAddress)
	to := common.BytesToAddress(c.ToAddress)

	fee := int64(0)
	if !ctx.State.AccountExists(to) {
		ctx.State.CreateAccount(to, corepb.AccountType_Normal)
		fee = ctx.DynProps.CreateNewAccountFeeInSystemContract()
		if fee > 0 {
			if err := ctx.State.SubBalance(from, fee); err != nil {
				return nil, err
			}
		}
	}

	if err := ctx.State.SubTRC10Balance(from, tokenID, c.Amount); err != nil {
		return nil, err
	}
	ctx.State.AddTRC10Balance(to, tokenID, c.Amount)

	return &Result{Fee: fee, ContractRet: 1}, nil
}
