package actuator

import (
	"errors"
	"math"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

type TransferActuator struct{}

func (a *TransferActuator) getContract(ctx *Context) (*contractpb.TransferContract, error) {
	return decodedContract[*contractpb.TransferContract](ctx, "TransferContract")
}

func (a *TransferActuator) Validate(ctx *Context) error {
	tc, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	ownerAddr, err := checkedAddress(tc.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	toAddr, err := checkedAddress(tc.ToAddress, "toAddress")
	if err != nil {
		return err
	}
	if ownerAddr == toAddr {
		return errors.New("cannot transfer to self")
	}
	if tc.Amount <= 0 {
		return errors.New("transfer amount must be positive")
	}
	if !ctx.State.AccountExists(ownerAddr) {
		return errors.New("owner account does not exist")
	}
	// Validation only needs immutable fields from the account envelope. Avoid
	// GetAccount here: it materializes every split account domain, including all
	// TRC10 maps, votes, permissions, and stake rows.
	toAccount := ctx.State.AccountReference(toAddr)
	if ctx.DynProps.ForbidTransferToContract() && toAccount != nil {
		if toAccount.Type() == corepb.AccountType_Contract {
			return errors.New("cannot transfer TRX to a smart contract")
		}
	}
	if ctx.DynProps.AllowTvmCompatibleEvm() && toAccount != nil && toAccount.Type() == corepb.AccountType_Contract {
		meta, ok := ctx.State.ContractRuntime(toAddr)
		if !ok {
			return errors.New("contract account missing contract metadata")
		}
		if meta.Version == 1 {
			return errors.New("cannot transfer TRX to a version 1 smart contract")
		}
	}
	fee := int64(0)
	if toAccount == nil {
		fee = ctx.DynProps.CreateNewAccountFeeInSystemContract()
	}
	if fee > math.MaxInt64-tc.Amount {
		return errors.New("transfer amount plus fee overflows int64")
	}
	if ctx.State.GetBalance(ownerAddr) < tc.Amount+fee {
		return errors.New("insufficient balance")
	}
	if toAccount != nil && ctx.State.GetBalance(toAddr) > math.MaxInt64-tc.Amount {
		return errors.New("recipient balance overflows int64")
	}
	return nil
}

func (a *TransferActuator) Execute(ctx *Context) (*Result, error) {
	tc, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	ownerAddr, err := checkedAddress(tc.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	toAddr, err := checkedAddress(tc.ToAddress, "toAddress")
	if err != nil {
		return nil, err
	}

	fee := int64(0)
	recipientExists := ctx.State.AccountExists(toAddr)
	if !recipientExists {
		fee = ctx.DynProps.CreateNewAccountFeeInSystemContract()
	}
	if fee > math.MaxInt64-tc.Amount {
		return nil, errors.New("transfer amount plus fee overflows int64")
	}
	if ctx.State.GetBalance(ownerAddr) < tc.Amount+fee {
		return nil, errors.New("insufficient balance")
	}
	if recipientExists && ctx.State.GetBalance(toAddr) > math.MaxInt64-tc.Amount {
		return nil, errors.New("recipient balance overflows int64")
	}
	if !recipientExists {
		ctx.State.CreateAccountWithTime(toAddr, corepb.AccountType_Normal, ctx.DynProps.LatestBlockHeaderTimestamp())
		if ctx.DynProps.AllowMultiSign() {
			ctx.State.ApplyDefaultAccountPermissions(toAddr, ctx.DynProps)
		}
		// Actuator-level extra fee (proposal #12, default 0). Burned on top
		// of whatever bandwidth processor already charged. java-tron does NOT
		// increment total_create_account_cost here — that counter belongs to
		// the bandwidth-side `create_account_fee` path
		// (`core.consumeBandwidthForCreateNewAccount`).
		if err := burnFee(ctx, ownerAddr, fee); err != nil {
			return nil, err
		}
	}
	if err := ctx.State.SubBalance(ownerAddr, tc.Amount); err != nil {
		return nil, err
	}
	ctx.State.AddBalance(toAddr, tc.Amount)
	result := ctx.newResult()
	result.Fee = fee
	result.ContractRet = 1
	return result, nil
}
