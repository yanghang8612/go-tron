package actuator

import (
	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
)

// burnFee subtracts fee from owner's balance. When AllowBlackholeOptimization
// is active (proposal #49), the fee is burned (removed from circulation) and
// the cumulative burn_trx_amount DP counter is incremented.
// Before activation, the fee is credited to the Blackhole genesis account,
// matching java-tron's pre-fork behaviour.
func burnFee(ctx *Context, owner common.Address, fee int64) error {
	if fee <= 0 {
		return nil
	}
	if err := ctx.State.SubBalance(owner, fee); err != nil {
		return err
	}
	if forks.IsActive(forks.AllowBlackholeOptimization, ctx.BlockNumber, ctx.DynProps) {
		ctx.DynProps.AddBurnTrx(fee)
	} else {
		ctx.State.AddBalance(params.BlackholeAddress, fee)
	}
	return nil
}

// extractOwner returns the owner address from the first contract in tx.
// Returns zero address if the tx has no contract or the contract type does not
// expose GetOwnerAddress (should not happen for any valid TRON transaction).
func extractOwner(tx *types.Transaction) common.Address {
	contract := tx.Contract()
	if contract == nil {
		return common.Address{}
	}
	msg, err := contract.Parameter.UnmarshalNew()
	if err != nil {
		return common.Address{}
	}
	type ownerAddressGetter interface {
		GetOwnerAddress() []byte
	}
	if oag, ok := msg.(ownerAddressGetter); ok {
		return common.BytesToAddress(oag.GetOwnerAddress())
	}
	return common.Address{}
}

// ConsumeMultiSignFee charges multi_sign_fee when the transaction has more than
// one signature and allow_multi_sign is active. Mirrors java-tron
// TransactionTrace.consumeMultiSignFee.
func ConsumeMultiSignFee(ctx *Context) error {
	if len(ctx.Tx.Signatures()) <= 1 {
		return nil
	}
	if !ctx.DynProps.AllowMultiSign() {
		return nil
	}
	fee := ctx.DynProps.MultiSignFee()
	if fee <= 0 {
		return nil
	}
	owner := extractOwner(ctx.Tx)
	return burnFee(ctx, owner, fee)
}

// ConsumeMemoFee charges memo_fee when the transaction has a non-empty data
// memo field. Only applies when memo_fee > 0 (governance-controlled, default 0).
// Mirrors java-tron TransactionTrace.consumeMemoFee.
func ConsumeMemoFee(ctx *Context) error {
	rawData := ctx.Tx.Proto().RawData
	if rawData == nil || len(rawData.Data) == 0 {
		return nil
	}
	fee := ctx.DynProps.MemoFee()
	if fee <= 0 {
		return nil
	}
	owner := extractOwner(ctx.Tx)
	return burnFee(ctx, owner, fee)
}
