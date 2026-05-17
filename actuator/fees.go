package actuator

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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

// ownerOfContract returns the owner address embedded in a contract parameter.
// Returns zero address when the contract type does not expose GetOwnerAddress.
func ownerOfContract(c *corepb.Transaction_Contract) common.Address {
	if c == nil || c.Parameter == nil {
		return common.Address{}
	}
	msg, err := c.Parameter.UnmarshalNew()
	if err != nil {
		return common.Address{}
	}
	type ownerAddressGetter interface{ GetOwnerAddress() []byte }
	if oag, ok := msg.(ownerAddressGetter); ok {
		return common.BytesToAddress(oag.GetOwnerAddress())
	}
	return common.Address{}
}

// ConsumeMultiSignFee charges multi_sign_fee for each contract owner when the
// transaction carries more than one signature. Mirrors java-tron Manager.consumeMultiSignFee:
// the fee is charged per-contract (not once per tx), matching the contract list loop there.
func ConsumeMultiSignFee(ctx *Context) (int64, error) {
	if len(ctx.Tx.Signatures()) <= 1 {
		return 0, nil
	}
	if !ctx.DynProps.AllowMultiSign() {
		return 0, nil
	}
	fee := ctx.DynProps.MultiSignFee()
	if fee <= 0 {
		return 0, nil
	}
	rawData := ctx.Tx.Proto().RawData
	if rawData == nil {
		return 0, nil
	}
	var charged int64
	for _, c := range rawData.Contract {
		owner := ownerOfContract(c)
		if owner == (common.Address{}) {
			continue
		}
		if err := burnFee(ctx, owner, fee); err != nil {
			return charged, fmt.Errorf("multi-sign fee for %s: %w", owner.Hex(), err)
		}
		charged += fee
	}
	return charged, nil
}

// ConsumeMemoFee charges memo_fee for each contract owner when the transaction
// carries a non-empty data memo. Mirrors java-tron Manager.consumeMemoFee:
// the fee is charged per-contract (not once per tx), matching the contract list loop there.
func ConsumeMemoFee(ctx *Context) (int64, error) {
	rawData := ctx.Tx.Proto().RawData
	if rawData == nil || len(rawData.Data) == 0 {
		return 0, nil
	}
	fee := ctx.DynProps.MemoFee()
	if fee <= 0 {
		return 0, nil
	}
	var charged int64
	for _, c := range rawData.Contract {
		owner := ownerOfContract(c)
		if owner == (common.Address{}) {
			continue
		}
		if err := burnFee(ctx, owner, fee); err != nil {
			return charged, fmt.Errorf("memo fee for %s: %w", owner.Hex(), err)
		}
		charged += fee
	}
	return charged, nil
}
