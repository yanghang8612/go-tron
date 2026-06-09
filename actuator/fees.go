package actuator

import (
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
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

// ownerOfContract returns the owner address embedded in a contract parameter,
// mirroring java-tron TransactionCapsule.getOwner. ShieldedTransferContract is a
// special case (sender is transparent_from_address, not owner_address); all other
// types expose GetOwnerAddress. Returns the zero address when there is no owner.
func ownerOfContract(c *corepb.Transaction_Contract) common.Address {
	if c == nil || c.Parameter == nil {
		return common.Address{}
	}
	msg, err := c.Parameter.UnmarshalNew()
	if err != nil {
		return common.Address{}
	}
	// ShieldedTransferContract carries its sender in transparent_from_address, not
	// owner_address. Mirror java-tron TransactionCapsule.getOwner: return the
	// transparent sender, or the zero address (java's new byte[0]) when it is empty.
	if sh, ok := msg.(*contractpb.ShieldedTransferContract); ok {
		from := sh.GetTransparentFromAddress()
		if len(from) == 0 {
			return common.Address{}
		}
		return common.BytesToAddress(from)
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
// java does NOT gate this on AllowMultiSign — it charges purely on signatureCount > 1
// (MULTI_SIGN_FEE is 1_000_000 from genesis). The gate is latent (a >1-sig tx cannot
// validate before AllowMultiSign activates) but go-tron must not add it for parity.
func ConsumeMultiSignFee(ctx *Context) (int64, error) {
	if len(ctx.Tx.Signatures()) <= 1 {
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
