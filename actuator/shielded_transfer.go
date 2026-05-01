package actuator

import (
	"errors"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ShieldedTransferActuator handles shielded (Sapling) ZEN token transfers.
// Stage 1: transparent state changes only; ZK proof verification is skipped.
type ShieldedTransferActuator struct{}

func (a *ShieldedTransferActuator) getContract(ctx *Context) (*contractpb.ShieldedTransferContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ShieldedTransferContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ShieldedTransferContract")
	}
	return c, nil
}

// calcFee returns the fee charged for this shielded transaction in ZEN smallest units.
// If the transparent receiver account does not yet exist, the create-account fee applies.
func (a *ShieldedTransferActuator) calcFee(ctx *Context, c *contractpb.ShieldedTransferContract) int64 {
	if len(c.TransparentToAddress) > 0 {
		to := common.BytesToAddress(c.TransparentToAddress)
		if !ctx.State.AccountExists(to) {
			return ctx.DynProps.ShieldedTransactionCreateAccountFee()
		}
	}
	return ctx.DynProps.ShieldedTransactionFee()
}

func (a *ShieldedTransferActuator) Validate(ctx *Context) error {
	if !ctx.DynProps.AllowShieldedTransaction() {
		return errors.New("shielded transactions are not enabled")
	}
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}

	hasFrom := len(c.TransparentFromAddress) > 0
	hasTo := len(c.TransparentToAddress) > 0

	if hasFrom && c.FromAmount <= 0 {
		return errors.New("from_amount must be positive when transparent_from_address is set")
	}
	if hasTo && c.ToAmount <= 0 {
		return errors.New("to_amount must be positive when transparent_to_address is set")
	}
	if !hasFrom && len(c.SpendDescription) == 0 {
		return errors.New("shielded spend descriptions required when no transparent sender")
	}

	// Check for double spends
	for _, spend := range c.SpendDescription {
		if len(spend.Nullifier) == 0 {
			return errors.New("spend description missing nullifier")
		}
		if rawdb.HasNullifier(ctx.DB, spend.Nullifier) {
			return errors.New("double spend: nullifier already used")
		}
	}

	// Check transparent sender has sufficient ZEN balance
	if hasFrom {
		from := common.BytesToAddress(c.TransparentFromAddress)
		if !ctx.State.AccountExists(from) {
			return errors.New("transparent sender account does not exist")
		}
		zenID := ctx.DynProps.ZenTokenID()
		fee := a.calcFee(ctx, c)
		if ctx.State.GetTRC10Balance(from, zenID) < c.FromAmount+fee {
			return errors.New("insufficient ZEN balance for shielded transfer")
		}
	}

	return nil
}

func (a *ShieldedTransferActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}

	zenID := ctx.DynProps.ZenTokenID()
	fee := a.calcFee(ctx, c)

	// Deduct ZEN from transparent sender (fromAmount + fee)
	if len(c.TransparentFromAddress) > 0 {
		from := common.BytesToAddress(c.TransparentFromAddress)
		if err := ctx.State.SubTRC10Balance(from, zenID, c.FromAmount+fee); err != nil {
			return nil, err
		}
	}

	// Record spend nullifiers to prevent double-spend
	for _, spend := range c.SpendDescription {
		if len(spend.Nullifier) == 0 {
			continue
		}
		if err := rawdb.WriteNullifier(ctx.DB, spend.Nullifier); err != nil {
			return nil, err
		}
	}

	// Record note commitments (Merkle tree position tracking for Stage 2)
	for _, recv := range c.ReceiveDescription {
		if len(recv.NoteCommitment) == 0 {
			continue
		}
		if err := rawdb.AppendNoteCommitment(ctx.DB, recv.NoteCommitment); err != nil {
			return nil, err
		}
	}

	// Credit transparent receiver
	if len(c.TransparentToAddress) > 0 {
		to := common.BytesToAddress(c.TransparentToAddress)
		if !ctx.State.AccountExists(to) {
			ctx.State.CreateAccountWithTime(to, corepb.AccountType_Normal, ctx.DynProps.LatestBlockHeaderTimestamp())
			if ctx.DynProps.AllowMultiSign() {
				ctx.State.ApplyDefaultAccountPermissions(to, ctx.DynProps)
			}
		}
		ctx.State.AddTRC10Balance(to, zenID, c.ToAmount)
	}

	// Adjust shielded pool total:
	// pool += fromAmount (enters pool from transparent sender)
	// pool -= toAmount  (leaves pool to transparent receiver)
	// pool -= fee       (burned from pool)
	ctx.DynProps.AdjustTotalShieldedPoolValue(c.FromAmount - c.ToAmount - fee)

	return &Result{Fee: fee, ContractRet: 1}, nil
}
