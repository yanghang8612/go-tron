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

// resolveAssetNameOrID accepts the wire-format `asset_name` field which is
// either a numeric token ID (post-AllowSameTokenName, e.g. "1000004") or a
// literal token name (pre-fork, e.g. "Bitcoin"). Returns the resolved
// token ID. Mirrors java-tron's TransferAssetActuator dual-path lookup.
//
// gtron originally only handled the numeric form, silently dropping the
// ParseInt error in Execute and ending up with tokenID=0 → "insufficient
// balance" on every pre-fork TRC10 transfer. Mainnet sync stalled at
// block 5584 (a TransferAssetContract for asset_name="Bitcoin") because
// of this.
func resolveAssetNameOrID(ctx *Context, assetName []byte) (int64, error) {
	if id, err := strconv.ParseInt(string(assetName), 10, 64); err == nil {
		return id, nil
	}
	if id, ok := rawdb.ReadAssetNameIndex(ctx.DB, assetName); ok {
		return id, nil
	}
	return 0, errors.New("invalid asset_name: not a numeric ID and no name index hit")
}

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
	tokenID, err := resolveAssetNameOrID(ctx, c.AssetName)
	if err != nil {
		return err
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
	tokenID, err := resolveAssetNameOrID(ctx, c.AssetName)
	if err != nil {
		return nil, err
	}
	from := common.BytesToAddress(c.OwnerAddress)
	to := common.BytesToAddress(c.ToAddress)

	fee := int64(0)
	if !ctx.State.AccountExists(to) {
		ctx.State.CreateAccountWithTime(to, corepb.AccountType_Normal, ctx.DynProps.LatestBlockHeaderTimestamp())
		if ctx.DynProps.AllowMultiSign() {
			ctx.State.ApplyDefaultAccountPermissions(to, ctx.DynProps)
		}
		// Actuator-level extra fee (proposal #12, default 0). java-tron does
		// NOT increment total_create_account_cost here — see transfer.go for
		// the rationale.
		fee = ctx.DynProps.CreateNewAccountFeeInSystemContract()
		if err := burnFee(ctx, from, fee); err != nil {
			return nil, err
		}
	}

	if err := ctx.State.SubTRC10Balance(from, tokenID, c.Amount); err != nil {
		return nil, err
	}
	ctx.State.AddTRC10Balance(to, tokenID, c.Amount)

	return &Result{Fee: fee, ContractRet: 1}, nil
}
