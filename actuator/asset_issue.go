package actuator

import (
	"errors"
	"fmt"
	"math"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// AssetIssueActuator handles TRC10 token issuance (contract type 6).
type AssetIssueActuator struct{}

func (a *AssetIssueActuator) getContract(ctx *Context) (*contractpb.AssetIssueContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.AssetIssueContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal AssetIssueContract")
	}
	return c, nil
}

func (a *AssetIssueActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	if !ctx.State.AccountExists(owner) {
		return errors.New("owner account does not exist")
	}
	if len(c.Name) == 0 {
		return errors.New("token name is required")
	}
	if len(c.Abbr) == 0 {
		return errors.New("token abbreviation is required")
	}
	if c.TotalSupply <= 0 {
		return errors.New("total supply must be positive")
	}
	if c.TrxNum <= 0 {
		return errors.New("trx_num must be positive")
	}
	if c.Num <= 0 {
		return errors.New("num must be positive")
	}
	if c.StartTime >= c.EndTime {
		return errors.New("start_time must be before end_time")
	}
	if c.Precision < 0 || c.Precision > 6 {
		return errors.New("precision must be 0-6")
	}
	if int64(len(c.FrozenSupply)) > ctx.DynProps.MaxFrozenSupplyNumber() {
		return errors.New("frozen supply count exceeds max_frozen_supply_number")
	}
	oneDayNetLimit := ctx.DynProps.OneDayNetLimit()
	if c.FreeAssetNetLimit < 0 || c.FreeAssetNetLimit >= oneDayNetLimit {
		return errors.New("free_asset_net_limit out of range")
	}
	if c.PublicFreeAssetNetLimit < 0 || c.PublicFreeAssetNetLimit >= oneDayNetLimit {
		return errors.New("public_free_asset_net_limit out of range")
	}
	minSupplyTime := ctx.DynProps.MinFrozenSupplyTime()
	maxSupplyTime := ctx.DynProps.MaxFrozenSupplyTime()
	var frozenTotal int64
	for _, f := range c.FrozenSupply {
		if f.FrozenAmount <= 0 {
			return errors.New("frozen_amount must be positive")
		}
		if f.FrozenDays < minSupplyTime || f.FrozenDays > maxSupplyTime {
			return fmt.Errorf("frozen_days must be in [%d, %d]", minSupplyTime, maxSupplyTime)
		}
		if frozenTotal > 0 && f.FrozenAmount > math.MaxInt64-frozenTotal {
			return errors.New("frozen supply total overflows int64")
		}
		frozenTotal += f.FrozenAmount
	}
	if frozenTotal > c.TotalSupply {
		return errors.New("frozen supply exceeds total supply")
	}
	if ctx.State.GetBalance(owner) < ctx.DynProps.AssetIssueFee() {
		return errors.New("insufficient balance for asset issue fee")
	}
	if !forks.IsActive(forks.AllowSameTokenName, ctx.BlockNumber, ctx.DynProps) {
		if _, ok := rawdb.ReadAssetNameIndex(ctx.DB, c.Name); ok {
			return errors.New("token name already exists")
		}
	}
	if _, ok := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:]); ok {
		return errors.New("address has already issued a token")
	}
	return nil
}

func (a *AssetIssueActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	owner := common.BytesToAddress(c.OwnerAddress)

	// Assign and increment token ID
	tokenID := ctx.DynProps.NextTokenID()
	ctx.DynProps.SetNextTokenID(tokenID + 1)
	c.Id = strconv.FormatInt(tokenID, 10)

	// Persist metadata and indexes
	if err := rawdb.WriteAssetIssue(ctx.DB, tokenID, c); err != nil {
		return nil, fmt.Errorf("write asset: %w", err)
	}
	if err := rawdb.WriteAssetNameIndex(ctx.DB, c.Name, tokenID); err != nil {
		return nil, fmt.Errorf("write name index: %w", err)
	}
	if err := rawdb.WriteAssetOwnerIndex(ctx.DB, owner[:], tokenID); err != nil {
		return nil, fmt.Errorf("write owner index: %w", err)
	}
	if err := rawdb.WriteAssetIssueTime(ctx.DB, tokenID, ctx.BlockTime); err != nil {
		return nil, fmt.Errorf("write issue time: %w", err)
	}

	// Mint free supply to issuer (frozen supply is held until UnfreezeAsset)
	var frozenTotal int64
	for _, f := range c.FrozenSupply {
		frozenTotal += f.FrozenAmount
	}
	freeAmount := c.TotalSupply - frozenTotal
	if freeAmount > 0 {
		ctx.State.SetTRC10Balance(owner, tokenID, freeAmount)
	}

	fee := ctx.DynProps.AssetIssueFee()
	if err := burnFee(ctx, owner, fee); err != nil {
		return nil, err
	}

	return &Result{Fee: fee, ContractRet: 1}, nil
}
