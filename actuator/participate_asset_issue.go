package actuator

import (
	"errors"
	"math"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ParticipateAssetIssueActuator handles TRC10 ICO participation (contract type 9).
// Buyers send TRX to the issuer in exchange for tokens at the asset's exchange rate.
type ParticipateAssetIssueActuator struct{}

func (a *ParticipateAssetIssueActuator) getContract(ctx *Context) (*contractpb.ParticipateAssetIssueContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.ParticipateAssetIssueContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal ParticipateAssetIssueContract")
	}
	return c, nil
}

func (a *ParticipateAssetIssueActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	resolved, err := resolveAsset(ctx, c.AssetName)
	if err != nil {
		return err
	}
	tokenID := resolved.TokenID
	asset := resolved.Asset
	if c.Amount <= 0 {
		return errors.New("amount must be positive")
	}
	buyer, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	issuer, err := checkedAddress(c.ToAddress, "toAddress")
	if err != nil {
		return err
	}
	if buyer == issuer {
		return errors.New("buyer and issuer must be different")
	}
	if !ctx.State.AccountExists(buyer) {
		return errors.New("buyer account does not exist")
	}
	assetOwner, err := checkedAddress(asset.OwnerAddress, "asset ownerAddress")
	if err != nil {
		return err
	}
	if issuer != assetOwner {
		return errors.New("asset is not issued by toAddress")
	}
	if ctx.PrevBlockTime < asset.StartTime {
		return errors.New("ICO has not started yet")
	}
	if ctx.PrevBlockTime >= asset.EndTime {
		return errors.New("ICO has ended")
	}
	if asset.TrxNum <= 0 || asset.Num <= 0 {
		return errors.New("invalid exchange rate in asset")
	}
	// Guard against overflow: c.Amount * asset.Num must not exceed int64 max.
	if int64(asset.Num) > 0 && c.Amount > math.MaxInt64/int64(asset.Num) {
		return errors.New("token amount overflows int64")
	}
	tokenAmount := c.Amount * int64(asset.Num) / int64(asset.TrxNum)
	if tokenAmount <= 0 {
		return errors.New("token amount rounds to zero")
	}
	if ctx.State.GetBalance(buyer) < c.Amount {
		return errors.New("insufficient TRX balance")
	}
	if !ctx.State.AccountExists(issuer) {
		return errors.New("issuer account does not exist")
	}
	if ctx.State.GetBalance(issuer) > math.MaxInt64-c.Amount {
		return errors.New("issuer balance overflows int64")
	}
	if ctx.State.GetTRC10BalanceFinal(issuer, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) < tokenAmount {
		return errors.New("issuer has insufficient token supply")
	}
	if ctx.State.GetTRC10BalanceFinal(buyer, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-tokenAmount {
		return errors.New("buyer TRC10 balance overflows int64")
	}
	return nil
}

func (a *ParticipateAssetIssueActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	resolved, err := resolveAsset(ctx, c.AssetName)
	if err != nil {
		return nil, err
	}
	tokenID := resolved.TokenID
	asset := resolved.Asset
	if int64(asset.Num) > 0 && c.Amount > math.MaxInt64/int64(asset.Num) {
		return nil, errors.New("token amount overflows int64")
	}
	tokenAmount := c.Amount * int64(asset.Num) / int64(asset.TrxNum) // overflow already checked in Validate

	buyer, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	issuer, err := checkedAddress(c.ToAddress, "toAddress")
	if err != nil {
		return nil, err
	}
	if ctx.State.GetBalance(issuer) > math.MaxInt64-c.Amount {
		return nil, errors.New("issuer balance overflows int64")
	}
	if ctx.State.GetTRC10BalanceFinal(buyer, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-tokenAmount {
		return nil, errors.New("buyer TRC10 balance overflows int64")
	}

	// Buyer pays TRX; issuer receives TRX
	if err := ctx.State.SubBalance(buyer, c.Amount); err != nil {
		return nil, err
	}
	ctx.State.AddBalance(issuer, c.Amount)

	// Issuer gives tokens; buyer receives tokens
	if err := ctx.State.SubTRC10BalanceFinal(issuer, c.AssetName, tokenID, tokenAmount, ctx.DynProps.AllowSameTokenName()); err != nil {
		return nil, err
	}
	ctx.State.AddTRC10BalanceFinal(buyer, c.AssetName, tokenID, tokenAmount, ctx.DynProps.AllowSameTokenName())

	return &Result{Fee: 0, ContractRet: 1}, nil
}
