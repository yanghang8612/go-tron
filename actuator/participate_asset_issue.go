package actuator

import (
	"errors"
	"math"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
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
	tokenID, err := strconv.ParseInt(string(c.AssetName), 10, 64)
	if err != nil {
		return errors.New("invalid token ID in asset_name")
	}
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	if asset == nil {
		return errors.New("token not found")
	}
	if c.Amount <= 0 {
		return errors.New("amount must be positive")
	}
	if ctx.BlockTime < asset.StartTime {
		return errors.New("ICO has not started yet")
	}
	if ctx.BlockTime > asset.EndTime {
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
	buyer := common.BytesToAddress(c.OwnerAddress)
	issuer := common.BytesToAddress(c.ToAddress)
	if buyer == issuer {
		return errors.New("buyer and issuer must be different")
	}
	if ctx.State.GetBalance(buyer) < c.Amount {
		return errors.New("insufficient TRX balance")
	}
	if ctx.State.GetTRC10Balance(issuer, tokenID) < tokenAmount {
		return errors.New("issuer has insufficient token supply")
	}
	return nil
}

func (a *ParticipateAssetIssueActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	tokenID, _ := strconv.ParseInt(string(c.AssetName), 10, 64)
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	tokenAmount := c.Amount * int64(asset.Num) / int64(asset.TrxNum) // overflow already checked in Validate

	buyer := common.BytesToAddress(c.OwnerAddress)
	issuer := common.BytesToAddress(c.ToAddress)

	// Buyer pays TRX; issuer receives TRX
	if err := ctx.State.SubBalance(buyer, c.Amount); err != nil {
		return nil, err
	}
	ctx.State.AddBalance(issuer, c.Amount)

	// Issuer gives tokens; buyer receives tokens
	if err := ctx.State.SubTRC10Balance(issuer, tokenID, tokenAmount); err != nil {
		return nil, err
	}
	ctx.State.AddTRC10Balance(buyer, tokenID, tokenAmount)

	return &Result{Fee: 0, ContractRet: 1}, nil
}
