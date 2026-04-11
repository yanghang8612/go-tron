package actuator

import (
	"errors"
	"fmt"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// UpdateAssetActuator handles TRC10 token metadata updates (contract type 15).
// Only the original issuer can update description, URL, and bandwidth limits.
type UpdateAssetActuator struct{}

func (a *UpdateAssetActuator) getContract(ctx *Context) (*contractpb.UpdateAssetContract, error) {
	contract := ctx.Tx.Contract()
	if contract == nil {
		return nil, errors.New("no contract in transaction")
	}
	c := &contractpb.UpdateAssetContract{}
	if err := contract.Parameter.UnmarshalTo(c); err != nil {
		return nil, errors.New("failed to unmarshal UpdateAssetContract")
	}
	return c, nil
}

func (a *UpdateAssetActuator) Validate(ctx *Context) error {
	if ctx.DB == nil {
		return errors.New("DB not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	tokenID, ok := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:])
	if !ok {
		return errors.New("no token issued by this address")
	}
	if rawdb.ReadAssetIssue(ctx.DB, tokenID) == nil {
		return errors.New("token not found")
	}
	return nil
}

func (a *UpdateAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	owner := common.BytesToAddress(c.OwnerAddress)
	tokenID, ok := rawdb.ReadAssetOwnerIndex(ctx.DB, owner[:])
	if !ok {
		return nil, errors.New("no token issued by this address")
	}
	asset := rawdb.ReadAssetIssue(ctx.DB, tokenID)
	if asset == nil {
		return nil, errors.New("token not found")
	}

	asset.Description = c.Description
	asset.Url = c.Url
	asset.FreeAssetNetLimit = c.NewLimit
	asset.PublicFreeAssetNetLimit = c.NewPublicLimit

	if err := rawdb.WriteAssetIssue(ctx.DB, tokenID, asset); err != nil {
		return nil, fmt.Errorf("write updated asset: %w", err)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}
