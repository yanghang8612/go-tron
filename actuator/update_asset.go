package actuator

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/tronprotocol/go-tron/core/types"
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
	if ctx.State == nil {
		return errors.New("state not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	owner, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	acct := ctx.State.GetAccount(owner)
	if acct == nil {
		return errors.New("owner account does not exist")
	}
	issued, err := issuedAssetRef(ctx, acct)
	if err != nil {
		return err
	}
	if !ctx.DynProps.AllowSameTokenName() {
		if ctx.State.ReadAssetIssueByName(issued.Name) == nil {
			return errors.New("token not found")
		}
	} else if ctx.State.ReadAssetIssue(issued.TokenID) == nil {
		return errors.New("token not found")
	}
	if !validBytesLen(c.Url, 256, false) {
		return errors.New("invalid url")
	}
	if !validBytesLen(c.Description, 200, true) {
		return errors.New("invalid description")
	}
	oneDayNetLimit := ctx.DynProps.OneDayNetLimit()
	if c.NewLimit < 0 || c.NewLimit >= oneDayNetLimit {
		return errors.New("new_limit out of range")
	}
	if c.NewPublicLimit < 0 || c.NewPublicLimit >= oneDayNetLimit {
		return errors.New("new_public_limit out of range")
	}
	return nil
}

func (a *UpdateAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	owner, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	acct := ctx.State.GetAccount(owner)
	if acct == nil {
		return nil, errors.New("owner account does not exist")
	}
	issued, err := issuedAssetRef(ctx, acct)
	if err != nil {
		return nil, err
	}
	v2Asset := ctx.State.ReadAssetIssue(issued.TokenID)
	if v2Asset == nil {
		return nil, errors.New("token not found")
	}

	v2Asset.Description = c.Description
	v2Asset.Url = c.Url
	v2Asset.FreeAssetNetLimit = c.NewLimit
	v2Asset.PublicFreeAssetNetLimit = c.NewPublicLimit

	if !ctx.DynProps.AllowSameTokenName() {
		legacyAsset := ctx.State.ReadAssetIssueByName(issued.Name)
		if legacyAsset == nil {
			return nil, errors.New("token not found")
		}
		legacyAsset.Description = c.Description
		legacyAsset.Url = c.Url
		legacyAsset.FreeAssetNetLimit = c.NewLimit
		legacyAsset.PublicFreeAssetNetLimit = c.NewPublicLimit
		if err := ctx.State.WriteAssetIssueByName(issued.Name, legacyAsset); err != nil {
			return nil, fmt.Errorf("write updated legacy asset: %w", err)
		}
	}
	if err := ctx.State.WriteAssetIssue(issued.TokenID, v2Asset); err != nil {
		return nil, fmt.Errorf("write updated asset: %w", err)
	}

	return &Result{Fee: 0, ContractRet: 1}, nil
}

type issuedAssetInfo struct {
	Name    []byte
	TokenID int64
}

func issuedAssetRef(ctx *Context, acct *types.Account) (*issuedAssetInfo, error) {
	pb := acct.Proto()
	tokenIDBytes := pb.GetAssetIssued_ID()
	if len(tokenIDBytes) == 0 {
		return nil, errors.New("owner account has not issued any asset")
	}
	tokenID, err := strconv.ParseInt(string(tokenIDBytes), 10, 64)
	if err != nil {
		return nil, errors.New("invalid issued asset ID")
	}
	if !ctx.DynProps.AllowSameTokenName() {
		name := pb.GetAssetIssuedName()
		if len(name) == 0 {
			return nil, errors.New("owner account has not issued any asset")
		}
		return &issuedAssetInfo{Name: name, TokenID: tokenID}, nil
	}
	return &issuedAssetInfo{TokenID: tokenID}, nil
}

func issuedAssetTokenID(ctx *Context, acct *types.Account) (int64, error) {
	issued, err := issuedAssetRef(ctx, acct)
	if err != nil {
		return 0, err
	}
	return issued.TokenID, nil
}
