package actuator

import (
	"errors"
	"math"
	"strconv"

	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// TransferAssetActuator handles TRC10 token transfers (contract type 2).
type TransferAssetActuator struct {
	assetCache assetResolverCache
}

type resolvedAsset struct {
	TokenID int64
	Asset   *contractpb.AssetIssueContract
}

// assetResolverCache carries the immutable asset metadata resolved during
// Validate into Execute. Core creates one actuator per transaction, and no
// TRC10 metadata mutation occurs between those two calls. Matching the wire
// name and fork mode keeps direct/reused test calls safe as well.
type assetResolverCache struct {
	assetName          string
	allowSameTokenName bool
	resolved           *resolvedAsset
}

func (c *assetResolverCache) reset() {
	c.assetName = ""
	c.allowSameTokenName = false
	c.resolved = nil
}

func (c *assetResolverCache) resolve(ctx *Context, assetName []byte) (*resolvedAsset, error) {
	allowSameTokenName := ctx.DynProps.AllowSameTokenName()
	if c.resolved != nil && c.assetName == string(assetName) && c.allowSameTokenName == allowSameTokenName {
		return c.resolved, nil
	}
	resolved, err := resolveAsset(ctx, assetName)
	if err != nil {
		return nil, err
	}
	c.assetName = string(assetName)
	c.allowSameTokenName = allowSameTokenName
	c.resolved = resolved
	return resolved, nil
}

// resolveAssetNameOrID accepts the wire-format asset_name field. Before
// AllowSameTokenName, java-tron treats it as the literal asset name and looks
// in AssetIssueStore; after the fork, it treats it as the numeric token ID and
// looks in AssetIssueV2Store. Numeric-looking pre-fork names must therefore
// still resolve through the legacy name index instead of ParseInt.
func resolveAssetNameOrID(ctx *Context, assetName []byte) (int64, error) {
	if !ctx.DynProps.AllowSameTokenName() {
		if asset := ctx.State.ReadAssetIssueByName(assetName); asset != nil {
			id, err := strconv.ParseInt(asset.Id, 10, 64)
			if err != nil {
				return 0, errors.New("invalid legacy asset ID")
			}
			return id, nil
		}
		if id, ok := ctx.State.ReadAssetNameIndex(assetName); ok {
			return id, nil
		}
		return 0, errors.New("invalid asset_name: no name index hit")
	}
	if id, err := strconv.ParseInt(string(assetName), 10, 64); err == nil {
		return id, nil
	}
	return 0, errors.New("invalid asset_name: not a numeric ID")
}

func resolveAsset(ctx *Context, assetName []byte) (*resolvedAsset, error) {
	// Before AllowSameTokenName the legacy metadata row already contains the
	// numeric ID. Return that same decoded message instead of resolving the ID
	// in one read and loading the identical name row again in a second read.
	if !ctx.DynProps.AllowSameTokenName() {
		if asset := ctx.State.ReadAssetIssueByName(assetName); asset != nil {
			tokenID, err := strconv.ParseInt(asset.Id, 10, 64)
			if err != nil {
				return nil, errors.New("invalid legacy asset ID")
			}
			return &resolvedAsset{TokenID: tokenID, Asset: asset}, nil
		}
		tokenID, ok := ctx.State.ReadAssetNameIndex(assetName)
		if !ok {
			return nil, errors.New("invalid asset_name: no name index hit")
		}
		asset := ctx.State.ReadAssetIssue(tokenID)
		if asset == nil {
			return nil, errors.New("token not found")
		}
		return &resolvedAsset{TokenID: tokenID, Asset: asset}, nil
	}
	tokenID, err := resolveAssetNameOrID(ctx, assetName)
	if err != nil {
		return nil, err
	}
	asset := ctx.State.ReadAssetIssue(tokenID)
	if asset == nil {
		return nil, errors.New("token not found")
	}
	return &resolvedAsset{TokenID: tokenID, Asset: asset}, nil
}

func (a *TransferAssetActuator) getContract(ctx *Context) (*contractpb.TransferAssetContract, error) {
	return decodedContract[*contractpb.TransferAssetContract](ctx, "TransferAssetContract")
}

func (a *TransferAssetActuator) Validate(ctx *Context) error {
	if ctx.State == nil {
		return errors.New("state not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	a.assetCache.reset()
	asset, err := a.assetCache.resolve(ctx, c.AssetName)
	if err != nil {
		return err
	}
	tokenID := asset.TokenID
	if c.Amount <= 0 {
		return errors.New("transfer amount must be positive")
	}
	from, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return err
	}
	to, err := checkedAddress(c.ToAddress, "toAddress")
	if err != nil {
		return err
	}
	if from == to {
		return errors.New("cannot transfer to self")
	}
	if !ctx.State.AccountExists(from) {
		return errors.New("owner account does not exist")
	}
	if ctx.State.GetTRC10BalanceFinal(from, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) < c.Amount {
		return errors.New("insufficient TRC10 balance")
	}
	// Type lives in the compact account envelope; loading the full account here
	// would scan every split auxiliary domain for each TRC10 recipient.
	toAccount := ctx.State.AccountReference(to)
	if toAccount != nil {
		if ctx.DynProps.ForbidTransferToContract() && toAccount.Type() == corepb.AccountType_Contract {
			return errors.New("cannot transfer TRC10 to a smart contract")
		}
		if ctx.State.GetTRC10BalanceFinal(to, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-c.Amount {
			return errors.New("recipient TRC10 balance overflows int64")
		}
	} else {
		fee := ctx.DynProps.CreateNewAccountFeeInSystemContract()
		if ctx.State.GetBalance(from) < fee {
			return errors.New("insufficient balance for create account fee")
		}
	}
	return nil
}

func (a *TransferAssetActuator) Execute(ctx *Context) (*Result, error) {
	c, err := a.getContract(ctx)
	if err != nil {
		return nil, err
	}
	asset, err := a.assetCache.resolve(ctx, c.AssetName)
	if err != nil {
		return nil, err
	}
	tokenID := asset.TokenID
	from, err := checkedAddress(c.OwnerAddress, "ownerAddress")
	if err != nil {
		return nil, err
	}
	to, err := checkedAddress(c.ToAddress, "toAddress")
	if err != nil {
		return nil, err
	}

	fee := int64(0)
	recipientExists := ctx.State.AccountExists(to)
	if !recipientExists {
		fee = ctx.DynProps.CreateNewAccountFeeInSystemContract()
	}
	if ctx.State.GetTRC10BalanceFinal(from, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) < c.Amount {
		return nil, errors.New("insufficient TRC10 balance")
	}
	if ctx.State.GetBalance(from) < fee {
		return nil, errors.New("insufficient balance for create account fee")
	}
	if recipientExists && ctx.State.GetTRC10BalanceFinal(to, c.AssetName, tokenID, ctx.DynProps.AllowSameTokenName()) > math.MaxInt64-c.Amount {
		return nil, errors.New("recipient TRC10 balance overflows int64")
	}
	if !recipientExists {
		ctx.State.CreateAccountWithTime(to, corepb.AccountType_Normal, ctx.DynProps.LatestBlockHeaderTimestamp())
		if ctx.DynProps.AllowMultiSign() {
			ctx.State.ApplyDefaultAccountPermissions(to, ctx.DynProps)
		}
		// Actuator-level extra fee (proposal #12, default 0). java-tron does
		// NOT increment total_create_account_cost here — see transfer.go for
		// the rationale.
		if err := burnFee(ctx, from, fee); err != nil {
			return nil, err
		}
	}

	if err := ctx.State.SubTRC10BalanceFinal(from, c.AssetName, tokenID, c.Amount, ctx.DynProps.AllowSameTokenName()); err != nil {
		return nil, err
	}
	ctx.State.AddTRC10BalanceFinal(to, c.AssetName, tokenID, c.Amount, ctx.DynProps.AllowSameTokenName())

	return &Result{Fee: fee, ContractRet: 1}, nil
}
