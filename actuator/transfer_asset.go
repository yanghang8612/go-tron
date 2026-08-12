package actuator

import (
	"bytes"
	"errors"
	"math"
	"strconv"

	"github.com/tronprotocol/go-tron/common"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// TransferAssetActuator handles TRC10 token transfers (contract type 2).
type TransferAssetActuator struct {
	validated transferAssetValidation
}

type resolvedAsset struct {
	TokenID int64
	Asset   *contractpb.AssetIssueContract
}

// transferAssetValidation carries immutable contract/state facts from
// Validate into Execute. Core runs bandwidth and fee charging between the two
// calls; those operations may change the owner's TRX balance, but never the
// recipient's existence or either TRC10 balance. Execute therefore rechecks
// only the account-creation fee and reuses the remaining validated reads.
type transferAssetValidation struct {
	ctx                *Context
	contract           *contractpb.TransferAssetContract
	from               common.Address
	to                 common.Address
	tokenID            int64
	fromAssetBalance   int64
	toAssetBalance     int64
	recipientExists    bool
	fee                int64
	allowSameTokenName bool
}

func (v *transferAssetValidation) reset() { *v = transferAssetValidation{} }

func (v *transferAssetValidation) matches(ctx *Context) bool {
	return v.ctx == ctx && v.contract != nil && v.allowSameTokenName == ctx.DynProps.AllowSameTokenName()
}

// assetResolverCache carries the immutable asset metadata resolved during
// Validate into Execute. Core creates one actuator per transaction, and no
// TRC10 metadata mutation occurs between those two calls. Matching the wire
// name and fork mode keeps direct/reused test calls safe as well.
type assetResolverCache struct {
	assetName          []byte
	allowSameTokenName bool
	resolved           *resolvedAsset
}

func (c *assetResolverCache) reset() {
	c.assetName = nil
	c.allowSameTokenName = false
	c.resolved = nil
}

func (c *assetResolverCache) resolve(ctx *Context, assetName []byte) (*resolvedAsset, error) {
	allowSameTokenName := ctx.DynProps.AllowSameTokenName()
	if c.resolved != nil && bytes.Equal(c.assetName, assetName) && c.allowSameTokenName == allowSameTokenName {
		return c.resolved, nil
	}
	resolved, err := resolveAsset(ctx, assetName)
	if err != nil {
		return nil, err
	}
	c.assetName = assetName
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
		id, exists, err := ctx.State.ReadAssetIssueIDByName(assetName)
		if err != nil {
			return 0, errors.New("invalid legacy asset ID")
		}
		if exists {
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

// resolveTransferAssetID is narrower than resolveAsset because a transfer
// consumes no issuance metadata. Pre-fork it extracts only field 41 from the
// legacy row; post-fork it mirrors AssetIssueV2Store.has() with an existence
// probe. ParticipateAssetIssue continues to use resolveAsset above because it
// needs owner, times and exchange-rate fields.
func resolveTransferAssetID(ctx *Context, assetName []byte) (int64, error) {
	if !ctx.DynProps.AllowSameTokenName() {
		tokenID, exists, err := ctx.State.ReadAssetIssueIDByName(assetName)
		if err != nil {
			return 0, errors.New("invalid legacy asset ID")
		}
		if exists {
			return tokenID, nil
		}
		tokenID, ok := ctx.State.ReadAssetNameIndex(assetName)
		if !ok {
			return 0, errors.New("invalid asset_name: no name index hit")
		}
		if !ctx.State.HasAssetIssue(tokenID) {
			return 0, errors.New("token not found")
		}
		return tokenID, nil
	}
	tokenID, err := resolveAssetNameOrID(ctx, assetName)
	if err != nil {
		return 0, err
	}
	if !ctx.State.HasAssetIssue(tokenID) {
		return 0, errors.New("token not found")
	}
	return tokenID, nil
}

func (a *TransferAssetActuator) getContract(ctx *Context) (*contractpb.TransferAssetContract, error) {
	return decodedContract[*contractpb.TransferAssetContract](ctx, "TransferAssetContract")
}

func (a *TransferAssetActuator) Validate(ctx *Context) error {
	a.validated.reset()
	if ctx.State == nil {
		return errors.New("state not available")
	}
	c, err := a.getContract(ctx)
	if err != nil {
		return err
	}
	tokenID, err := resolveTransferAssetID(ctx, c.AssetName)
	if err != nil {
		return err
	}
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
	allowSameTokenName := ctx.DynProps.AllowSameTokenName()
	fromAssetBalance, err := ctx.State.GetTRC10BalanceFinalStrict(from, c.AssetName, tokenID, allowSameTokenName)
	if err != nil {
		return err
	}
	if fromAssetBalance < c.Amount {
		return errors.New("insufficient TRC10 balance")
	}
	toType, toExists := ctx.State.GetAccountType(to)
	toAssetBalance := int64(0)
	fee := int64(0)
	if toExists {
		if ctx.DynProps.ForbidTransferToContract() && toType == corepb.AccountType_Contract {
			return errors.New("cannot transfer TRC10 to a smart contract")
		}
		toAssetBalance, err = ctx.State.GetTRC10BalanceFinalStrict(to, c.AssetName, tokenID, allowSameTokenName)
		if err != nil {
			return err
		}
		if toAssetBalance > math.MaxInt64-c.Amount {
			return errors.New("recipient TRC10 balance overflows int64")
		}
	} else {
		fee = ctx.DynProps.CreateNewAccountFeeInSystemContract()
		if ctx.State.GetBalance(from) < fee {
			return errors.New("insufficient balance for create account fee")
		}
	}
	a.validated = transferAssetValidation{
		ctx:                ctx,
		contract:           c,
		from:               from,
		to:                 to,
		tokenID:            tokenID,
		fromAssetBalance:   fromAssetBalance,
		toAssetBalance:     toAssetBalance,
		recipientExists:    toExists,
		fee:                fee,
		allowSameTokenName: allowSameTokenName,
	}
	return nil
}

func (a *TransferAssetActuator) Execute(ctx *Context) (*Result, error) {
	plan, err := a.executionPlan(ctx)
	if err != nil {
		return nil, err
	}
	c := plan.contract
	if plan.fee > 0 && ctx.State.GetBalance(plan.from) < plan.fee {
		return nil, errors.New("insufficient balance for create account fee")
	}
	if !plan.recipientExists {
		ctx.State.CreateAccountWithTime(plan.to, corepb.AccountType_Normal, ctx.DynProps.LatestBlockHeaderTimestamp())
		if ctx.DynProps.AllowMultiSign() {
			ctx.State.ApplyDefaultAccountPermissions(plan.to, ctx.DynProps)
		}
		// Actuator-level extra fee (proposal #12, default 0). java-tron does
		// NOT increment total_create_account_cost here — see transfer.go for
		// the rationale.
		if err := burnFee(ctx, plan.from, plan.fee); err != nil {
			return nil, err
		}
	}

	if err := ctx.State.SetTRC10BalanceFinalStrict(plan.from, c.AssetName, plan.tokenID, plan.fromAssetBalance-c.Amount, plan.allowSameTokenName); err != nil {
		return nil, err
	}
	if err := ctx.State.SetTRC10BalanceFinalStrict(plan.to, c.AssetName, plan.tokenID, plan.toAssetBalance+c.Amount, plan.allowSameTokenName); err != nil {
		return nil, err
	}

	result := ctx.newResult()
	result.Fee = plan.fee
	result.ContractRet = 1
	return result, nil
}

// executionPlan returns the successful Validate snapshot on the canonical
// path. Execute is also public and ApplyTransaction permits validate=false, so
// a cache miss must run the exact same validation rather than maintaining a
// weaker second implementation of consensus preconditions.
func (a *TransferAssetActuator) executionPlan(ctx *Context) (transferAssetValidation, error) {
	if a.validated.matches(ctx) {
		plan := a.validated
		a.validated.reset()
		return plan, nil
	}
	if err := a.Validate(ctx); err != nil {
		return transferAssetValidation{}, err
	}
	plan := a.validated
	a.validated.reset()
	return plan, nil
}
