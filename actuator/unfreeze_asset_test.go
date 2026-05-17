package actuator

import (
	"strconv"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

const unfreezeTokenID = int64(1_000_001)
const testDayMs = int64(86_400_000) // use testDayMs to avoid conflict with dayMs in unfreeze_asset.go
const unfreezeTokenIDString = "1000001"

func makeUnfreezeAssetTx(ownerByte byte) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.UnfreezeAssetContract{
		OwnerAddress: owner.Bytes(),
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_UnfreezeAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// setupUnfreezeCtx creates a context with owner at ownerByte holding one
// account-level frozen_supply entry.
func setupUnfreezeCtx(t *testing.T, ownerByte byte, frozenAmount, frozenDays int64, blockTimeOffset int64) *Context {
	t.Helper()
	owner := makeTestAddr(ownerByte)
	issueTime := int64(1_000_000)

	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], unfreezeTokenID); err != nil {
		t.Fatal(err)
	}
	asset := &contractpb.AssetIssueContract{Name: []byte("FROZENTOKEN"), Id: unfreezeTokenIDString}
	if err := rawdb.WriteAssetIssueByName(db, []byte("FROZENTOKEN"), asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, unfreezeTokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetNameIndex(db, []byte("FROZENTOKEN"), unfreezeTokenID); err != nil {
		t.Fatal(err)
	}

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetAssetIssued(owner, []byte("FROZENTOKEN"), unfreezeTokenIDString)
	statedb.AddFrozenSupply(owner, []*corepb.Account_Frozen{
		{FrozenBalance: frozenAmount, ExpireTime: issueTime + frozenDays*testDayMs},
	})

	tx := makeUnfreezeAssetTx(ownerByte)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = issueTime + blockTimeOffset
	ctx.PrevBlockTime = ctx.BlockTime
	ctx.DynProps.SetLatestBlockHeaderTimestamp(ctx.BlockTime)
	return ctx
}

func TestUnfreezeAssetValidate_Success(t *testing.T) {
	// frozen 1 day; blockTime = issueTime + 2 days → eligible
	ctx := setupUnfreezeCtx(t, 1, 200_000, 1, 2*testDayMs)
	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestUnfreezeAssetValidate_FreezeNotElapsed(t *testing.T) {
	// frozen 30 days; blockTime = issueTime + 1 day → not eligible
	ctx := setupUnfreezeCtx(t, 1, 200_000, 30, 1*testDayMs)
	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: freeze period not elapsed")
	}
}

func TestUnfreezeAssetValidate_NotOwner(t *testing.T) {
	// owner at byte 1, trying byte 2
	ctx := setupUnfreezeCtx(t, 1, 200_000, 1, 2*testDayMs)
	// rebuild tx with a different owner
	otherOwner := makeTestAddr(2)
	ctx.State.CreateAccount(otherOwner, corepb.AccountType_Normal)
	c := &contractpb.UnfreezeAssetContract{OwnerAddress: otherOwner.Bytes()}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_UnfreezeAssetContract, Parameter: anyParam},
			},
		},
	}
	ctx.Tx = types.NewTransactionFromPB(pb)

	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: not token owner")
	}
}

func TestUnfreezeAssetExecute(t *testing.T) {
	// Two frozen entries: entry 0 is past due, entry 1 is still locked.
	owner := makeTestAddr(1)
	issueTime := int64(1_000_000)
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], unfreezeTokenID); err != nil {
		t.Fatal(err)
	}
	asset := &contractpb.AssetIssueContract{Name: []byte("FROZENTOKEN"), Id: unfreezeTokenIDString}
	if err := rawdb.WriteAssetIssueByName(db, []byte("FROZENTOKEN"), asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, unfreezeTokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetNameIndex(db, []byte("FROZENTOKEN"), unfreezeTokenID); err != nil {
		t.Fatal(err)
	}

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetAssetIssued(owner, []byte("FROZENTOKEN"), unfreezeTokenIDString)
	statedb.AddFrozenSupply(owner, []*corepb.Account_Frozen{
		{FrozenBalance: 100_000, ExpireTime: issueTime + 1*testDayMs},
		{FrozenBalance: 200_000, ExpireTime: issueTime + 30*testDayMs},
	})

	tx := makeUnfreezeAssetTx(1)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = issueTime + 2*testDayMs // 2 days: entry 0 eligible, entry 1 not
	ctx.PrevBlockTime = ctx.BlockTime
	ctx.DynProps.SetLatestBlockHeaderTimestamp(ctx.BlockTime)

	act := &UnfreezeAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Only entry 0 (100,000 tokens) should have been credited
	if got := statedb.GetTRC10Balance(owner, unfreezeTokenID); got != 100_000 {
		t.Fatalf("TRC10 balance: want 100000, got %d", got)
	}
	remaining := statedb.GetAccount(owner).Proto().GetFrozenSupply()
	if len(remaining) != 1 {
		t.Fatalf("remaining frozen_supply: want 1, got %d", len(remaining))
	}
	if remaining[0].FrozenBalance != 200_000 {
		t.Fatalf("remaining frozen balance: want 200000, got %d", remaining[0].FrozenBalance)
	}
}

func TestUnfreezeAsset_PreSameTokenNameUsesIssuedName(t *testing.T) {
	owner := makeTestAddr(1)
	issueTime := int64(1_000_000)
	db := ethrawdb.NewMemoryDatabase()
	asset := &contractpb.AssetIssueContract{Name: []byte("123"), Id: strconv.FormatInt(unfreezeTokenID, 10)}
	if err := rawdb.WriteAssetIssueByName(db, []byte("123"), asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, unfreezeTokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, 123, &contractpb.AssetIssueContract{Name: []byte("OTHER")}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetNameIndex(db, []byte("123"), unfreezeTokenID); err != nil {
		t.Fatal(err)
	}

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.SetAssetIssued(owner, []byte("123"), strconv.FormatInt(unfreezeTokenID, 10))
	statedb.AddFrozenSupply(owner, []*corepb.Account_Frozen{
		{FrozenBalance: 100_000, ExpireTime: issueTime + testDayMs},
	})

	tx := makeUnfreezeAssetTx(1)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = issueTime + 2*testDayMs
	ctx.PrevBlockTime = ctx.BlockTime
	ctx.DynProps.SetLatestBlockHeaderTimestamp(ctx.BlockTime)
	ctx.DynProps.SetAllowSameTokenName(false)

	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should use asset_issued_name before AllowSameTokenName: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute should credit name-index token ID: %v", err)
	}
	if got := statedb.GetTRC10Balance(owner, unfreezeTokenID); got != 100_000 {
		t.Fatalf("name-index token balance: got %d want 100000", got)
	}
	if got := statedb.GetTRC10Balance(owner, 123); got != 0 {
		t.Fatalf("parsed-ID token balance must stay zero, got %d", got)
	}
}

func TestUnfreezeAssetExecute_NoRemainingExpiredSupply(t *testing.T) {
	ctx := setupUnfreezeCtx(t, 1, 200_000, 1, 2*testDayMs)

	act := &UnfreezeAssetActuator{}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: nothing left to unfreeze")
	}
}
