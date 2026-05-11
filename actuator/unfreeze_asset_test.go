package actuator

import (
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

// setupUnfreezeCtx creates a context with:
// - owner at ownerByte has issued a token with frozen_supply[0] = {frozenAmount, frozenDays}
// - issue time stored in rawdb
// - ctx.BlockTime = issueTime + blockTimeOffsetMs
func setupUnfreezeCtx(t *testing.T, ownerByte byte, frozenAmount, frozenDays int64, blockTimeOffset int64) *Context {
	t.Helper()
	owner := makeTestAddr(ownerByte)
	issueTime := int64(1_000_000)

	asset := &contractpb.AssetIssueContract{
		Name:        []byte("FROZENTOKEN"),
		TotalSupply: 1_000_000,
		Id:          "1000001",
		FrozenSupply: []*contractpb.AssetIssueContract_FrozenSupply{
			{FrozenAmount: frozenAmount, FrozenDays: frozenDays},
		},
	}
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, unfreezeTokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], unfreezeTokenID); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssueTime(db, unfreezeTokenID, issueTime); err != nil {
		t.Fatal(err)
	}

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	tx := makeUnfreezeAssetTx(ownerByte)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = issueTime + blockTimeOffset
	ctx.PrevBlockTime = ctx.BlockTime
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
	asset := &contractpb.AssetIssueContract{
		Name:        []byte("FROZENTOKEN"),
		TotalSupply: 1_000_000,
		Id:          "1000001",
		FrozenSupply: []*contractpb.AssetIssueContract_FrozenSupply{
			{FrozenAmount: 100_000, FrozenDays: 1},  // entry 0: 1 day
			{FrozenAmount: 200_000, FrozenDays: 30}, // entry 1: 30 days
		},
	}
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, unfreezeTokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], unfreezeTokenID); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssueTime(db, unfreezeTokenID, issueTime); err != nil {
		t.Fatal(err)
	}

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	tx := makeUnfreezeAssetTx(1)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = issueTime + 2*testDayMs // 2 days: entry 0 eligible, entry 1 not
	ctx.PrevBlockTime = ctx.BlockTime

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
	// Entry 0 marked claimed
	if !statedb.IsFrozenClaimed(owner, unfreezeTokenID, 0) {
		t.Fatal("entry 0 should be marked claimed")
	}
	// Entry 1 not claimed
	if statedb.IsFrozenClaimed(owner, unfreezeTokenID, 1) {
		t.Fatal("entry 1 should not be claimed yet")
	}
}

func TestUnfreezeAssetExecute_AlreadyClaimed(t *testing.T) {
	// Entry already claimed — second call should find nothing to unfreeze
	ctx := setupUnfreezeCtx(t, 1, 200_000, 1, 2*testDayMs)
	owner := makeTestAddr(1)
	// Mark entry 0 as already claimed
	ctx.State.SetFrozenClaimed(owner, unfreezeTokenID, 0)

	act := &UnfreezeAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: nothing to unfreeze (already claimed)")
	}
}
