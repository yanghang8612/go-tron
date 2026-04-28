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

func makeUpdateAssetTx(ownerByte byte, desc, url string, newLimit, newPublicLimit int64) *types.Transaction {
	owner := makeTestAddr(ownerByte)
	c := &contractpb.UpdateAssetContract{
		OwnerAddress:   owner.Bytes(),
		Description:    []byte(desc),
		Url:            []byte(url),
		NewLimit:       newLimit,
		NewPublicLimit: newPublicLimit,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_UpdateAssetContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func TestUpdateAssetValidate_Success(t *testing.T) {
	owner := makeTestAddr(1)
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], 1_000_001); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, 1_000_001, &contractpb.AssetIssueContract{
		Name: []byte("MYTOKEN"),
	}); err != nil {
		t.Fatal(err)
	}

	tx := makeUpdateAssetTx(1, "new desc", "http://new.url", 500, 1000)
	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &UpdateAssetActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestUpdateAssetValidate_NotOwner(t *testing.T) {
	nonOwner := makeTestAddr(2)
	db := ethrawdb.NewMemoryDatabase()
	// No entry for nonOwner in owner index

	tx := makeUpdateAssetTx(2, "desc", "url", 0, 0)
	statedb := setupStateDB(t)
	statedb.CreateAccount(nonOwner, corepb.AccountType_Normal)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &UpdateAssetActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: not token owner")
	}
}

func TestUpdateAssetValidate_NewLimitOutOfRange(t *testing.T) {
	owner := makeTestAddr(1)
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], 1_000_001); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, 1_000_001, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)

	act := &UpdateAssetActuator{}

	// negative new_limit
	tx := makeUpdateAssetTx(1, "", "", -1, 0)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for negative new_limit")
	}

	// new_limit == oneDayNetLimit (at upper bound)
	tx = makeUpdateAssetTx(1, "", "", ctx.DynProps.OneDayNetLimit(), 0)
	ctx = setupContext(t, statedb, tx)
	ctx.DB = db
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for new_limit >= one_day_net_limit")
	}
}

func TestUpdateAssetValidate_NewPublicLimitOutOfRange(t *testing.T) {
	owner := makeTestAddr(1)
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], 1_000_001); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, 1_000_001, &contractpb.AssetIssueContract{Name: []byte("T")}); err != nil {
		t.Fatal(err)
	}

	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	act := &UpdateAssetActuator{}

	tx := makeUpdateAssetTx(1, "", "", 0, -1)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for negative new_public_limit")
	}

	tx = makeUpdateAssetTx(1, "", "", 0, ctx.DynProps.OneDayNetLimit())
	ctx = setupContext(t, statedb, tx)
	ctx.DB = db
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error for new_public_limit >= one_day_net_limit")
	}
}

func TestUpdateAssetExecute(t *testing.T) {
	owner := makeTestAddr(1)
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetOwnerIndex(db, owner[:], 1_000_001); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, 1_000_001, &contractpb.AssetIssueContract{
		Name:                    []byte("MYTOKEN"),
		Description:             []byte("old desc"),
		Url:                     []byte("http://old.url"),
		FreeAssetNetLimit:       100,
		PublicFreeAssetNetLimit: 200,
	}); err != nil {
		t.Fatal(err)
	}

	tx := makeUpdateAssetTx(1, "new desc", "http://new.url", 500, 1000)
	statedb := setupStateDB(t)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	ctx := setupContext(t, statedb, tx)
	ctx.DB = db

	act := &UpdateAssetActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	updated := rawdb.ReadAssetIssue(db, 1_000_001)
	if updated == nil {
		t.Fatal("asset should still be in rawdb")
	}
	if string(updated.Description) != "new desc" {
		t.Fatalf("description: want 'new desc', got %s", updated.Description)
	}
	if string(updated.Url) != "http://new.url" {
		t.Fatalf("url: want 'http://new.url', got %s", updated.Url)
	}
	if updated.FreeAssetNetLimit != 500 {
		t.Fatalf("free_asset_net_limit: want 500, got %d", updated.FreeAssetNetLimit)
	}
	if updated.PublicFreeAssetNetLimit != 1000 {
		t.Fatalf("public_free_asset_net_limit: want 1000, got %d", updated.PublicFreeAssetNetLimit)
	}
}
