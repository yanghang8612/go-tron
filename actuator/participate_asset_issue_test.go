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

const participateTokenID = int64(1_000_001)

func makeParticipateAssetTx(buyerByte, issuerByte byte, tokenID int64, trxAmount int64) *types.Transaction {
	buyer := makeTestAddr(buyerByte)
	issuer := makeTestAddr(issuerByte)
	c := &contractpb.ParticipateAssetIssueContract{
		OwnerAddress: buyer.Bytes(),
		ToAddress:    issuer.Bytes(),
		AssetName:    []byte(strconv.FormatInt(tokenID, 10)),
		Amount:       trxAmount,
	}
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_ParticipateAssetIssueContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// makeTestICO creates a test context with buyer and issuer accounts, an asset in rawdb,
// and BlockTime set to midpoint of ICO window.
func makeTestICO(t *testing.T, buyerByte, issuerByte byte, trxNum, num int32, startTime, endTime int64) *Context {
	t.Helper()
	buyer := makeTestAddr(buyerByte)
	issuer := makeTestAddr(issuerByte)

	asset := &contractpb.AssetIssueContract{
		OwnerAddress: issuer.Bytes(),
		Name:         []byte("ICOTOKEN"),
		TotalSupply:  1_000_000_000,
		TrxNum:       trxNum,
		Num:          num,
		StartTime:    startTime,
		EndTime:      endTime,
		Id:           strconv.FormatInt(participateTokenID, 10),
	}
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, participateTokenID, asset); err != nil {
		t.Fatal(err)
	}

	tx := makeParticipateAssetTx(buyerByte, issuerByte, participateTokenID, 1_000_000)
	statedb := setupStateDB(t)
	statedb.CreateAccount(buyer, corepb.AccountType_Normal)
	statedb.CreateAccount(issuer, corepb.AccountType_Normal)
	statedb.AddBalance(buyer, 100_000_000)
	statedb.SetTRC10Balance(issuer, participateTokenID, 1_000_000_000)

	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.DynProps.SetAllowSameTokenName(true)
	ctx.BlockTime = (startTime + endTime) / 2 // midpoint within ICO window
	ctx.PrevBlockTime = ctx.BlockTime
	return ctx
}

func TestParticipateAssetValidate_Success(t *testing.T) {
	ctx := makeTestICO(t, 1, 2, 1, 100, 500, 2000)
	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate should pass: %v", err)
	}
}

func TestParticipateAssetValidate_ICONotStarted(t *testing.T) {
	ctx := makeTestICO(t, 1, 2, 1, 100, 5000, 10000) // window starts at 5000
	ctx.BlockTime = 100                              // before ICO
	ctx.PrevBlockTime = ctx.BlockTime
	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: ICO not started")
	}
}

func TestParticipateAssetValidate_ICOEnded(t *testing.T) {
	ctx := makeTestICO(t, 1, 2, 1, 100, 100, 500) // window ends at 500
	ctx.BlockTime = 1000                          // after ICO
	ctx.PrevBlockTime = ctx.BlockTime
	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: ICO ended")
	}
}

func TestParticipateAssetValidate_InsufficientTRX(t *testing.T) {
	ctx := makeTestICO(t, 1, 2, 1, 1, 500, 2000)
	buyer := makeTestAddr(1)
	// Drain all TRX
	bal := ctx.State.GetBalance(buyer)
	ctx.State.SubBalance(buyer, bal)
	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err == nil {
		t.Fatal("expected error: insufficient TRX")
	}
}

func TestParticipateAssetExecute(t *testing.T) {
	// rate: 1 TRX drop = 1 token (trxNum=1, num=1)
	buyer := makeTestAddr(1)
	issuer := makeTestAddr(2)
	asset := &contractpb.AssetIssueContract{
		OwnerAddress: issuer.Bytes(),
		Name:         []byte("ICOTOKEN"),
		TotalSupply:  10_000_000,
		TrxNum:       1,
		Num:          1,
		StartTime:    500,
		EndTime:      2000,
		Id:           strconv.FormatInt(participateTokenID, 10),
	}
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssue(db, participateTokenID, asset); err != nil {
		t.Fatal(err)
	}

	tx := makeParticipateAssetTx(1, 2, participateTokenID, 1_000_000)
	statedb := setupStateDB(t)
	statedb.CreateAccount(buyer, corepb.AccountType_Normal)
	statedb.CreateAccount(issuer, corepb.AccountType_Normal)
	statedb.AddBalance(buyer, 100_000_000)
	statedb.SetTRC10Balance(issuer, participateTokenID, 10_000_000)

	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.DynProps.SetAllowSameTokenName(true)
	ctx.BlockTime = 1000 // within window
	ctx.PrevBlockTime = ctx.BlockTime

	initialBuyerTRX := ctx.State.GetBalance(buyer)

	act := &ParticipateAssetIssueActuator{}
	result, err := act.Execute(ctx)
	if err != nil {
		t.Fatalf("execute failed: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatal("expected ContractRet=1")
	}

	// Buyer paid 1_000_000 TRX drops
	if got := ctx.State.GetBalance(buyer); got != initialBuyerTRX-1_000_000 {
		t.Fatalf("buyer TRX: want %d, got %d", initialBuyerTRX-1_000_000, got)
	}
	// Buyer received 1_000_000 tokens (1:1 rate)
	if got := ctx.State.GetTRC10Balance(buyer, participateTokenID); got != 1_000_000 {
		t.Fatalf("buyer TRC10: want 1000000, got %d", got)
	}
	// Issuer received TRX
	if got := ctx.State.GetBalance(issuer); got != 1_000_000 {
		t.Fatalf("issuer TRX: want 1000000, got %d", got)
	}
	// Issuer lost tokens
	if got := ctx.State.GetTRC10Balance(issuer, participateTokenID); got != 9_000_000 {
		t.Fatalf("issuer TRC10: want 9000000, got %d", got)
	}
}

func TestParticipateAssetExecute_PreSameTokenNameNumericNameUsesNameIndex(t *testing.T) {
	const numericName = "123"
	buyer := makeTestAddr(1)
	issuer := makeTestAddr(2)
	asset := &contractpb.AssetIssueContract{
		OwnerAddress: issuer.Bytes(),
		Name:         []byte(numericName),
		TotalSupply:  10_000_000,
		TrxNum:       1,
		Num:          1,
		StartTime:    500,
		EndTime:      2000,
		Id:           strconv.FormatInt(participateTokenID, 10),
	}
	db := ethrawdb.NewMemoryDatabase()
	if err := rawdb.WriteAssetIssueByName(db, []byte(numericName), asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, participateTokenID, asset); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetIssue(db, 123, &contractpb.AssetIssueContract{Name: []byte("OTHER")}); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteAssetNameIndex(db, []byte(numericName), participateTokenID); err != nil {
		t.Fatal(err)
	}

	c := &contractpb.ParticipateAssetIssueContract{
		OwnerAddress: buyer.Bytes(),
		ToAddress:    issuer.Bytes(),
		AssetName:    []byte(numericName),
		Amount:       1_000_000,
	}
	anyParam, _ := anypb.New(c)
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{Type: corepb.Transaction_Contract_ParticipateAssetIssueContract, Parameter: anyParam},
			},
		},
	})
	statedb := setupStateDB(t)
	statedb.CreateAccount(buyer, corepb.AccountType_Normal)
	statedb.CreateAccount(issuer, corepb.AccountType_Normal)
	statedb.AddBalance(buyer, 100_000_000)
	statedb.SetTRC10BalanceLegacyAndV2(issuer, []byte(numericName), participateTokenID, 10_000_000)

	ctx := setupContext(t, statedb, tx)
	ctx.DB = db
	ctx.BlockTime = 1000
	ctx.PrevBlockTime = ctx.BlockTime

	act := &ParticipateAssetIssueActuator{}
	if err := act.Validate(ctx); err != nil {
		t.Fatalf("validate must resolve numeric-looking pre-fork name through name index: %v", err)
	}
	if _, err := act.Execute(ctx); err != nil {
		t.Fatalf("execute must use name-index token ID: %v", err)
	}
	if got := ctx.State.GetTRC10Balance(buyer, participateTokenID); got != 1_000_000 {
		t.Fatalf("buyer name-index token balance: want 1000000, got %d", got)
	}
	if got := ctx.State.GetTRC10Balance(buyer, 123); got != 0 {
		t.Fatalf("buyer parsed-ID token balance must stay zero, got %d", got)
	}
}
