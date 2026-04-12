package actuator

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	trawdb "github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

// ownerExchAddr is the test owner address (byte 0x41, last byte 0x02).
var ownerExchAddr = makeTestAddr(2)

func makeExchangeCreateTx(owner tcommon.Address, c *contractpb.ExchangeCreateContract) *types.Transaction {
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_ExchangeCreateContract,
					Parameter: anyParam,
				},
			},
		},
	}
	_ = owner
	return types.NewTransactionFromPB(pb)
}

func makeExchangeInjectTx(c *contractpb.ExchangeInjectContract) *types.Transaction {
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_ExchangeInjectContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func makeExchangeWithdrawTx(c *contractpb.ExchangeWithdrawContract) *types.Transaction {
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_ExchangeWithdrawContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

func makeExchangeTransactionTx(c *contractpb.ExchangeTransactionContract) *types.Transaction {
	anyParam, _ := anypb.New(c)
	pb := &corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Contract: []*corepb.Transaction_Contract{
				{
					Type:      corepb.Transaction_Contract_ExchangeTransactionContract,
					Parameter: anyParam,
				},
			},
		},
	}
	return types.NewTransactionFromPB(pb)
}

// setupExchangeCtx creates a test context with an owner seeded with 3000 TRX + 1,000,000 TRC10.
// 3000 TRX covers the 1024 TRX create fee + up to 1000 TRX token deposit.
func setupExchangeCtx(t *testing.T, tx *types.Transaction) *Context {
	t.Helper()
	statedb := setupStateDB(t)
	seedAccount(statedb, ownerExchAddr, 3_000_000_000)
	statedb.SetTRC10Balance(ownerExchAddr, 1_000_001, 1_000_000)

	ctx := setupContext(t, statedb, tx)
	ctx.DB = ethrawdb.NewMemoryDatabase()
	return ctx
}

func TestExchangeCreateBasic(t *testing.T) {
	c := &contractpb.ExchangeCreateContract{
		OwnerAddress:       ownerExchAddr.Bytes(),
		FirstTokenId:       []byte("_"),
		FirstTokenBalance:  1_000_000_000, // 1000 TRX
		SecondTokenId:      []byte("1000001"),
		SecondTokenBalance: 500_000,
	}
	ctx := setupExchangeCtx(t, makeExchangeCreateTx(ownerExchAddr, c))

	a := &ExchangeCreateActuator{}
	if err := a.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	result, err := a.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.ContractRet != 1 {
		t.Fatalf("ContractRet should be 1")
	}

	// Exchange ID 1 should exist
	ex := trawdb.ReadExchange(ctx.DB, 1)
	if ex == nil {
		t.Fatal("exchange not stored")
	}
	if ex.FirstTokenBalance != 1_000_000_000 {
		t.Fatalf("wrong FirstTokenBalance: %d", ex.FirstTokenBalance)
	}
	// next_exchange_id should be 2
	if ctx.DynProps.NextExchangeID() != 2 {
		t.Fatalf("expected next_exchange_id=2, got %d", ctx.DynProps.NextExchangeID())
	}
	// Owner should have paid fee (1024 TRX = 1,024,000,000 sun) + first token balance (1000 TRX)
	expectedBalance := int64(3_000_000_000) - 1_000_000_000 - 1_024_000_000
	if ctx.State.GetBalance(ownerExchAddr) != expectedBalance {
		t.Fatalf("unexpected owner TRX balance: %d (expected %d)", ctx.State.GetBalance(ownerExchAddr), expectedBalance)
	}
}

func TestExchangeInjectBasic(t *testing.T) {
	// First create an exchange
	createC := &contractpb.ExchangeCreateContract{
		OwnerAddress:       ownerExchAddr.Bytes(),
		FirstTokenId:       []byte("_"),
		FirstTokenBalance:  1_000_000_000,
		SecondTokenId:      []byte("1000001"),
		SecondTokenBalance: 500_000,
	}
	ctx := setupExchangeCtx(t, makeExchangeCreateTx(ownerExchAddr, createC))
	(&ExchangeCreateActuator{}).Execute(ctx) //nolint

	// Give owner more tokens for injection
	ctx.State.AddBalance(ownerExchAddr, 500_000_000)    // +500 TRX
	ctx.State.AddTRC10Balance(ownerExchAddr, 1_000_001, 500_000)

	// Inject 200 TRX
	injectC := &contractpb.ExchangeInjectContract{
		OwnerAddress: ownerExchAddr.Bytes(),
		ExchangeId:   1,
		TokenId:      []byte("_"),
		Quant:        200_000_000, // 200 TRX
	}
	ctx.Tx = makeExchangeInjectTx(injectC)

	a := &ExchangeInjectActuator{}
	if err := a.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	_, err := a.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	ex := trawdb.ReadExchange(ctx.DB, 1)
	// 1000M + 200M = 1200M TRX in pool
	if ex.FirstTokenBalance != 1_200_000_000 {
		t.Fatalf("wrong FirstTokenBalance after inject: %d", ex.FirstTokenBalance)
	}
	// 500000 * 200M / 1000M = 100000 second token added → 600000
	if ex.SecondTokenBalance != 600_000 {
		t.Fatalf("wrong SecondTokenBalance after inject: %d", ex.SecondTokenBalance)
	}
}

func TestExchangeWithdrawBasic(t *testing.T) {
	// Create exchange with 1000M TRX and 500000 TRC10
	createC := &contractpb.ExchangeCreateContract{
		OwnerAddress:       ownerExchAddr.Bytes(),
		FirstTokenId:       []byte("_"),
		FirstTokenBalance:  1_000_000_000,
		SecondTokenId:      []byte("1000001"),
		SecondTokenBalance: 500_000,
	}
	ctx := setupExchangeCtx(t, makeExchangeCreateTx(ownerExchAddr, createC))
	(&ExchangeCreateActuator{}).Execute(ctx) //nolint

	trxBefore := ctx.State.GetBalance(ownerExchAddr)
	trc10Before := ctx.State.GetTRC10Balance(ownerExchAddr, 1_000_001)

	// Withdraw 200M TRX
	withdrawC := &contractpb.ExchangeWithdrawContract{
		OwnerAddress: ownerExchAddr.Bytes(),
		ExchangeId:   1,
		TokenId:      []byte("_"),
		Quant:        200_000_000,
	}
	ctx.Tx = makeExchangeWithdrawTx(withdrawC)

	a := &ExchangeWithdrawActuator{}
	if err := a.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	_, err := a.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	// Owner gets back 200M TRX + proportional TRC10
	// proportional TRC10: 500000 * 200M / 1000M = 100000
	trxAfter := ctx.State.GetBalance(ownerExchAddr)
	trc10After := ctx.State.GetTRC10Balance(ownerExchAddr, 1_000_001)

	if trxAfter != trxBefore+200_000_000 {
		t.Fatalf("TRX after withdraw: got %d, want %d", trxAfter, trxBefore+200_000_000)
	}
	if trc10After != trc10Before+100_000 {
		t.Fatalf("TRC10 after withdraw: got %d, want %d", trc10After, trc10Before+100_000)
	}
	ex := trawdb.ReadExchange(ctx.DB, 1)
	if ex.FirstTokenBalance != 800_000_000 || ex.SecondTokenBalance != 400_000 {
		t.Fatalf("exchange state wrong: %+v", ex)
	}
}

func TestExchangeTransactionBasic(t *testing.T) {
	// Create exchange: 1000M TRX / 500000 TRC10
	createC := &contractpb.ExchangeCreateContract{
		OwnerAddress:       ownerExchAddr.Bytes(),
		FirstTokenId:       []byte("_"),
		FirstTokenBalance:  1_000_000_000,
		SecondTokenId:      []byte("1000001"),
		SecondTokenBalance: 500_000,
	}
	ctx := setupExchangeCtx(t, makeExchangeCreateTx(ownerExchAddr, createC))
	(&ExchangeCreateActuator{}).Execute(ctx) //nolint

	// Trader sells 100M TRX into the pool. The expected receive is computed
	// with the Bancor formula (see exchange_processor.go) — the same routine
	// the actuator runs, so any drift is caught here.
	ctx.State.AddBalance(ownerExchAddr, 200_000_000) // extra TRX to sell
	trc10Before := ctx.State.GetTRC10Balance(ownerExchAddr, 1_000_001)
	expectedReceive := newExchangeProcessor().exchange(1_000_000_000, 500_000, 100_000_000)

	txC := &contractpb.ExchangeTransactionContract{
		OwnerAddress: ownerExchAddr.Bytes(),
		ExchangeId:   1,
		TokenId:      []byte("_"),
		Quant:        100_000_000,
		Expected:     1, // slippage tolerance (any nonzero payout is fine here)
	}
	ctx.Tx = makeExchangeTransactionTx(txC)

	a := &ExchangeTransactionActuator{}
	if err := a.Validate(ctx); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	_, err := a.Execute(ctx)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}

	trc10After := ctx.State.GetTRC10Balance(ownerExchAddr, 1_000_001)
	if trc10After != trc10Before+expectedReceive {
		t.Fatalf("got %d TRC10, want %d (delta=%d)", trc10After, trc10Before+expectedReceive, expectedReceive)
	}

	// Check exchange pool updated.
	ex := trawdb.ReadExchange(ctx.DB, 1)
	if ex.FirstTokenBalance != 1_100_000_000 {
		t.Fatalf("wrong FirstTokenBalance: %d", ex.FirstTokenBalance)
	}
	if ex.SecondTokenBalance != 500_000-expectedReceive {
		t.Fatalf("pool buy-side: got %d, want %d", ex.SecondTokenBalance, 500_000-expectedReceive)
	}
}
