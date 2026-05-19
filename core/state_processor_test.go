package core

import (
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/types/known/anypb"
)

func newTestState(t *testing.T) *state.StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func testProcessorAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func makeTestTransferTx(from, to byte, amount int64) *types.Transaction {
	tc := &contractpb.TransferContract{
		OwnerAddress: testProcessorAddr(from).Bytes(),
		ToAddress:    testProcessorAddr(to).Bytes(),
		Amount:       amount,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TransferContract,
				Parameter: param,
			}},
		},
	})
}

func TestApplyTransaction_Transfer(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)
	// Pre-create the recipient so this stays on the regular bandwidth path.
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	tx := makeTestTransferTx(1, 2, 300_000)
	result, err := ApplyTransaction(statedb, dynProps, tx, 3000, 3000, 1, nil, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if result.Fee != 0 {
		t.Fatalf("fee: got %d, want 0", result.Fee)
	}
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 700_000 {
		t.Fatalf("sender: got %d, want 700000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(2)); got != 300_000 {
		t.Fatalf("recipient: got %d, want 300000", got)
	}
}

func TestApplyTransaction_ValidationFails(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	// No account seeded — validation should fail
	tx := makeTestTransferTx(1, 2, 100)
	_, err := ApplyTransaction(statedb, dynProps, tx, 3000, 3000, 1, nil, nil, true, false)
	if err == nil {
		t.Fatal("expected validation error")
	}
}

func TestProcessBlock_WithTransactions(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)
	statedb.CreateAccount(testProcessorAddr(3), corepb.AccountType_Normal)

	// Commit the initial state so we have a clean base
	_, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	tx2 := makeTestTransferTx(1, 3, 2_000_000)

	witnessAddr := testProcessorAddr(0xFF)
	// Witnesses always have an account in practice (created before becoming witness)
	statedb.CreateAccount(witnessAddr, corepb.AccountType_Normal)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3000,
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{tx1.Proto(), tx2.Proto()},
	})

	txInfos, err := ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	_ = txInfos

	// Verify: sender lost 3M, recipients got 1M and 2M
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 7_000_000 {
		t.Fatalf("sender: got %d, want 7000000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(2)); got != 1_000_000 {
		t.Fatalf("recipient 2: got %d, want 1000000", got)
	}
	if got := statedb.GetBalance(testProcessorAddr(3)); got != 2_000_000 {
		t.Fatalf("recipient 3: got %d, want 2000000", got)
	}

	// Verify witness reward
	reward := dynProps.WitnessPayPerBlock()
	if got := statedb.GetAllowance(witnessAddr); got != reward {
		t.Fatalf("witness reward: got %d, want %d", got, reward)
	}
}

func TestProcessBlock_FailingTxRevertsState(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 100)

	// tx tries to transfer 200 — should fail validation
	tx := makeTestTransferTx(1, 2, 200)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	_, err := ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if err == nil {
		t.Fatal("expected error for invalid transaction")
	}

	// Balance should be unchanged
	if got := statedb.GetBalance(testProcessorAddr(1)); got != 100 {
		t.Fatalf("balance should be unchanged: got %d, want 100", got)
	}
}

func TestApplyTransaction_ReturnsResult(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)

	tx := makeTestTransferTx(1, 2, 300_000)
	result, err := ApplyTransaction(statedb, dynProps, tx, 3000, 3000, 1, nil, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	if result == nil {
		t.Fatal("expected non-nil result")
	}
	if result.ContractRet != 1 {
		t.Fatalf("expected ContractRet=1, got %d", result.ContractRet)
	}
}

// makeExchangeTransactionTx builds a syntactically valid
// ExchangeTransactionContract transaction. Used by the v33 fork-gated
// reject tests below.
func makeExchangeTransactionTx(owner byte) *types.Transaction {
	tc := &contractpb.ExchangeTransactionContract{
		OwnerAddress: testProcessorAddr(owner).Bytes(),
		ExchangeId:   1,
		TokenId:      []byte("_"),
		Quant:        1,
		Expected:     1,
	}
	param, _ := anypb.New(tc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 1_700_000_060_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_ExchangeTransactionContract,
				Parameter: param,
			}},
		},
	})
}

// TestApplyTransaction_ExchangeRejectedAfterFork seeds the v33 fork bitmap
// at quorum and asserts that an ExchangeTransactionContract is rejected at
// the block-apply path with the master-aligned error string. Mirrors
// java-tron Manager.rejectExchangeTransaction (PR #6507).
func TestApplyTransaction_ExchangeRejectedAfterFork(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties() // maintenance_time_interval defaults to 21_600_000

	db := ethrawdb.NewMemoryDatabase()
	// Seed v33 votes at quorum: 70% of 27 witnesses = ceil(18.9) = 19.
	stats := make([]byte, 27)
	for i := 0; i < 19; i++ {
		stats[i] = forks.VoteUpgrade
	}
	rawdb.WriteForkStats(db, 33, stats)

	tx := makeExchangeTransactionTx(1)
	// blockTime well past the v33 HardForkTime ceiling.
	_, err := ApplyTransaction(statedb, dynProps, tx, 1_700_000_000_000, 1_700_000_000_000, 1, db, nil, false, false)
	if !errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("expected ErrExchangeRejected, got %v", err)
	}
}

// TestApplyTransaction_ExchangePassesPreFork asserts that with no v33
// votes, the early reject does not fire — preserving replay safety for
// historical pre-fork blocks. Whether the actuator itself succeeds is
// unrelated to this gate; the test only locks in that the early-return
// path is gated.
func TestApplyTransaction_ExchangePassesPreFork(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	db := ethrawdb.NewMemoryDatabase()
	// No fork stats written → PassVersion returns false.

	tx := makeExchangeTransactionTx(1)
	_, err := ApplyTransaction(statedb, dynProps, tx, 1_700_000_000_000, 1_700_000_000_000, 1, db, nil, false, false)
	// The actuator can fail later for unrelated reasons (no exchange
	// state seeded); the only thing we care about here is that the
	// failure mode is NOT the v33 early reject.
	if errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("pre-fork exchange tx must not hit the v33 early reject; got %v", err)
	}
}

func TestProcessBlock_RejectsRetCountGreaterThanContractCountWhenOptimized(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetConsensusLogicOptimization(true)

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	tx := makeTestTransferTx(1, 2, 1)
	tx.Proto().Ret = []*corepb.Transaction_Result{
		{Ret: corepb.Transaction_Result_SUCESS},
		{Ret: corepb.Transaction_Result_SUCESS},
	}

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	_, err := ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if !errors.Is(err, ErrTransactionRetCount) {
		t.Fatalf("expected ErrTransactionRetCount, got %v", err)
	}
}

func TestProcessBlock_RejectsVMContractRetMismatch(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 100_000_000)
	statedb.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress:   owner.Bytes(),
		ContractAddress: contractAddr.Bytes(),
	})
	statedb.SetCode(contractAddr, []byte{0x00}) // STOP

	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    owner.Bytes(),
		ContractAddress: contractAddr.Bytes(),
	}
	param, err := anypb.New(tsc)
	if err != nil {
		t.Fatal(err)
	}
	tx := types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			FeeLimit:   10_000_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TriggerSmartContract,
				Parameter: param,
			}},
		},
		Ret: []*corepb.Transaction_Result{{
			ContractRet: corepb.Transaction_Result_REVERT,
		}},
	})

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:    1,
				Timestamp: 3000,
			},
		},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	_, err = ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if !errors.Is(err, ErrTransactionRetMismatch) {
		t.Fatalf("expected ErrTransactionRetMismatch, got %v", err)
	}
	if got := statedb.GetBalance(owner); got != 100_000_000 {
		t.Fatalf("failed block must roll back state: owner balance got %d, want 100000000", got)
	}
}

func TestProcessBlock_ReturnsTransactionInfos(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	_, err := statedb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	tx1 := makeTestTransferTx(1, 2, 1_000_000)
	tx2 := makeTestTransferTx(1, 3, 2_000_000)
	witnessAddr := testProcessorAddr(0xFF)
	statedb.CreateAccount(witnessAddr, corepb.AccountType_Normal)

	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         1,
				Timestamp:      3000,
				WitnessAddress: witnessAddr.Bytes(),
			},
		},
		Transactions: []*corepb.Transaction{tx1.Proto(), tx2.Proto()},
	})

	txInfos, err := ProcessBlock(statedb, dynProps, block, nil, nil, 0, false)
	if err != nil {
		t.Fatal(err)
	}
	if len(txInfos) != 2 {
		t.Fatalf("expected 2 txInfos, got %d", len(txInfos))
	}
	for i, info := range txInfos {
		if info.BlockNumber != 1 {
			t.Fatalf("txInfo[%d] blockNumber: got %d, want 1", i, info.BlockNumber)
		}
		if info.BlockTimeStamp != 3000 {
			t.Fatalf("txInfo[%d] blockTimeStamp: got %d, want 3000", i, info.BlockTimeStamp)
		}
		if len(info.Id) == 0 {
			t.Fatalf("txInfo[%d] has empty ID", i)
		}
	}
}

func TestBuildTransactionInfo_PackingFee(t *testing.T) {
	tx := makeTestTransferTx(1, 2, 100)

	result := &actuator.Result{
		NetFee:             123,
		NetFeeForBandwidth: true,
		EnergyFee:          456,
		ContractRet:        int32(corepb.Transaction_Result_SUCCESS),
	}
	info := buildTransactionInfo(tx, result, 1, 3000, true)
	if info.PackingFee != 579 {
		t.Fatalf("packingFee: got %d, want 579", info.PackingFee)
	}

	info = buildTransactionInfo(tx, result, 1, 3000, false)
	if info.PackingFee != 0 {
		t.Fatalf("packingFee without support_transaction_fee_pool: got %d, want 0", info.PackingFee)
	}

	result.NetFeeForBandwidth = false
	info = buildTransactionInfo(tx, result, 1, 3000, true)
	if info.PackingFee != 456 {
		t.Fatalf("packingFee must exclude create-account net fee: got %d, want 456", info.PackingFee)
	}

	result.ContractRet = int32(corepb.Transaction_Result_OUT_OF_TIME)
	info = buildTransactionInfo(tx, result, 1, 3000, true)
	if info.PackingFee != 0 {
		t.Fatalf("packingFee must exclude OUT_OF_TIME energy fee: got %d, want 0", info.PackingFee)
	}
}

func TestApplyTransaction_IncludesMemoAndMultiSignFees(t *testing.T) {
	statedb := newTestState(t)
	dp := state.NewDynamicProperties()
	dp.SetAllowMultiSign(true)
	dp.SetMultiSignFee(10)
	dp.SetMemoFee(20)

	owner := testProcessorAddr(1)
	to := testProcessorAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 1_000_000)

	tx := makeTestTransferTx(1, 2, 100)
	tx.Proto().Signature = [][]byte{make([]byte, 65), make([]byte, 65)}
	tx.Proto().RawData.Data = []byte("memo")

	db := ethrawdb.NewMemoryDatabase()
	result, err := ApplyTransaction(statedb, dp, tx, 0, 3000, 1, db, nil, true, false)
	if err != nil {
		t.Fatalf("ApplyTransaction: %v", err)
	}
	if result.Fee != 30 {
		t.Fatalf("result fee: got %d, want 30", result.Fee)
	}
	info := buildTransactionInfo(tx, result, 1, 3000, false)
	if info.Fee != 30 {
		t.Fatalf("transaction info fee: got %d, want 30", info.Fee)
	}
	if got := statedb.GetBalance(owner); got != 1_000_000-100-30 {
		t.Fatalf("owner balance: got %d, want %d", got, int64(1_000_000-100-30))
	}
}

func TestApplyTransaction_RollsBackPreExecutionFeesOnMemoFailure(t *testing.T) {
	statedb := newTestState(t)
	dp := state.NewDynamicProperties()
	dp.SetAllowMultiSign(true)
	dp.SetMultiSignFee(100)
	dp.SetMemoFee(100)
	dp.SetAllowBlackHoleOptimization(true)

	owner := testProcessorAddr(1)
	to := testProcessorAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.CreateAccount(to, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 150)

	tx := makeTestTransferTx(1, 2, 1)
	tx.Proto().Signature = [][]byte{make([]byte, 65), make([]byte, 65)}
	tx.Proto().RawData.Data = []byte("memo")

	db := ethrawdb.NewMemoryDatabase()
	if _, err := ApplyTransaction(statedb, dp, tx, 0, 3000, 1, db, nil, true, false); err == nil {
		t.Fatal("expected memo fee failure")
	}
	if got := statedb.GetBalance(owner); got != 150 {
		t.Fatalf("owner balance should be rolled back, got %d want 150", got)
	}
	if got := dp.BurnTrxAmount(); got != 0 {
		t.Fatalf("burn_trx_amount should be rolled back, got %d", got)
	}
}
