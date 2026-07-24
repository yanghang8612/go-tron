package core

import (
	"bytes"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"github.com/tronprotocol/go-tron/vm"
	"google.golang.org/protobuf/types/known/anypb"
)

var transactionInfoBenchmarkSink *corepb.TransactionInfo

func BenchmarkTransactionInfoLogBuild(b *testing.B) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, nil)
	for _, tc := range []struct {
		name       string
		logCount   int
		topicCount int
	}{
		{name: "logs_1_topics_1", logCount: 1, topicCount: 1},
		{name: "logs_4_topics_4", logCount: 4, topicCount: 4},
	} {
		b.Run(tc.name, func(b *testing.B) {
			result := &actuator.Result{ContractRet: int32(corepb.Transaction_Result_SUCCESS)}
			result.Logs = make([]vm.Log, tc.logCount)
			for i := range result.Logs {
				result.Logs[i] = vm.Log{
					Address: contractAddr,
					Data:    bytes.Repeat([]byte{byte(i + 1)}, 64),
					Topics:  make([][]byte, tc.topicCount),
				}
				for topic := range result.Logs[i].Topics {
					result.Logs[i].Topics[topic] = bytes.Repeat([]byte{byte(topic + 1)}, 32)
				}
			}
			slot := new(transactionInfoSlot)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				transactionInfoBenchmarkSink = slot.build(tx, result, 1, 3000, false)
			}
		})
	}
}

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

func makeTestTriggerTx(owner byte, contractAddr tcommon.Address, data []byte) *types.Transaction {
	tsc := &contractpb.TriggerSmartContract{
		OwnerAddress:    testProcessorAddr(owner).Bytes(),
		ContractAddress: contractAddr.Bytes(),
		Data:            data,
	}
	param, _ := anypb.New(tsc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_TriggerSmartContract,
				Parameter: param,
			}},
		},
	})
}

func makeTestProposalCreateTx(owner tcommon.Address, params map[int64]int64) *types.Transaction {
	pc := &contractpb.ProposalCreateContract{
		OwnerAddress: owner.Bytes(),
		Parameters:   params,
	}
	param, _ := anypb.New(pc)
	return types.NewTransactionFromPB(&corepb.Transaction{
		RawData: &corepb.TransactionRaw{
			Expiration: 60_000,
			Contract: []*corepb.Transaction_Contract{{
				Type:      corepb.Transaction_Contract_ProposalCreateContract,
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

func TestApplyTransaction_CapturesOwnerSnapshot(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
	statedb.AddBalance(testProcessorAddr(1), 1_000_000)
	statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

	tx := makeTestTransferTx(1, 2, 300_000)
	result, err := ApplyTransaction(statedb, dynProps, tx, 3000, 3000, 1, nil, nil, true, false)
	if err != nil {
		t.Fatal(err)
	}
	// The diagnostic snapshot is taken at execution start, so it must report the
	// owner's pre-transfer balance (1_000_000) — NOT the post-transfer 700_000.
	if result.OwnerBalance != 1_000_000 {
		t.Fatalf("OwnerBalance = %d, want 1000000 (pre-tx snapshot, not post-transfer 700000)", result.OwnerBalance)
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

func TestApplyTransaction_InBlockPreConsensusSkipsResultSizeGate(t *testing.T) {
	run := func(consensusLogicOptimization bool) error {
		statedb := newTestState(t)
		dynProps := state.NewDynamicProperties()
		dynProps.SetConsensusLogicOptimization(consensusLogicOptimization)

		statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(1), 20_000_000)
		statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

		tx := makeTestTransferTx(1, 2, 1)
		tx.Proto().RawData.Expiration = 1001
		padTxDataToLargestValidSize(t, tx)

		_, err := applyTransaction(
			statedb, dynProps, tx, 1000, true, HeadSlot(1000, 0), 2000, 1,
			nil, nil, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, true, false, true, nil, nil,
		)
		return err
	}

	if err := run(false); err != nil {
		t.Fatalf("pre-consensus in-block transaction rejected: %v", err)
	}
	if err := run(true); !errors.Is(err, ErrTransactionTooLarge) {
		t.Fatalf("expected post-consensus result-size rejection, got %v", err)
	}
}

// TestApplyTransaction_InBlockExpirationLowerBound pins java Manager.validateCommon's
// in-block expiration LOWER bound (active once consensus_logic_optimization is on):
// the tx must not be expired as of the next block slot. With prevBlockTime=1000 and
// StateFlag=0, nextSlotTime = 1000 + 1*3000 = 4000, so an expiration in (1000, 4000)
// is accepted with the flag off but rejected with it on.
func TestApplyTransaction_InBlockExpirationLowerBound(t *testing.T) {
	run := func(clo bool, expiration int64) error {
		statedb := newTestState(t)
		dynProps := state.NewDynamicProperties()
		dynProps.SetConsensusLogicOptimization(clo)
		statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(1), 20_000_000)
		statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

		tx := makeTestTransferTx(1, 2, 1)
		tx.Proto().RawData.Expiration = expiration
		_, err := applyTransaction(
			statedb, dynProps, tx, 1000, true, HeadSlot(1000, 0), 2000, 1,
			nil, nil, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, true, false, true, nil, nil,
		)
		return err
	}

	if err := run(false, 2000); err != nil {
		t.Fatalf("CLO off: sub-slot expiration must pass (base bound only), got %v", err)
	}
	if err := run(true, 2000); !errors.Is(err, ErrTransactionExpiration) {
		t.Fatalf("CLO on: expiration < nextSlotTime must be rejected, got %v", err)
	}
	if err := run(true, 5000); err != nil {
		t.Fatalf("CLO on: expiration >= nextSlotTime must pass, got %v", err)
	}
}

// TestApplyTransaction_RejectsOversizedResult pins java BandwidthProcessor
// .consume's always-on (in-block) getResultSerializedSize() > 64*contractCount
// reject. A normal/no ret passes; a result padded past 64 bytes is rejected.
func TestApplyTransaction_RejectsOversizedResult(t *testing.T) {
	run := func(orderIDLen int) error {
		statedb := newTestState(t)
		dynProps := state.NewDynamicProperties()
		statedb.CreateAccount(testProcessorAddr(1), corepb.AccountType_Normal)
		statedb.AddBalance(testProcessorAddr(1), 20_000_000)
		statedb.CreateAccount(testProcessorAddr(2), corepb.AccountType_Normal)

		tx := makeTestTransferTx(1, 2, 1)
		if orderIDLen > 0 {
			tx.Proto().Ret = []*corepb.Transaction_Result{{OrderId: make([]byte, orderIDLen)}}
		}
		_, err := applyTransaction(
			statedb, dynProps, tx, 1000, true, HeadSlot(1000, 0), 2000, 1,
			nil, nil, params.DefaultBlockNumForEnergyLimit, tcommon.Hash{}, tcommon.Address{}, true, false, true, nil, nil,
		)
		return err
	}

	if err := run(0); err != nil {
		t.Fatalf("no ret: expected accept, got %v", err)
	}
	if err := run(100); !errors.Is(err, ErrTransactionResultTooLarge) {
		t.Fatalf("oversized ret (100-byte OrderId > 64): expected ErrTransactionResultTooLarge, got %v", err)
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

func TestProcessBlock_PassesGenesisHashToProposalValidation(t *testing.T) {
	nileGenesisHash := tcommon.HexToHash("0000000000000000d698d4192c56cb6be724a558448e2684802de4d6cd8690dc")
	type historicalProposalCase struct {
		name        string
		blockNumber int64
		proposal    map[int64]int64
	}
	cases := []historicalProposalCase{
		{
			name:        "shielded transaction",
			blockNumber: 1_628_391,
			proposal:    map[int64]int64{27: 1},
		},
		{
			name:        "shielded TRC20",
			blockNumber: 6_360_101,
			proposal:    map[int64]int64{39: 1},
		},
	}

	run := func(tc historicalProposalCase, genesisHash tcommon.Hash) error {
		diskdb := ethrawdb.NewMemoryDatabase()
		statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), state.NewDatabase(diskdb))
		if err != nil {
			t.Fatal(err)
		}
		dynProps := state.NewDynamicProperties()
		owner := testProcessorAddr(1)
		statedb.CreateAccount(owner, corepb.AccountType_Normal)
		statedb.PutWitness(owner, "http://w.com")

		tx := makeTestProposalCreateTx(owner, tc.proposal)
		block := types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:    tc.blockNumber,
					Timestamp: 3_000,
				},
			},
			Transactions: []*corepb.Transaction{tx.Proto()},
		})
		_, err = ProcessBlock(statedb, dynProps, block, diskdb, []tcommon.Address{owner}, 0, false, genesisHash)
		return err
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if err := run(tc, tcommon.Hash{}); err == nil {
				t.Fatal("expected historical proposal to fail without the Nile genesis hash")
			}
			if err := run(tc, nileGenesisHash); err != nil {
				t.Fatalf("Nile historical proposal rejected: %v", err)
			}
		})
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
	statedb.WriteForkStats(33, stats)

	tx := makeExchangeTransactionTx(1)
	// blockTime well past the v33 HardForkTime ceiling.
	_, err := ApplyTransaction(statedb, dynProps, tx, 1_700_000_000_000, 1_700_000_000_000, 1, db, nil, false, false)
	if !errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("expected ErrExchangeRejected, got %v", err)
	}
}

func TestApplyTransaction_ExchangeNileUsesVersion34(t *testing.T) {
	run := func(t *testing.T, passedVersion int32) error {
		t.Helper()
		statedb := newTestState(t)
		dynProps := state.NewDynamicProperties()
		db := ethrawdb.NewMemoryDatabase()
		stats := make([]byte, 27)
		for i := 0; i < 22; i++ { // v34 requires ceil(80% * 27) = 22
			stats[i] = forks.VoteUpgrade
		}
		statedb.WriteForkStats(passedVersion, stats)

		_, err := applyTransaction(
			statedb, dynProps, makeExchangeTransactionTx(1),
			1_700_000_000_000, true, 0, 1_700_000_000_000, 1,
			db, nil, params.DefaultBlockNumForEnergyLimit, params.NileGenesisHash,
			tcommon.Address{}, false, false, true, nil, nil,
		)
		return err
	}

	// Historical Nile version 33 was release-v4.8.1, before the exchange
	// disable patch. The transaction may fail later because this focused test
	// does not seed exchange state, but it must not hit the fork rejection.
	if err := run(t, 33); errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("historical Nile v33 must allow exchange transaction, got %v", err)
	}

	if err := run(t, 34); !errors.Is(err, ErrExchangeRejected) {
		t.Fatalf("Nile v34 must reject exchange transaction, got %v", err)
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
	if txInfos[0] == txInfos[1] || txInfos[0].Receipt == txInfos[1].Receipt {
		t.Fatal("transaction info slots alias each other's fixed-size messages")
	}
	secondIDFirstByte := txInfos[1].Id[0]
	txInfos[0].Id[0] ^= 0xff
	if txInfos[1].Id[0] != secondIDFirstByte {
		t.Fatal("transaction IDs share mutable backing storage")
	}
	txInfos[0].ContractResult[0] = []byte{1}
	if len(txInfos[1].ContractResult[0]) != 0 {
		t.Fatal("contract-result cells share mutable backing storage")
	}
	txInfos[0].Receipt.EnergyFee = 99
	if txInfos[1].Receipt.EnergyFee == 99 {
		t.Fatal("resource receipts share mutable backing storage")
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

func TestBuildTransactionInfo_IncludesEmptyVMContractResult(t *testing.T) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, nil)
	result := &actuator.Result{
		ContractRet:           int32(corepb.Transaction_Result_OUT_OF_TIME),
		ContractResult:        []byte{},
		ContractResultPresent: true,
		ResMessage:            []byte("Already Time Out"),
	}

	info := buildTransactionInfo(tx, result, 1, 3000, false)
	if len(info.ContractResult) != 1 {
		t.Fatalf("contractResult entries: got %d, want 1", len(info.ContractResult))
	}
	if len(info.ContractResult[0]) != 0 {
		t.Fatalf("contractResult[0] length: got %d, want 0", len(info.ContractResult[0]))
	}
	if got := info.Receipt.Result; got != corepb.Transaction_Result_OUT_OF_TIME {
		t.Fatalf("receipt result: got %s, want OUT_OF_TIME", got)
	}
	if got := string(info.ResMessage); got != "Already Time Out" {
		t.Fatalf("resMessage: got %q", got)
	}
}

func TestBuildTransactionInfo_DiagnosticReceiptFields(t *testing.T) {
	tx := makeTestTransferTx(1, 2, 100)
	result := &actuator.Result{
		ContractRet:                 int32(corepb.Transaction_Result_SUCCESS),
		OwnerBalance:                5_000_000,
		OwnerFreeNetLeft:            400,
		OwnerFrozenNetLeft:          700,
		OwnerNetLastConsumeTime:     111,
		OwnerFreeNetLastConsumeTime: 222,
		OwnerFrozenForNet:           1_000_000,
		OwnerFrozenForEnergy:        2_000_000,
		OriginEnergyWindow:          28_800,
		CallerEnergyWindow:          14_400,
		CallerEnergyLimit:           3_300,
		OriginEnergyLimit:           17_227_485,
		OriginFrozenForEnergy:       62_826_000_000,
		CallerEnergyUsagePre:        1_234,
		OriginEnergyUsagePre:        17_225_691,
		CallerEnergyLastConsumeTime: 551_787_654,
		OriginEnergyLastConsumeTime: 551_787_600,
		TotalEnergyWeight:           328_216_199,
		TotalEnergyCurrentLimit:     90_000_000_000,
	}

	r := buildTransactionInfo(tx, result, 1, 3000, false).Receipt
	checks := []struct {
		name string
		got  int64
		want int64
	}{
		{"OwnerBalance", r.GetOwnerBalance(), 5_000_000},
		{"OwnerFreeNetLeft", r.GetOwnerFreeNetLeft(), 400},
		{"OwnerFrozenNetLeft", r.GetOwnerFrozenNetLeft(), 700},
		{"OwnerNetLastConsumeTime", r.GetOwnerNetLastConsumeTime(), 111},
		{"OwnerFreeNetLastConsumeTime", r.GetOwnerFreeNetLastConsumeTime(), 222},
		{"OwnerFrozenForNet", r.GetOwnerFrozenForNet(), 1_000_000},
		{"OwnerFrozenForEnergy", r.GetOwnerFrozenForEnergy(), 2_000_000},
		{"OriginEnergyWindow", r.GetOriginEnergyWindow(), 28_800},
		{"CallerEnergyWindow", r.GetCallerEnergyWindow(), 14_400},
		{"CallerEnergyLimit", r.GetCallerEnergyLimit(), 3_300},
		{"OriginEnergyLimit", r.GetOriginEnergyLimit(), 17_227_485},
		{"OriginFrozenForEnergy", r.GetOriginFrozenForEnergy(), 62_826_000_000},
		{"CallerEnergyUsagePre", r.GetCallerEnergyUsagePre(), 1_234},
		{"OriginEnergyUsagePre", r.GetOriginEnergyUsagePre(), 17_225_691},
		{"CallerEnergyLastConsumeTime", r.GetCallerEnergyLastConsumeTime(), 551_787_654},
		{"OriginEnergyLastConsumeTime", r.GetOriginEnergyLastConsumeTime(), 551_787_600},
		{"TotalEnergyWeight", r.GetTotalEnergyWeight(), 328_216_199},
		{"TotalEnergyCurrentLimit", r.GetTotalEnergyCurrentLimit(), 90_000_000_000},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("receipt.%s = %d, want %d", c.name, c.got, c.want)
		}
	}
}

func TestBuildTransactionInfo_NonVMReceiptShapeMatchesJavaTron(t *testing.T) {
	tx := makeTestTransferTx(1, 2, 100)
	result := &actuator.Result{
		ContractRet: int32(corepb.Transaction_Result_SUCCESS),
	}

	info := buildTransactionInfo(tx, result, 1, 3000, false)
	if got := info.Receipt.Result; got != corepb.Transaction_Result_DEFAULT {
		t.Fatalf("receipt result: got %s, want DEFAULT", got)
	}
	if len(info.ContractResult) != 1 {
		t.Fatalf("contractResult entries: got %d, want 1", len(info.ContractResult))
	}
	if len(info.ContractResult[0]) != 0 {
		t.Fatalf("contractResult[0] length: got %d, want 0", len(info.ContractResult[0]))
	}
}

func TestBuildTransactionInfo_VMReceiptAndLogShapeMatchesJavaTron(t *testing.T) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, []byte{0x12, 0x34})
	result := &actuator.Result{
		ContractRet:           int32(corepb.Transaction_Result_SUCCESS),
		ContractResultPresent: true,
		ContractResult:        []byte{0xab},
		ContractAddress:       contractAddr.Bytes(),
		Logs: []vm.Log{{
			Address: contractAddr,
			Data:    []byte{0xcd},
			Topics:  [][]byte{{0x01}},
		}},
		InternalTransactions: []*corepb.InternalTransaction{{
			Hash:              []byte{0x01},
			CallerAddress:     testProcessorAddr(1).Bytes(),
			TransferToAddress: contractAddr.Bytes(),
			CallValueInfo: []*corepb.InternalTransaction_CallValueInfo{{
				CallValue: 7,
			}},
			Note: []byte("call"),
		}},
	}

	info := buildTransactionInfo(tx, result, 1, 3000, false)
	if got := info.Receipt.Result; got != corepb.Transaction_Result_SUCCESS {
		t.Fatalf("receipt result: got %s, want SUCCESS", got)
	}
	if len(info.ContractResult) != 1 || string(info.ContractResult[0]) != string([]byte{0xab}) {
		t.Fatalf("contractResult: got %x, want ab", info.ContractResult)
	}
	if string(info.ContractAddress) != string(contractAddr.Bytes()) {
		t.Fatalf("contract_address: got %x, want %x", info.ContractAddress, contractAddr.Bytes())
	}
	if len(info.Log) != 1 {
		t.Fatalf("logs: got %d, want 1", len(info.Log))
	}
	wantLogAddress := contractAddr.Bytes()[1:]
	if string(info.Log[0].Address) != string(wantLogAddress) {
		t.Fatalf("log address: got %x, want %x", info.Log[0].Address, wantLogAddress)
	}
	if len(info.Log[0].Topics) != 1 || string(info.Log[0].Topics[0]) != string([]byte{0x01}) {
		t.Fatalf("log topics: got %x, want 01", info.Log[0].Topics)
	}
	if len(info.InternalTransactions) != 1 {
		t.Fatalf("internal_transactions: got %d, want 1", len(info.InternalTransactions))
	}
	if string(info.InternalTransactions[0].Note) != "call" {
		t.Fatalf("internal transaction note: got %q, want call", info.InternalTransactions[0].Note)
	}
}

func TestTransactionInfoSlotReuseClearsVariableFields(t *testing.T) {
	contractAddr := testProcessorAddr(2)
	tx := makeTestTriggerTx(1, contractAddr, nil)
	internalA := &corepb.InternalTransaction{Note: []byte("a")}
	internalB := &corepb.InternalTransaction{Note: []byte("b")}
	slot := new(transactionInfoSlot)

	first := &actuator.Result{
		ContractRet: int32(corepb.Transaction_Result_SUCCESS),
		Logs: []vm.Log{
			{Address: contractAddr, Topics: [][]byte{{0x01}, {0x02}}, Data: []byte{0xa1}},
			{Address: contractAddr, Topics: [][]byte{{0x03}}, Data: []byte{0xa2}},
		},
		InternalTransactions: []*corepb.InternalTransaction{internalA, internalB},
	}
	info := slot.build(tx, first, 1, 3000, false)
	if len(info.Log) != 2 || len(info.InternalTransactions) != 2 {
		t.Fatalf("first build shape: logs=%d internal=%d", len(info.Log), len(info.InternalTransactions))
	}

	info = slot.build(tx, &actuator.Result{ContractRet: int32(corepb.Transaction_Result_SUCCESS)}, 2, 6000, false)
	if info.Log != nil || info.InternalTransactions != nil {
		t.Fatalf("empty reuse retained variable fields: logs=%v internal=%v", info.Log, info.InternalTransactions)
	}

	nonMainnet := tcommon.Address{0x42, 0x11}
	third := &actuator.Result{
		ContractRet: int32(corepb.Transaction_Result_SUCCESS),
		Logs: []vm.Log{{
			Address: nonMainnet,
			Data:    []byte{0xb1},
		}},
		InternalTransactions: []*corepb.InternalTransaction{internalB},
	}
	info = slot.build(tx, third, 3, 9000, false)
	if len(info.Log) != 1 || len(info.Log[0].Topics) != 0 || len(info.Log[0].Address) != tcommon.AddressLength {
		t.Fatalf("third build log shape: %+v", info.Log)
	}
	if !bytes.Equal(info.Log[0].Address, nonMainnet[:]) {
		t.Fatalf("non-mainnet log address = %x, want %x", info.Log[0].Address, nonMainnet)
	}
	if len(info.InternalTransactions) != 1 || info.InternalTransactions[0] != internalB {
		t.Fatalf("third build internal transactions = %+v", info.InternalTransactions)
	}
	if cap(info.Log) != len(info.Log) || cap(info.InternalTransactions) != len(info.InternalTransactions) {
		t.Fatal("receipt repeated fields expose spare reusable capacity")
	}
}

func TestTransactionInfoLogSlotsDoNotAlias(t *testing.T) {
	tx := makeTestTriggerTx(1, testProcessorAddr(2), nil)
	results := [2]*actuator.Result{
		{ContractRet: int32(corepb.Transaction_Result_SUCCESS), Logs: []vm.Log{{Address: testProcessorAddr(2), Topics: [][]byte{{0x01}}}}},
		{ContractRet: int32(corepb.Transaction_Result_SUCCESS), Logs: []vm.Log{{Address: testProcessorAddr(3), Topics: [][]byte{{0x02}}}}},
	}
	slots := make([]transactionInfoSlot, 2)
	first := slots[0].build(tx, results[0], 1, 3000, false)
	second := slots[1].build(tx, results[1], 1, 3000, false)
	secondAddress := append([]byte(nil), second.Log[0].Address...)
	first.Log[0].Address[0] ^= 0xff
	if !bytes.Equal(second.Log[0].Address, secondAddress) {
		t.Fatal("receipt log address buffers alias across transaction slots")
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
