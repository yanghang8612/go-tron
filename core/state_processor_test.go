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
	"google.golang.org/protobuf/proto"
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

func TestBuildTransactionInfoFromOpcodeLogTopics(t *testing.T) {
	statedb := newTestState(t)
	caller := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x80)
	statedb.GetOrCreateAccount(caller)
	// Push topic 4 through topic 1, followed by zero data size and offset, so
	// LOG4 pops the topics back in canonical topic 1 through topic 4 order.
	statedb.SetCode(contractAddr, []byte{
		0x60, 0x04,
		0x60, 0x03,
		0x60, 0x02,
		0x60, 0x01,
		0x60, 0x00,
		0x60, 0x00,
		0xa4,
		0x00,
	})

	slot := new(transactionInfoSlot)
	slot.executionLogArena.Reset()
	tvm := vm.NewTVM(statedb, nil, caller, 1, 1000, tcommon.Address{}, 1, vm.TVMConfig{})
	tvm.SetExecutionLogArena(&slot.executionLogArena)
	_, _, err := tvm.Call(caller, contractAddr, nil, 1_000_000, 0)
	if err != nil {
		t.Fatalf("execute LOG4: %v", err)
	}
	if len(tvm.Logs) != 1 {
		t.Fatalf("opcode logs = %d, want 1", len(tvm.Logs))
	}
	result := &actuator.Result{
		ContractRet: int32(corepb.Transaction_Result_SUCCESS),
		Logs:        tvm.Logs,
	}
	vm.ReleaseTVM(tvm)

	tx := makeTestTriggerTx(1, contractAddr, nil)
	info := slot.build(tx, result, 1, 3000, false)
	// The block path releases the VM's Log structs immediately after copying
	// their slice headers into TransactionInfo. Receipt topics must retain the
	// shared immutable payload without depending on that backing array.
	vm.ReleaseExecutionLogs(result.Logs)
	result.Logs = nil
	if len(info.Log) != 1 {
		t.Fatalf("receipt logs = %d, want 1", len(info.Log))
	}
	if len(info.Log[0].Topics) != 4 {
		t.Fatalf("receipt topics = %d, want 4", len(info.Log[0].Topics))
	}
	for i, topic := range info.Log[0].Topics {
		if len(topic) != 32 || topic[31] != byte(i+1) {
			t.Fatalf("topic %d = %x, want 32-byte value %d", i, topic, i+1)
		}
	}
}

func newTestState(t testing.TB) *state.StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	sdb := state.NewDatabase(diskdb)
	statedb, err := state.New(tcommon.Hash(ethtypes.EmptyRootHash), sdb)
	if err != nil {
		t.Fatal(err)
	}
	return statedb
}

func TestRepairMainnetCreateTransferFailureOvercharge(t *testing.T) {
	statedb := newTestState(t)
	statedb.CreateAccount(mainnetCreateTransferFailurePayer, corepb.AccountType_Normal)
	statedb.AddBalance(mainnetCreateTransferFailurePayer, mainnetCreateTransferFailureBadBalance)
	snapshot := statedb.Snapshot()

	if repaired := repairMainnetCreateTransferFailureOvercharge(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
	); !repaired {
		t.Fatal("legacy bad balance was not repaired")
	}
	if got := statedb.GetBalance(mainnetCreateTransferFailurePayer); got != mainnetCreateTransferFailureCanonicalBalance {
		t.Fatalf("repaired balance = %d, want %d", got, mainnetCreateTransferFailureCanonicalBalance)
	}
	statedb.RevertToSnapshot(snapshot)
	if got := statedb.GetBalance(mainnetCreateTransferFailurePayer); got != mainnetCreateTransferFailureBadBalance {
		t.Fatalf("balance after block snapshot rollback = %d, want %d", got, mainnetCreateTransferFailureBadBalance)
	}
	if repaired := repairMainnetCreateTransferFailureOvercharge(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
	); !repaired {
		t.Fatal("legacy bad balance was not repaired after block retry")
	}
	if repaired := repairMainnetCreateTransferFailureOvercharge(
		statedb,
		mainnetCreateTransferFailureRepairBlock,
		mainnetCreateTransferFailureRepairBlockID,
	); repaired {
		t.Fatal("canonical balance must not be repaired twice")
	}

	badHashState := newTestState(t)
	badHashState.CreateAccount(mainnetCreateTransferFailurePayer, corepb.AccountType_Normal)
	badHashState.AddBalance(mainnetCreateTransferFailurePayer, mainnetCreateTransferFailureBadBalance)
	if repaired := repairMainnetCreateTransferFailureOvercharge(
		badHashState,
		mainnetCreateTransferFailureRepairBlock,
		tcommon.Hash{0xff},
	); repaired {
		t.Fatal("non-canonical block hash activated the repair")
	}
	if got := badHashState.GetBalance(mainnetCreateTransferFailurePayer); got != mainnetCreateTransferFailureBadBalance {
		t.Fatalf("non-canonical block changed balance to %d", got)
	}
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

func TestProcessBlockParallelTransfersMatchesSerial(t *testing.T) {
	base := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4} {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 10_000_000)
	base.AddBalance(testProcessorAddr(3), 20_000_000)
	base.DynamicProperties().SetLatestBlockHeaderTimestamp(30_000)
	base.DynamicProperties().SetPublicNetUsage(1_000)
	base.DynamicProperties().SetPublicNetTime(0)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 4, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 33_000}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, nil, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	publicNetRebasedBefore := parallelTransferPublicNetRebasedCounter.Snapshot().Count()
	candidatesBefore := parallelTransferCandidatesCounter.Snapshot().Count()
	conflictsBefore := parallelTransferConflictFallbackCounter.Snapshot().Count()
	unavailableBefore := parallelTransferUnavailableFallbackCounter.Snapshot().Count()
	preflightBefore := parallelTransferPreflightFallbackCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; published != 2 {
		t.Fatalf("published transfers = %d, want 2 (candidates=%d conflicts=%d unavailable=%d preflight=%d)",
			published,
			parallelTransferCandidatesCounter.Snapshot().Count()-candidatesBefore,
			parallelTransferConflictFallbackCounter.Snapshot().Count()-conflictsBefore,
			parallelTransferUnavailableFallbackCounter.Snapshot().Count()-unavailableBefore,
			parallelTransferPreflightFallbackCounter.Snapshot().Count()-preflightBefore)
	}
	if conflicts := parallelTransferConflictFallbackCounter.Snapshot().Count() - conflictsBefore; conflicts != 0 {
		t.Fatalf("serial conflict fallbacks = %d, want 0", conflicts)
	}
	if rebased := parallelTransferPublicNetRebasedCounter.Snapshot().Count() - publicNetRebasedBefore; rebased != 1 {
		t.Fatalf("rebased public-net publications = %d, want 1", rebased)
	}
	if len(serialInfos) != len(parallelInfos) {
		t.Fatalf("info count serial=%d parallel=%d", len(serialInfos), len(parallelInfos))
	}
	for i := range serialInfos {
		if !proto.Equal(serialInfos[i], parallelInfos[i]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", i, serialInfos[i], parallelInfos[i])
		}
	}
	for _, id := range []byte{1, 2, 3, 4} {
		address := testProcessorAddr(id)
		if serial, parallel := serialState.GetBalance(address), parallelState.GetBalance(address); serial != parallel {
			t.Fatalf("account %d balance serial=%d parallel=%d", id, serial, parallel)
		}
	}
	if serial, parallel := serialState.DynamicProperties().PublicNetUsage(), parallelState.DynamicProperties().PublicNetUsage(); serial != parallel {
		t.Fatalf("public net usage serial=%d parallel=%d", serial, parallel)
	}
	if serial, parallel := serialState.DynamicProperties().PublicNetTime(), parallelState.DynamicProperties().PublicNetTime(); serial != parallel {
		t.Fatalf("public net time serial=%d parallel=%d", serial, parallel)
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockPublishesVMSenderChainCohort(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner1 := testProcessorAddr(1)
	owner2 := testProcessorAddr(3)
	contract1 := testProcessorAddr(0x80)
	contract2 := testProcessorAddr(0x81)
	for _, owner := range []tcommon.Address{owner1, owner2} {
		base.CreateAccount(owner, corepb.AccountType_Normal)
		base.AddBalance(owner, 100_000_000)
	}
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	for _, contract := range []struct {
		address tcommon.Address
		origin  tcommon.Address
	}{{address: contract1, origin: owner1}, {address: contract2, origin: owner2}} {
		base.CreateAccount(contract.address, corepb.AccountType_Contract)
		base.SetContract(contract.address, &contractpb.SmartContract{
			OriginAddress: contract.origin.Bytes(), ContractAddress: contract.address.Bytes(),
		})
		base.SetCode(contract.address, []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x50, 0x00})
	}
	// A small non-mutating program still consumes VM energy. Interleaving a
	// second sender forces the final owner1 result to cross both a forwarded
	// sender boundary and an independently ordered public-net boundary.
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	transactions := []*types.Transaction{
		makeTestTriggerTx(1, contract1, []byte{0x01}),
		makeTestTriggerTx(3, contract2, []byte{0x02}),
		makeTestTriggerTx(1, contract1, []byte{0x03}),
	}
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for txIndex, tx := range transactions {
		tx.Proto().RawData.FeeLimit = 10_000_000
		tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
		transactionProtos[txIndex] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: transactionProtos,
	})

	run := func(statedb *state.StateDB, db actuator.BufferedKVStore, options processBlockOptions) ([]*corepb.TransactionInfo, *contractpb.BlockBalanceTrace, map[tcommon.Address]int64, error) {
		statedb.BeginBalanceTrace(int64(block.Number()), block.Hash().Bytes(), block.Timestamp())
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, db, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		trace, finalBalances := statedb.FinishBalanceTrace()
		return infos, trace, finalBalances, processErr
	}
	serialInfos, serialTrace, serialFinalBalances, err := run(serialState, ethrawdb.NewMemoryDatabase(), processBlockOptions{captureBalanceTrace: true})
	if err != nil {
		t.Fatalf("serial VM process: %v", err)
	}
	blocksBefore := parallelVMBlocksCounter.Snapshot().Count()
	preexecutedBefore := parallelVMPreexecutedCounter.Snapshot().Count()
	candidatesBefore := parallelVMCandidatesCounter.Snapshot().Count()
	publishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	chainCandidatesBefore := parallelVMChainCandidatesCounter.Snapshot().Count()
	chainPublishedBefore := parallelVMChainPublishedCounter.Snapshot().Count()
	energyPublishedBefore := parallelVMBlockEnergyPublishedCounter.Snapshot().Count()
	publicNetReservationsBefore := parallelVMPublicNetReservationsCounter.Snapshot().Count()
	publicNetPublishedBefore := parallelVMPublicNetPublishedCounter.Snapshot().Count()
	publicNetRebasedBefore := parallelVMPublicNetRebasedCounter.Snapshot().Count()
	projectionMatchesBefore := discardShadowVMPublicNetProjectionMatchesCounter.Snapshot().Count()
	projectionMismatchesBefore := discardShadowVMPublicNetProjectionMismatchCounter.Snapshot().Count()
	projectionMissingBefore := discardShadowVMPublicNetProjectionMissingCounter.Snapshot().Count()
	energyMatchesBefore := discardShadowVMBlockEnergyMatchesCounter.Snapshot().Count()
	energyMismatchesBefore := discardShadowVMBlockEnergyMismatchesCounter.Snapshot().Count()
	energyMissingBefore := discardShadowVMBlockEnergyMissingCounter.Snapshot().Count()
	errorsBefore := parallelVMErrorsCounter.Snapshot().Count()
	fallbacksBefore := parallelVMUnavailableFallbackCounter.Snapshot().Count() +
		parallelVMConflictFallbackCounter.Snapshot().Count() +
		parallelVMPreflightFallbackCounter.Snapshot().Count() +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	parallelInfos, parallelTrace, parallelFinalBalances, err := run(parallelState, ethrawdb.NewMemoryDatabase(), processBlockOptions{parallelTransfers: true, captureBalanceTrace: true})
	if err != nil {
		t.Fatalf("parallel VM process: %v", err)
	}
	if blocks := parallelVMBlocksCounter.Snapshot().Count() - blocksBefore; blocks != 1 {
		t.Fatalf("parallel VM blocks = %d, want 1", blocks)
	}
	if preexecuted := parallelVMPreexecutedCounter.Snapshot().Count() - preexecutedBefore; preexecuted != 3 {
		t.Fatalf("parallel VM preexecuted = %d, want 3", preexecuted)
	}
	if candidates := parallelVMCandidatesCounter.Snapshot().Count() - candidatesBefore; candidates != 3 {
		t.Fatalf("parallel VM candidates = %d, want 3 (published=%d unavailable=%d conflict=%d preflight=%d public=%d energy=%d)",
			candidates,
			parallelVMPublishedCounter.Snapshot().Count()-publishedBefore,
			parallelVMUnavailableFallbackCounter.Snapshot().Count(),
			parallelVMConflictFallbackCounter.Snapshot().Count(),
			parallelVMPreflightFallbackCounter.Snapshot().Count(),
			parallelVMPublicNetFallbackCounter.Snapshot().Count(),
			parallelVMBlockEnergyFallbackCounter.Snapshot().Count())
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - publishedBefore; published != 3 {
		t.Fatalf("parallel VM published = %d, want 3", published)
	}
	if candidates := parallelVMChainCandidatesCounter.Snapshot().Count() - chainCandidatesBefore; candidates != 1 {
		t.Fatalf("parallel VM sender-chain candidates = %d, want 1", candidates)
	}
	if published := parallelVMChainPublishedCounter.Snapshot().Count() - chainPublishedBefore; published != 1 {
		t.Fatalf("parallel VM sender-chain published = %d, want 1", published)
	}
	if published := parallelVMBlockEnergyPublishedCounter.Snapshot().Count() - energyPublishedBefore; published != 3 {
		t.Fatalf("parallel VM block-energy publications = %d, want 3", published)
	}
	if reservations := parallelVMPublicNetReservationsCounter.Snapshot().Count() - publicNetReservationsBefore; reservations != 3 {
		t.Fatalf("parallel VM public-net reservations = %d, want 3", reservations)
	}
	if published := parallelVMPublicNetPublishedCounter.Snapshot().Count() - publicNetPublishedBefore; published != 3 {
		t.Fatalf("parallel VM public-net publications = %d, want 3", published)
	}
	if rebased := parallelVMPublicNetRebasedCounter.Snapshot().Count() - publicNetRebasedBefore; rebased != 2 {
		t.Fatalf("parallel VM public-net rebases = %d, want 2", rebased)
	}
	if matches := discardShadowVMPublicNetProjectionMatchesCounter.Snapshot().Count() - projectionMatchesBefore; matches != 3 {
		t.Fatalf("parallel VM projected public-net matches = %d, want 3", matches)
	}
	if mismatches := discardShadowVMPublicNetProjectionMismatchCounter.Snapshot().Count() - projectionMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM projected public-net mismatches = %d, want 0", mismatches)
	}
	if missing := discardShadowVMPublicNetProjectionMissingCounter.Snapshot().Count() - projectionMissingBefore; missing != 0 {
		t.Fatalf("parallel VM projected public-net missing = %d, want 0", missing)
	}
	if matches := discardShadowVMBlockEnergyMatchesCounter.Snapshot().Count() - energyMatchesBefore; matches != 3 {
		t.Fatalf("parallel VM projected block-energy matches = %d, want 3", matches)
	}
	if mismatches := discardShadowVMBlockEnergyMismatchesCounter.Snapshot().Count() - energyMismatchesBefore; mismatches != 0 {
		t.Fatalf("parallel VM projected block-energy mismatches = %d, want 0", mismatches)
	}
	if missing := discardShadowVMBlockEnergyMissingCounter.Snapshot().Count() - energyMissingBefore; missing != 0 {
		t.Fatalf("parallel VM projected block-energy missing = %d, want 0", missing)
	}
	if failures := parallelVMErrorsCounter.Snapshot().Count() - errorsBefore; failures != 0 {
		t.Fatalf("parallel VM errors = %d, want 0", failures)
	}
	fallbacksAfter := parallelVMUnavailableFallbackCounter.Snapshot().Count() +
		parallelVMConflictFallbackCounter.Snapshot().Count() +
		parallelVMPreflightFallbackCounter.Snapshot().Count() +
		parallelVMPublicNetFallbackCounter.Snapshot().Count() +
		parallelVMBlockEnergyFallbackCounter.Snapshot().Count()
	if fallbacks := fallbacksAfter - fallbacksBefore; fallbacks != 0 {
		t.Fatalf("parallel VM fallbacks = %d, want 0", fallbacks)
	}

	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	if !proto.Equal(serialTrace, parallelTrace) {
		t.Fatalf("block balance trace mismatch\nserial=%v\nparallel=%v", serialTrace, parallelTrace)
	}
	if len(serialFinalBalances) != len(parallelFinalBalances) {
		t.Fatalf("final balance count serial=%d parallel=%d", len(serialFinalBalances), len(parallelFinalBalances))
	}
	for address, serialBalance := range serialFinalBalances {
		if parallelBalance, ok := parallelFinalBalances[address]; !ok || parallelBalance != serialBalance {
			t.Fatalf("final balance %s serial=%d parallel=%d present=%t", address.Hex(), serialBalance, parallelBalance, ok)
		}
	}
	for _, address := range []tcommon.Address{owner1, owner2, contract1, contract2} {
		if serialBalance, parallelBalance := serialState.GetBalance(address), parallelState.GetBalance(address); serialBalance != parallelBalance {
			t.Fatalf("balance %s serial=%d parallel=%d", address.Hex(), serialBalance, parallelBalance)
		}
	}
	for _, property := range []struct {
		name     string
		serial   int64
		parallel int64
	}{
		{name: "public_net_usage", serial: serialState.DynamicProperties().PublicNetUsage(), parallel: parallelState.DynamicProperties().PublicNetUsage()},
		{name: "public_net_time", serial: serialState.DynamicProperties().PublicNetTime(), parallel: parallelState.DynamicProperties().PublicNetTime()},
		{name: "block_energy_usage", serial: serialState.DynamicProperties().BlockEnergyUsage(), parallel: parallelState.DynamicProperties().BlockEnergyUsage()},
	} {
		if property.serial != property.parallel {
			t.Fatalf("%s serial=%d parallel=%d", property.name, property.serial, property.parallel)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("VM state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockPublishesBoundaryReadyAsyncVMRetry(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	funder := testProcessorAddr(3)
	churnOwner := testProcessorAddr(5)
	churnRecipient := testProcessorAddr(6)
	contractAddr := testProcessorAddr(0x80)
	for _, address := range []tcommon.Address{owner, funder, churnOwner, churnRecipient, params.BlackholeAddress} {
		base.CreateAccount(address, corepb.AccountType_Normal)
	}
	base.AddBalance(owner, 100_000_000)
	base.AddBalance(funder, 100_000_000)
	base.AddBalance(churnOwner, 100_000_000)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	base.SetCode(contractAddr, []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x50, 0x00})
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())

	transactions := []*types.Transaction{
		makeTestTriggerTx(1, contractAddr, []byte{0x01}),
		makeTestTransferTx(3, 1, 2_000_000),
		makeTestTriggerTx(1, contractAddr, []byte{0x02}),
	}
	// The conflict at tx 2 launches a two-result sender suffix. Unrelated
	// canonical work gives its descendant a deterministic opportunity to finish
	// before its own boundary without introducing any wait in production code.
	for range 256 {
		transactions = append(transactions, makeTestTransferTx(5, 6, 1))
	}
	transactions = append(transactions, makeTestTriggerTx(1, contractAddr, []byte{0x03}))
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for txIndex, tx := range transactions {
		if tx.ContractType() == corepb.Transaction_Contract_TriggerSmartContract {
			tx.Proto().RawData.FeeLimit = 10_000_000
			tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
		}
		transactionProtos[txIndex] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderRetryPublishInterval + vmSenderRetryCoScheduleOffset), Timestamp: 33_000,
		}},
		Transactions: transactionProtos,
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, ethrawdb.NewMemoryDatabase(), nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial VM retry process: %v", err)
	}
	blocksBefore := parallelVMAsyncRetryBlocksCounter.Snapshot().Count()
	attemptsBefore := parallelVMAsyncRetryAttemptsCounter.Snapshot().Count()
	jobsBefore := parallelVMAsyncRetryJobsCounter.Snapshot().Count()
	executedBefore := parallelVMAsyncRetryExecutedCounter.Snapshot().Count()
	readyBefore := parallelVMAsyncRetryReadyCounter.Snapshot().Count()
	lateBefore := parallelVMAsyncRetryLateCounter.Snapshot().Count()
	staleBefore := parallelVMAsyncRetryStaleCounter.Snapshot().Count()
	candidatesBefore := parallelVMAsyncRetryCandidatesCounter.Snapshot().Count()
	validatedBefore := parallelVMAsyncRetryValidatedCounter.Snapshot().Count()
	recoveredBefore := parallelVMAsyncRetryRecoveredCounter.Snapshot().Count()
	errorsBefore := parallelVMAsyncRetryErrorsCounter.Snapshot().Count()
	prewarmedBefore := parallelVMAsyncRetryPrewarmedCounter.Snapshot().Count()
	capacityBefore := parallelVMAsyncRetryCapacityCounter.Snapshot().Count()
	sharedJobsBefore := parallelVMAsyncRetrySharedJobsCounter.Snapshot().Count()
	sharedErrorsBefore := parallelVMAsyncRetrySharedErrorsCounter.Snapshot().Count()
	sharedValueBlocksBefore := versionedShadowSharedValueBlocksCounter.Snapshot().Count()
	sharedValueHitsBefore := versionedShadowSharedValueHitsCounter.Snapshot().Count()
	publishBlocksBefore := parallelVMAsyncRetryPublishBlocksCounter.Snapshot().Count()
	publishCandidatesBefore := parallelVMAsyncRetryPublishCandidatesCounter.Snapshot().Count()
	retryPublishedBefore := parallelVMAsyncRetryPublishedCounter.Snapshot().Count()
	publishErrorsBefore := parallelVMAsyncRetryPublishErrorsCounter.Snapshot().Count()
	publishFallbacksBefore := parallelVMAsyncRetryPublishEnergyFallbackCounter.Snapshot().Count() +
		parallelVMAsyncRetryPublishNetFallbackCounter.Snapshot().Count() +
		parallelVMAsyncRetryPublishPreflightCounter.Snapshot().Count()
	publishWriteOKBefore := parallelVMAsyncRetryPublishWriteOKCounter.Snapshot().Count()
	publishWriteMismatchBefore := parallelVMAsyncRetryPublishWriteMismatchCounter.Snapshot().Count()
	mismatchesBefore := parallelVMAsyncRetryInfoMismatchCounter.Snapshot().Count() +
		parallelVMAsyncRetryWriteMismatchCounter.Snapshot().Count() +
		parallelVMAsyncRetryBalanceMismatchCounter.Snapshot().Count()
	vmPublishedBefore := parallelVMPublishedCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel VM retry process: %v", err)
	}
	if blocks := parallelVMAsyncRetryBlocksCounter.Snapshot().Count() - blocksBefore; blocks != 1 {
		t.Fatalf("async VM retry blocks = %d, want 1", blocks)
	}
	if attempts := parallelVMAsyncRetryAttemptsCounter.Snapshot().Count() - attemptsBefore; attempts != 1 {
		t.Fatalf("async VM retry attempts = %d, want 1", attempts)
	}
	if jobs := parallelVMAsyncRetryJobsCounter.Snapshot().Count() - jobsBefore; jobs != 1 {
		t.Fatalf("async VM retry jobs = %d, want 1", jobs)
	}
	executed := parallelVMAsyncRetryExecutedCounter.Snapshot().Count() - executedBefore
	if executed != 2 {
		t.Fatalf("async VM retry executions = %d, want 2", executed)
	}
	classified := parallelVMAsyncRetryReadyCounter.Snapshot().Count() - readyBefore +
		parallelVMAsyncRetryLateCounter.Snapshot().Count() - lateBefore +
		parallelVMAsyncRetryStaleCounter.Snapshot().Count() - staleBefore
	if classified != executed {
		t.Fatalf("async VM retry classifications = %d, executions = %d", classified, executed)
	}
	candidates := parallelVMAsyncRetryCandidatesCounter.Snapshot().Count() - candidatesBefore
	validated := parallelVMAsyncRetryValidatedCounter.Snapshot().Count() - validatedBefore
	recovered := parallelVMAsyncRetryRecoveredCounter.Snapshot().Count() - recoveredBefore
	if candidates != 1 || validated != 0 || recovered != 0 {
		t.Fatalf("async VM retry candidates=%d validated=%d recovered=%d, want 1/0/0 for a published result", candidates, validated, recovered)
	}
	if failures := parallelVMAsyncRetryErrorsCounter.Snapshot().Count() - errorsBefore; failures != 0 {
		t.Fatalf("async VM retry errors = %d, want 0", failures)
	}
	if prewarmed := parallelVMAsyncRetryPrewarmedCounter.Snapshot().Count() - prewarmedBefore; prewarmed != 1 {
		t.Fatalf("async VM retry prewarmed runners = %d, want 1", prewarmed)
	}
	if capacity := parallelVMAsyncRetryCapacityCounter.Snapshot().Count() - capacityBefore; capacity != 1 {
		t.Fatalf("async VM retry runner capacity = %d, want 1", capacity)
	}
	if jobs := parallelVMAsyncRetrySharedJobsCounter.Snapshot().Count() - sharedJobsBefore; jobs != 1 {
		t.Fatalf("async VM retry shared-state jobs = %d, want 1", jobs)
	}
	if failures := parallelVMAsyncRetrySharedErrorsCounter.Snapshot().Count() - sharedErrorsBefore; failures != 0 {
		t.Fatalf("async VM retry shared-state errors = %d, want 0", failures)
	}
	if blocks := versionedShadowSharedValueBlocksCounter.Snapshot().Count() - sharedValueBlocksBefore; blocks != 1 {
		t.Fatalf("async VM retry shared-version blocks = %d, want 1", blocks)
	}
	if hits := versionedShadowSharedValueHitsCounter.Snapshot().Count() - sharedValueHitsBefore; hits <= 0 {
		t.Fatalf("async VM retry shared-version hits = %d, want > 0", hits)
	}
	mismatchesAfter := parallelVMAsyncRetryInfoMismatchCounter.Snapshot().Count() +
		parallelVMAsyncRetryWriteMismatchCounter.Snapshot().Count() +
		parallelVMAsyncRetryBalanceMismatchCounter.Snapshot().Count()
	if mismatches := mismatchesAfter - mismatchesBefore; mismatches != 0 {
		t.Fatalf("VM retry mismatches = %d, want 0", mismatches)
	}
	if blocks := parallelVMAsyncRetryPublishBlocksCounter.Snapshot().Count() - publishBlocksBefore; blocks != 1 {
		t.Fatalf("async VM retry publish blocks = %d, want 1", blocks)
	}
	if candidates := parallelVMAsyncRetryPublishCandidatesCounter.Snapshot().Count() - publishCandidatesBefore; candidates != 1 {
		t.Fatalf("async VM retry publish candidates = %d, want 1", candidates)
	}
	if published := parallelVMAsyncRetryPublishedCounter.Snapshot().Count() - retryPublishedBefore; published != 1 {
		t.Fatalf("async VM retry publications = %d, want 1", published)
	}
	if failures := parallelVMAsyncRetryPublishErrorsCounter.Snapshot().Count() - publishErrorsBefore; failures != 0 {
		t.Fatalf("async VM retry publish errors = %d, want 0", failures)
	}
	publishFallbacksAfter := parallelVMAsyncRetryPublishEnergyFallbackCounter.Snapshot().Count() +
		parallelVMAsyncRetryPublishNetFallbackCounter.Snapshot().Count() +
		parallelVMAsyncRetryPublishPreflightCounter.Snapshot().Count()
	if fallbacks := publishFallbacksAfter - publishFallbacksBefore; fallbacks != 0 {
		t.Fatalf("async VM retry publish fallbacks = %d, want 0", fallbacks)
	}
	if matches := parallelVMAsyncRetryPublishWriteOKCounter.Snapshot().Count() - publishWriteOKBefore; matches != 1 {
		t.Fatalf("async VM retry publish write-set matches = %d, want 1", matches)
	}
	if mismatches := parallelVMAsyncRetryPublishWriteMismatchCounter.Snapshot().Count() - publishWriteMismatchBefore; mismatches != 0 {
		t.Fatalf("async VM retry publish write-set mismatches = %d, want 0", mismatches)
	}
	if published := parallelVMPublishedCounter.Snapshot().Count() - vmPublishedBefore; published != 1 {
		t.Fatalf("VM publications = %d, want 1 async retry descendant", published)
	}
	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	for _, property := range []struct {
		name     string
		serial   int64
		parallel int64
	}{
		{name: "public_net_usage", serial: serialState.DynamicProperties().PublicNetUsage(), parallel: parallelState.DynamicProperties().PublicNetUsage()},
		{name: "public_net_time", serial: serialState.DynamicProperties().PublicNetTime(), parallel: parallelState.DynamicProperties().PublicNetTime()},
		{name: "block_energy_usage", serial: serialState.DynamicProperties().BlockEnergyUsage(), parallel: parallelState.DynamicProperties().BlockEnergyUsage()},
	} {
		if property.serial != property.parallel {
			t.Fatalf("%s serial=%d parallel=%d", property.name, property.serial, property.parallel)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("VM retry state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockSamplesSenderChainForwarding(t *testing.T) {
	statedb := newTestState(t)
	for _, id := range []byte{1, 2, 3} {
		statedb.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(1, 3, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryInterval), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	forwardedBefore := discardShadowSenderChainForwardedCounter.Snapshot().Count()
	validatedBefore := discardShadowSenderChainForwardedOKCounter.Snapshot().Count()
	mismatchBefore := discardShadowSenderChainInfoMismatchesCounter.Snapshot().Count() +
		discardShadowSenderChainWriteMismatchesCounter.Snapshot().Count() +
		discardShadowSenderChainBalanceMismatchesCounter.Snapshot().Count()
	if _, _, err := processBlockWithOptions(
		statedb, statedb.DynamicProperties(), block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
		processBlockOptions{parallelTransfers: true},
	); err != nil {
		t.Fatalf("process sampled sender chain: %v", err)
	}
	if forwarded := discardShadowSenderChainForwardedCounter.Snapshot().Count() - forwardedBefore; forwarded != 1 {
		t.Fatalf("forwarded sender-chain results = %d, want 1", forwarded)
	}
	if validated := discardShadowSenderChainForwardedOKCounter.Snapshot().Count() - validatedBefore; validated != 1 {
		t.Fatalf("validated forwarded results = %d, want 1", validated)
	}
	mismatchAfter := discardShadowSenderChainInfoMismatchesCounter.Snapshot().Count() +
		discardShadowSenderChainWriteMismatchesCounter.Snapshot().Count() +
		discardShadowSenderChainBalanceMismatchesCounter.Snapshot().Count()
	if mismatches := mismatchAfter - mismatchBefore; mismatches != 0 {
		t.Fatalf("sender-chain mismatches = %d, want 0", mismatches)
	}
	if balance := statedb.GetBalance(testProcessorAddr(1)); balance != 7_000_000 {
		t.Fatalf("owner balance = %d, want 7000000", balance)
	}
}

func TestProcessBlockSamplesSenderRetryIncarnation(t *testing.T) {
	statedb := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4, 5} {
		statedb.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	statedb.AddBalance(testProcessorAddr(3), 10_000_000)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 1, 2_000_000),
		makeTestTransferTx(1, 4, 3_000_000),
		makeTestTransferTx(1, 5, 1_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryInterval), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{
			transactions[0].Proto(), transactions[1].Proto(), transactions[2].Proto(), transactions[3].Proto(),
		},
	})
	attemptsBefore := discardShadowRetryAttemptsCounter.Snapshot().Count()
	executedBefore := discardShadowRetryExecutedCounter.Snapshot().Count()
	candidatesBefore := discardShadowRetryCandidatesCounter.Snapshot().Count()
	recoveredBefore := discardShadowRetryRecoveredCounter.Snapshot().Count()
	validatedBefore := discardShadowRetryValidatedCounter.Snapshot().Count()
	prefixRefreshBefore := discardShadowRetryPrefixRefreshCounter.Snapshot().Count()
	prefixReuseBefore := discardShadowRetryPrefixReuseCounter.Snapshot().Count()
	prefixAdvanceBefore := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count()
	asyncCandidatesBefore := discardShadowRetryAsyncCandidatesCounter.Snapshot().Count()
	asyncReadyBefore := discardShadowRetryAsyncReadyCounter.Snapshot().Count()
	asyncLateBefore := discardShadowRetryAsyncLateCounter.Snapshot().Count()
	asyncUnknownBefore := discardShadowRetryAsyncUnknownCounter.Snapshot().Count()
	mismatchBefore := discardShadowRetryInfoMismatchCounter.Snapshot().Count() +
		discardShadowRetryWriteMismatchCounter.Snapshot().Count() +
		discardShadowRetryBalanceMismatchCounter.Snapshot().Count() +
		discardShadowRetryErrorsCounter.Snapshot().Count()
	if _, _, err := processBlockWithOptions(
		statedb, statedb.DynamicProperties(), block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
		processBlockOptions{parallelTransfers: true},
	); err != nil {
		t.Fatalf("process sampled sender retry: %v", err)
	}
	if attempts := discardShadowRetryAttemptsCounter.Snapshot().Count() - attemptsBefore; attempts != 1 {
		t.Fatalf("sender retry attempts = %d, want 1 (executed=%d candidates=%d recovered=%d validated=%d)", attempts,
			discardShadowRetryExecutedCounter.Snapshot().Count()-executedBefore,
			discardShadowRetryCandidatesCounter.Snapshot().Count()-candidatesBefore,
			discardShadowRetryRecoveredCounter.Snapshot().Count()-recoveredBefore,
			discardShadowRetryValidatedCounter.Snapshot().Count()-validatedBefore)
	}
	if executed := discardShadowRetryExecutedCounter.Snapshot().Count() - executedBefore; executed != 2 {
		t.Fatalf("sender retry executions = %d, want 2", executed)
	}
	if candidates := discardShadowRetryCandidatesCounter.Snapshot().Count() - candidatesBefore; candidates != 2 {
		t.Fatalf("sender retry candidates = %d, want 2", candidates)
	}
	if recovered := discardShadowRetryRecoveredCounter.Snapshot().Count() - recoveredBefore; recovered != 2 {
		t.Fatalf("sender retry recovered = %d, want 2", recovered)
	}
	if validated := discardShadowRetryValidatedCounter.Snapshot().Count() - validatedBefore; validated != 2 {
		t.Fatalf("sender retry validated = %d, want 2", validated)
	}
	if refreshes := discardShadowRetryPrefixRefreshCounter.Snapshot().Count() - prefixRefreshBefore; refreshes != 1 {
		t.Fatalf("sender retry prefix refreshes = %d, want 1", refreshes)
	}
	if reuses := discardShadowRetryPrefixReuseCounter.Snapshot().Count() - prefixReuseBefore; reuses != 0 {
		t.Fatalf("sender retry prefix reuses = %d, want 0", reuses)
	}
	if advances := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count() - prefixAdvanceBefore; advances != 0 {
		t.Fatalf("sender retry prefix advances = %d, want 0", advances)
	}
	asyncCandidates := discardShadowRetryAsyncCandidatesCounter.Snapshot().Count() - asyncCandidatesBefore
	asyncClassified := discardShadowRetryAsyncReadyCounter.Snapshot().Count() - asyncReadyBefore +
		discardShadowRetryAsyncLateCounter.Snapshot().Count() - asyncLateBefore +
		discardShadowRetryAsyncUnknownCounter.Snapshot().Count() - asyncUnknownBefore
	if asyncCandidates != 2 || asyncClassified != asyncCandidates {
		t.Fatalf("sender retry async projection candidates=%d classified=%d, want 2", asyncCandidates, asyncClassified)
	}
	mismatchAfter := discardShadowRetryInfoMismatchCounter.Snapshot().Count() +
		discardShadowRetryWriteMismatchCounter.Snapshot().Count() +
		discardShadowRetryBalanceMismatchCounter.Snapshot().Count() +
		discardShadowRetryErrorsCounter.Snapshot().Count()
	if mismatches := mismatchAfter - mismatchBefore; mismatches != 0 {
		t.Fatalf("sender retry mismatches/errors = %d, want 0", mismatches)
	}
	if balance := statedb.GetBalance(testProcessorAddr(1)); balance != 7_000_000 {
		t.Fatalf("owner balance = %d, want 7000000", balance)
	}
}

func TestProcessBlockRunsActualAsyncSenderRetryCanary(t *testing.T) {
	statedb := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4, 5} {
		statedb.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	statedb.AddBalance(testProcessorAddr(1), 10_000_000)
	statedb.AddBalance(testProcessorAddr(3), 10_000_000)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 1, 2_000_000),
		makeTestTransferTx(1, 4, 3_000_000),
		makeTestTransferTx(1, 5, 1_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryFirstOffset), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{
			transactions[0].Proto(), transactions[1].Proto(), transactions[2].Proto(), transactions[3].Proto(),
		},
	})
	blocksBefore := discardShadowRetryActualBlocksCounter.Snapshot().Count()
	prewarmedBefore := discardShadowRetryActualPrewarmedCounter.Snapshot().Count()
	capacityBefore := discardShadowRetryActualRunnerCapacityCounter.Snapshot().Count()
	maxInflightBefore := discardShadowRetryActualMaxInflightCounter.Snapshot().Count()
	deferredBefore := discardShadowRetryActualDeferredCounter.Snapshot().Count()
	supersededBefore := discardShadowRetryActualSupersededCounter.Snapshot().Count()
	queueEnqueuedBefore := discardShadowRetryActualQueueEnqueuedCounter.Snapshot().Count()
	queueDequeuedBefore := discardShadowRetryActualQueueDequeuedCounter.Snapshot().Count()
	queueDroppedBefore := discardShadowRetryActualQueueDroppedCounter.Snapshot().Count()
	publishedBefore := parallelTransferRetryPublishedCounter.Snapshot().Count()
	workerPrefixJobsBefore := discardShadowRetryActualWorkerPrefixJobsCounter.Snapshot().Count()
	workerPrefixAdvancesBefore := discardShadowRetryActualWorkerPrefixAdvanceCounter.Snapshot().Count()
	workerPrefixNanosBefore := discardShadowRetryActualWorkerPrefixNanosCounter.Snapshot().Count()
	workerPrefixErrorsBefore := discardShadowRetryActualWorkerPrefixErrorsCounter.Snapshot().Count()
	sharedStateJobsBefore := discardShadowRetrySharedStateJobsCounter.Snapshot().Count()
	sharedStateCopyNanosBefore := discardShadowRetrySharedStateCopyNanosCounter.Snapshot().Count()
	sharedStateErrorsBefore := discardShadowRetrySharedStateErrorsCounter.Snapshot().Count()
	sharedValueBlocksBefore := versionedShadowSharedValueBlocksCounter.Snapshot().Count()
	sharedValueHitsBefore := versionedShadowSharedValueHitsCounter.Snapshot().Count()
	jobsBefore := discardShadowRetryActualJobsCounter.Snapshot().Count()
	executedBefore := discardShadowRetryActualExecutedCounter.Snapshot().Count()
	readyBefore := discardShadowRetryActualReadyCounter.Snapshot().Count()
	lateBefore := discardShadowRetryActualLateCounter.Snapshot().Count()
	staleBefore := discardShadowRetryActualStaleCounter.Snapshot().Count()
	errorsBefore := discardShadowRetryActualErrorsCounter.Snapshot().Count()
	prefixRefreshBefore := discardShadowRetryPrefixRefreshCounter.Snapshot().Count()
	prefixAdvanceBefore := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count()
	actualPrefixRefreshBefore := discardShadowRetryActualPrefixRefreshCounter.Snapshot().Count()
	actualPrefixAdvanceBefore := discardShadowRetryActualPrefixAdvanceCounter.Snapshot().Count()
	if _, _, err := processBlockWithOptions(
		statedb, statedb.DynamicProperties(), block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
		processBlockOptions{parallelTransfers: true},
	); err != nil {
		t.Fatalf("process actual async sender retry: %v", err)
	}
	if blocks := discardShadowRetryActualBlocksCounter.Snapshot().Count() - blocksBefore; blocks != 1 {
		t.Fatalf("actual async blocks = %d, want 1", blocks)
	}
	if jobs := discardShadowRetryActualJobsCounter.Snapshot().Count() - jobsBefore; jobs != 1 {
		t.Fatalf("actual async jobs = %d, want 1", jobs)
	}
	if prewarmed := discardShadowRetryActualPrewarmedCounter.Snapshot().Count() - prewarmedBefore; prewarmed != 1 {
		t.Fatalf("actual async prewarmed runners = %d, want 1", prewarmed)
	}
	if capacity := discardShadowRetryActualRunnerCapacityCounter.Snapshot().Count() - capacityBefore; capacity != 1 {
		t.Fatalf("actual async runner capacity = %d, want 1", capacity)
	}
	if maxInflight := discardShadowRetryActualMaxInflightCounter.Snapshot().Count() - maxInflightBefore; maxInflight != 1 {
		t.Fatalf("actual async max inflight = %d, want 1", maxInflight)
	}
	if deferred := discardShadowRetryActualDeferredCounter.Snapshot().Count() - deferredBefore; deferred != 0 {
		t.Fatalf("actual async deferred suffix = %d, want 0", deferred)
	}
	if superseded := discardShadowRetryActualSupersededCounter.Snapshot().Count() - supersededBefore; superseded != 0 {
		t.Fatalf("actual async superseded suffix = %d, want 0", superseded)
	}
	if enqueued := discardShadowRetryActualQueueEnqueuedCounter.Snapshot().Count() - queueEnqueuedBefore; enqueued != 1 {
		t.Fatalf("actual async queued requests = %d, want 1", enqueued)
	}
	if dequeued := discardShadowRetryActualQueueDequeuedCounter.Snapshot().Count() - queueDequeuedBefore; dequeued != 1 {
		t.Fatalf("actual async dequeued requests = %d, want 1", dequeued)
	}
	if dropped := discardShadowRetryActualQueueDroppedCounter.Snapshot().Count() - queueDroppedBefore; dropped != 0 {
		t.Fatalf("actual async dropped queued tasks = %d, want 0", dropped)
	}
	if published := parallelTransferRetryPublishedCounter.Snapshot().Count() - publishedBefore; published != 0 {
		t.Fatalf("non-publication async cohort published %d retries", published)
	}
	if jobs := discardShadowRetryActualWorkerPrefixJobsCounter.Snapshot().Count() - workerPrefixJobsBefore; jobs != 0 {
		t.Fatalf("actual async worker prefix jobs = %d, want 0", jobs)
	}
	if advances := discardShadowRetryActualWorkerPrefixAdvanceCounter.Snapshot().Count() - workerPrefixAdvancesBefore; advances != 0 {
		t.Fatalf("actual async worker prefix advances = %d, want 0", advances)
	}
	if nanos := discardShadowRetryActualWorkerPrefixNanosCounter.Snapshot().Count() - workerPrefixNanosBefore; nanos != 0 {
		t.Fatalf("actual async worker prefix nanos = %d, want 0", nanos)
	}
	if errors := discardShadowRetryActualWorkerPrefixErrorsCounter.Snapshot().Count() - workerPrefixErrorsBefore; errors != 0 {
		t.Fatalf("actual async worker prefix errors = %d, want 0", errors)
	}
	if refreshes := discardShadowRetryPrefixRefreshCounter.Snapshot().Count() - prefixRefreshBefore; refreshes != 0 {
		t.Fatalf("actual async prefix refreshes = %d, want 0", refreshes)
	}
	if advances := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count() - prefixAdvanceBefore; advances != 0 {
		t.Fatalf("actual async prefix advances = %d, want 0", advances)
	}
	if refreshes := discardShadowRetryActualPrefixRefreshCounter.Snapshot().Count() - actualPrefixRefreshBefore; refreshes != 0 {
		t.Fatalf("actual-only async prefix refreshes = %d, want 0", refreshes)
	}
	if advances := discardShadowRetryActualPrefixAdvanceCounter.Snapshot().Count() - actualPrefixAdvanceBefore; advances != 0 {
		t.Fatalf("actual-only async prefix advances = %d, want 0", advances)
	}
	if jobs := discardShadowRetrySharedStateJobsCounter.Snapshot().Count() - sharedStateJobsBefore; jobs != 1 {
		t.Fatalf("actual async shared-state jobs = %d, want 1", jobs)
	}
	if nanos := discardShadowRetrySharedStateCopyNanosCounter.Snapshot().Count() - sharedStateCopyNanosBefore; nanos <= 0 {
		t.Fatalf("actual async shared-state copy nanos = %d, want > 0", nanos)
	}
	if errors := discardShadowRetrySharedStateErrorsCounter.Snapshot().Count() - sharedStateErrorsBefore; errors != 0 {
		t.Fatalf("actual async shared-state errors = %d, want 0", errors)
	}
	if blocks := versionedShadowSharedValueBlocksCounter.Snapshot().Count() - sharedValueBlocksBefore; blocks != 1 {
		t.Fatalf("shared version blocks = %d, want 1", blocks)
	}
	if hits := versionedShadowSharedValueHitsCounter.Snapshot().Count() - sharedValueHitsBefore; hits <= 0 {
		t.Fatalf("shared version hits = %d, want > 0", hits)
	}
	executed := discardShadowRetryActualExecutedCounter.Snapshot().Count() - executedBefore
	if executed != 2 {
		t.Fatalf("actual async executions = %d, want 2", executed)
	}
	classified := discardShadowRetryActualReadyCounter.Snapshot().Count() - readyBefore +
		discardShadowRetryActualLateCounter.Snapshot().Count() - lateBefore +
		discardShadowRetryActualStaleCounter.Snapshot().Count() - staleBefore
	if classified != executed {
		t.Fatalf("actual async classified results = %d, executions = %d", classified, executed)
	}
	if errors := discardShadowRetryActualErrorsCounter.Snapshot().Count() - errorsBefore; errors != 0 {
		t.Fatalf("actual async errors = %d, want 0", errors)
	}
	if balance := statedb.GetBalance(testProcessorAddr(1)); balance != 7_000_000 {
		t.Fatalf("owner balance = %d, want 7000000", balance)
	}
}

func TestProcessBlockPublishesAsyncSenderRetryCohort(t *testing.T) {
	testProcessBlockPublishesAsyncSenderRetry(t, discardShadowAsyncPublishOffset)
}

func TestProcessBlockPublishesAsyncSenderRetryOnOrdinaryBlock(t *testing.T) {
	testProcessBlockPublishesAsyncSenderRetry(t, discardShadowAsyncPublishOffset+1)
}

func testProcessBlockPublishesAsyncSenderRetry(t *testing.T, blockNumber uint64) {
	t.Helper()
	base := newTestState(t)
	for id := byte(1); id <= 250; id++ {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 20_000_000)
	base.AddBalance(testProcessorAddr(3), 10_000_000)
	for id := byte(10); id <= 99; id++ {
		base.AddBalance(testProcessorAddr(id), 1_000_000)
	}
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 1, 2_000_000),
		makeTestTransferTx(1, 4, 3_000_000),
	}
	// Leave enough independent canonical work between the conflicted sender
	// retry and its next publication boundary for the background incarnation
	// to complete even under the race detector. Production never waits for the
	// worker: a result that misses its boundary still falls back to serial.
	for id := byte(10); id <= 99; id++ {
		transactions = append(transactions, makeTestTransferTx(id, id+140, 1_000))
	}
	transactions = append(transactions, makeTestTransferTx(1, 5, 1_000_000))
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for txIndex, tx := range transactions {
		transactionProtos[txIndex] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(blockNumber), Timestamp: 3_000,
		}},
		Transactions: transactionProtos,
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, nil, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{captureBalanceTrace: true})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	retryCandidatesBefore := parallelTransferRetryCandidatesCounter.Snapshot().Count()
	retryPublishedBefore := parallelTransferRetryPublishedCounter.Snapshot().Count()
	publishedBefore := discardShadowRetryActualPublishedCounter.Snapshot().Count()
	writeMatchesBefore := discardShadowRetryActualPublishedWriteOKCounter.Snapshot().Count()
	writeMismatchesBefore := discardShadowRetryActualPublishedWriteMismatchCounter.Snapshot().Count()
	captureBlocksBefore := versionedShadowWriteCaptureBlocksCounter.Snapshot().Count()
	captureTransactionsBefore := versionedShadowWriteCaptureTransactionsCounter.Snapshot().Count()
	captureFullBefore := versionedShadowWriteCaptureFullTransactionsCounter.Snapshot().Count()
	captureFilteredBefore := versionedShadowWriteCaptureFilteredTransactionsCounter.Snapshot().Count()
	captureCellsBefore := versionedShadowWriteCaptureCellsCounter.Snapshot().Count()
	captureNanosBefore := versionedShadowWriteCaptureNanosCounter.Snapshot().Count()
	captureFullCellsBefore := versionedShadowWriteCaptureFullCellsCounter.Snapshot().Count()
	captureFilteredCellsBefore := versionedShadowWriteCaptureFilteredCellsCounter.Snapshot().Count()
	captureFullNanosBefore := versionedShadowWriteCaptureFullNanosCounter.Snapshot().Count()
	captureFilteredNanosBefore := versionedShadowWriteCaptureFilteredNanosCounter.Snapshot().Count()
	captureRecorderTransactionsBefore := versionedShadowWriteCaptureRecorderTransactionsCounter.Snapshot().Count()
	captureRecorderNanosBefore := versionedShadowWriteCaptureRecorderNanosCounter.Snapshot().Count()
	captureRecorderFullTransactionsBefore := versionedShadowRecorderFullTxCounter.Snapshot().Count()
	captureRecorderFullNanosBefore := versionedShadowRecorderFullNanosCounter.Snapshot().Count()
	captureFilteredEmptyBefore := versionedShadowWriteCaptureFilteredEmptyCounter.Snapshot().Count()
	captureUnsupportedBefore := versionedShadowWriteCaptureUnsupportedCounter.Snapshot().Count()
	captureErrorsBefore := versionedShadowWriteCaptureErrorsCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true, captureBalanceTrace: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if candidates := parallelTransferRetryCandidatesCounter.Snapshot().Count() - retryCandidatesBefore; candidates != 1 {
		t.Fatalf("async retry publication candidates = %d, want 1", candidates)
	}
	if published := parallelTransferRetryPublishedCounter.Snapshot().Count() - retryPublishedBefore; published != 1 {
		t.Fatalf("async retry publications = %d, want 1", published)
	}
	if published := discardShadowRetryActualPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("async retry published results = %d, want 1", published)
	}
	if matches := discardShadowRetryActualPublishedWriteOKCounter.Snapshot().Count() - writeMatchesBefore; matches != 1 {
		mismatches := discardShadowRetryActualPublishedWriteMismatchCounter.Snapshot().Count() - writeMismatchesBefore
		t.Fatalf("async retry published write matches = %d, mismatches = %d, want 1/0", matches, mismatches)
	}
	if mismatches := discardShadowRetryActualPublishedWriteMismatchCounter.Snapshot().Count() - writeMismatchesBefore; mismatches != 0 {
		t.Fatalf("async retry published write mismatches = %d, want 0", mismatches)
	}
	if blocks := versionedShadowWriteCaptureBlocksCounter.Snapshot().Count() - captureBlocksBefore; blocks != 1 {
		t.Fatalf("write capture blocks = %d, want 1", blocks)
	}
	if captured := versionedShadowWriteCaptureTransactionsCounter.Snapshot().Count() - captureTransactionsBefore; captured != int64(len(transactions)) {
		t.Fatalf("write capture transactions = %d, want %d", captured, len(transactions))
	}
	fullCaptured := versionedShadowWriteCaptureFullTransactionsCounter.Snapshot().Count() - captureFullBefore
	filteredCaptured := versionedShadowWriteCaptureFilteredTransactionsCounter.Snapshot().Count() - captureFilteredBefore
	if blockNumber%discardShadowSampleInterval == 0 {
		if fullCaptured != int64(len(transactions)) || filteredCaptured != 0 {
			t.Fatalf("sampled write capture full/filtered = %d/%d, want %d/0", fullCaptured, filteredCaptured, len(transactions))
		}
	} else if fullCaptured != 3 || filteredCaptured != int64(len(transactions)-3) {
		t.Fatalf("ordinary write capture full/filtered = %d/%d, want 3/%d", fullCaptured, filteredCaptured, len(transactions)-3)
	}
	if cells := versionedShadowWriteCaptureCellsCounter.Snapshot().Count() - captureCellsBefore; cells <= 0 {
		t.Fatalf("write capture cells = %d, want > 0", cells)
	}
	if nanos := versionedShadowWriteCaptureNanosCounter.Snapshot().Count() - captureNanosBefore; nanos <= 0 {
		t.Fatalf("write capture nanos = %d, want > 0", nanos)
	}
	fullCells := versionedShadowWriteCaptureFullCellsCounter.Snapshot().Count() - captureFullCellsBefore
	filteredCells := versionedShadowWriteCaptureFilteredCellsCounter.Snapshot().Count() - captureFilteredCellsBefore
	if cells := versionedShadowWriteCaptureCellsCounter.Snapshot().Count() - captureCellsBefore; fullCells+filteredCells != cells {
		t.Fatalf("write capture full/filtered cells = %d/%d, total %d", fullCells, filteredCells, cells)
	}
	fullNanos := versionedShadowWriteCaptureFullNanosCounter.Snapshot().Count() - captureFullNanosBefore
	filteredNanos := versionedShadowWriteCaptureFilteredNanosCounter.Snapshot().Count() - captureFilteredNanosBefore
	if nanos := versionedShadowWriteCaptureNanosCounter.Snapshot().Count() - captureNanosBefore; fullNanos+filteredNanos != nanos {
		t.Fatalf("write capture full/filtered nanos = %d/%d, total %d", fullNanos, filteredNanos, nanos)
	}
	if blockNumber%discardShadowSampleInterval == 0 && (filteredCells != 0 || filteredNanos != 0) {
		t.Fatalf("sampled filtered write capture cells/nanos = %d/%d, want 0/0", filteredCells, filteredNanos)
	}
	recorderTransactions := versionedShadowWriteCaptureRecorderTransactionsCounter.Snapshot().Count() - captureRecorderTransactionsBefore
	recorderNanos := versionedShadowWriteCaptureRecorderNanosCounter.Snapshot().Count() - captureRecorderNanosBefore
	recorderFullTransactions := versionedShadowRecorderFullTxCounter.Snapshot().Count() - captureRecorderFullTransactionsBefore
	recorderFullNanos := versionedShadowRecorderFullNanosCounter.Snapshot().Count() - captureRecorderFullNanosBefore
	if blockNumber%discardShadowSampleInterval == 0 {
		if recorderTransactions != 0 || recorderNanos != 0 || recorderFullTransactions != 0 || recorderFullNanos != 0 {
			t.Fatalf("sampled recorder-only capture total/full transactions/nanos = %d/%d %d/%d, want zero", recorderTransactions, recorderNanos, recorderFullTransactions, recorderFullNanos)
		}
	} else if recorderTransactions != int64(len(transactions)) || recorderNanos <= 0 || recorderFullTransactions != fullCaptured || recorderFullNanos <= 0 {
		t.Fatalf("ordinary recorder-only capture total/full transactions/nanos = %d/%d %d/%d, want %d/>0 %d/>0", recorderTransactions, recorderNanos, recorderFullTransactions, recorderFullNanos, len(transactions), fullCaptured)
	}
	filteredEmpty := versionedShadowWriteCaptureFilteredEmptyCounter.Snapshot().Count() - captureFilteredEmptyBefore
	if blockNumber%discardShadowSampleInterval == 0 && filteredEmpty != 0 {
		t.Fatalf("sampled filtered empty captures = %d, want 0", filteredEmpty)
	}
	if blockNumber%discardShadowSampleInterval != 0 && filteredEmpty == 0 {
		t.Fatal("ordinary filtered empty captures = 0, want > 0")
	}
	if unsupported := versionedShadowWriteCaptureUnsupportedCounter.Snapshot().Count() - captureUnsupportedBefore; unsupported != 0 {
		t.Fatalf("write capture unsupported = %d, want 0", unsupported)
	}
	if captureErrors := versionedShadowWriteCaptureErrorsCounter.Snapshot().Count() - captureErrorsBefore; captureErrors != 0 {
		t.Fatalf("write capture errors = %d, want 0", captureErrors)
	}
	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockReincarnatesSenderRetryAfterLaterConflict(t *testing.T) {
	statedb := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4, 5, 6, 7} {
		statedb.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	statedb.AddBalance(testProcessorAddr(1), 20_000_000)
	statedb.AddBalance(testProcessorAddr(6), 10_000_000)
	statedb.AddBalance(testProcessorAddr(7), 10_000_000)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(6, 1, 2_000_000),
		makeTestTransferTx(1, 3, 3_000_000),
		makeTestTransferTx(7, 1, 2_000_000),
		makeTestTransferTx(1, 4, 4_000_000),
		makeTestTransferTx(1, 5, 1_000_000),
	}
	transactionProtos := make([]*corepb.Transaction, len(transactions))
	for index, tx := range transactions {
		transactionProtos[index] = tx.Proto()
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryInterval), Timestamp: 3_000,
		}},
		Transactions: transactionProtos,
	})
	attemptsBefore := discardShadowRetryAttemptsCounter.Snapshot().Count()
	executedBefore := discardShadowRetryExecutedCounter.Snapshot().Count()
	candidatesBefore := discardShadowRetryCandidatesCounter.Snapshot().Count()
	recoveredBefore := discardShadowRetryRecoveredCounter.Snapshot().Count()
	validatedBefore := discardShadowRetryValidatedCounter.Snapshot().Count()
	prefixRefreshBefore := discardShadowRetryPrefixRefreshCounter.Snapshot().Count()
	prefixReuseBefore := discardShadowRetryPrefixReuseCounter.Snapshot().Count()
	prefixAdvanceBefore := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count()
	mismatchBefore := discardShadowRetryInfoMismatchCounter.Snapshot().Count() +
		discardShadowRetryWriteMismatchCounter.Snapshot().Count() +
		discardShadowRetryBalanceMismatchCounter.Snapshot().Count() +
		discardShadowRetryErrorsCounter.Snapshot().Count()
	if _, _, err := processBlockWithOptions(
		statedb, statedb.DynamicProperties(), block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
		processBlockOptions{parallelTransfers: true},
	); err != nil {
		t.Fatalf("process reincarnated sender retry: %v", err)
	}
	if attempts := discardShadowRetryAttemptsCounter.Snapshot().Count() - attemptsBefore; attempts != 2 {
		t.Fatalf("sender retry attempts = %d, want 2", attempts)
	}
	if executed := discardShadowRetryExecutedCounter.Snapshot().Count() - executedBefore; executed != 5 {
		t.Fatalf("sender retry executions = %d, want 5", executed)
	}
	if candidates := discardShadowRetryCandidatesCounter.Snapshot().Count() - candidatesBefore; candidates != 3 {
		t.Fatalf("sender retry candidates = %d, want 3", candidates)
	}
	if recovered := discardShadowRetryRecoveredCounter.Snapshot().Count() - recoveredBefore; recovered != 3 {
		t.Fatalf("sender retry recovered = %d, want 3", recovered)
	}
	if validated := discardShadowRetryValidatedCounter.Snapshot().Count() - validatedBefore; validated != 3 {
		t.Fatalf("sender retry validated = %d, want 3", validated)
	}
	if refreshes := discardShadowRetryPrefixRefreshCounter.Snapshot().Count() - prefixRefreshBefore; refreshes != 1 {
		t.Fatalf("sender retry prefix refreshes = %d, want 1", refreshes)
	}
	if reuses := discardShadowRetryPrefixReuseCounter.Snapshot().Count() - prefixReuseBefore; reuses != 1 {
		t.Fatalf("sender retry prefix reuses = %d, want 1", reuses)
	}
	if advances := discardShadowRetryPrefixAdvanceCounter.Snapshot().Count() - prefixAdvanceBefore; advances != 2 {
		t.Fatalf("sender retry prefix advances = %d, want 2", advances)
	}
	mismatchAfter := discardShadowRetryInfoMismatchCounter.Snapshot().Count() +
		discardShadowRetryWriteMismatchCounter.Snapshot().Count() +
		discardShadowRetryBalanceMismatchCounter.Snapshot().Count() +
		discardShadowRetryErrorsCounter.Snapshot().Count()
	if mismatches := mismatchAfter - mismatchBefore; mismatches != 0 {
		t.Fatalf("sender retry mismatches/errors = %d, want 0", mismatches)
	}
	if balance := statedb.GetBalance(testProcessorAddr(1)); balance != 15_000_000 {
		t.Fatalf("owner balance = %d, want 15000000", balance)
	}
}

func TestProcessBlockPublishesVersionedSenderChain(t *testing.T) {
	base := newTestState(t)
	for _, id := range []byte{1, 2, 3} {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 10_000_000)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(1, 3, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3_000}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, nil, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	conflictsBefore := parallelTransferConflictFallbackCounter.Snapshot().Count()
	chainPreexecutedBefore := parallelTransferChainPreexecutedCounter.Snapshot().Count()
	chainCandidatesBefore := parallelTransferChainCandidatesCounter.Snapshot().Count()
	chainPublishedBefore := parallelTransferChainPublishedCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; published != 2 {
		t.Fatalf("published sender-chain transfers = %d, want 2", published)
	}
	if conflicts := parallelTransferConflictFallbackCounter.Snapshot().Count() - conflictsBefore; conflicts != 0 {
		t.Fatalf("sender-chain conflict fallbacks = %d, want 0", conflicts)
	}
	if preexecuted := parallelTransferChainPreexecutedCounter.Snapshot().Count() - chainPreexecutedBefore; preexecuted != 1 {
		t.Fatalf("preexecuted sender-chain dependents = %d, want 1", preexecuted)
	}
	if candidates := parallelTransferChainCandidatesCounter.Snapshot().Count() - chainCandidatesBefore; candidates != 1 {
		t.Fatalf("sender-chain candidates = %d, want 1", candidates)
	}
	if published := parallelTransferChainPublishedCounter.Snapshot().Count() - chainPublishedBefore; published != 1 {
		t.Fatalf("published sender-chain dependents = %d, want 1", published)
	}
	for txIndex := range serialInfos {
		if !proto.Equal(serialInfos[txIndex], parallelInfos[txIndex]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", txIndex, serialInfos[txIndex], parallelInfos[txIndex])
		}
	}
	for _, id := range []byte{1, 2, 3} {
		address := testProcessorAddr(id)
		if serial, parallel := serialState.GetBalance(address), parallelState.GetBalance(address); serial != parallel {
			t.Fatalf("account %d balance serial=%d parallel=%d", id, serial, parallel)
		}
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
}

func TestProcessBlockParallelTransfersFallsBackWhenPublicNetLimitIsExhausted(t *testing.T) {
	base := newTestState(t)
	for _, id := range []byte{1, 2, 3, 4} {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	base.AddBalance(testProcessorAddr(1), 10_000_000)
	base.AddBalance(testProcessorAddr(3), 20_000_000)
	base.DynamicProperties().SetPublicNetLimit(150)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	serialState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	serialState.SetDynamicProperties(base.DynamicProperties().Copy())
	parallelState, err := base.Copy()
	if err != nil {
		t.Fatal(err)
	}
	parallelState.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(3, 4, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1, Timestamp: 3_000}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	run := func(statedb *state.StateDB, options processBlockOptions) ([]*corepb.TransactionInfo, error) {
		infos, _, processErr := processBlockWithOptions(
			statedb, statedb.DynamicProperties(), block, nil, nil, 0,
			params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
			nil, forks.NewVersionPassCache(), new(transactionInfoBatch), true, -1, nil,
			options,
		)
		return infos, processErr
	}
	serialInfos, err := run(serialState, processBlockOptions{})
	if err != nil {
		t.Fatalf("serial process: %v", err)
	}
	publishedBefore := parallelTransferPublishedCounter.Snapshot().Count()
	limitFallbackBefore := parallelTransferPublicNetLimitFallbackCounter.Snapshot().Count()
	parallelInfos, err := run(parallelState, processBlockOptions{parallelTransfers: true})
	if err != nil {
		t.Fatalf("parallel process: %v", err)
	}
	if published := parallelTransferPublishedCounter.Snapshot().Count() - publishedBefore; published != 1 {
		t.Fatalf("published transfers = %d, want 1", published)
	}
	if fallbacks := parallelTransferPublicNetLimitFallbackCounter.Snapshot().Count() - limitFallbackBefore; fallbacks != 1 {
		t.Fatalf("public-net limit fallbacks = %d, want 1", fallbacks)
	}
	for i := range serialInfos {
		if !proto.Equal(serialInfos[i], parallelInfos[i]) {
			t.Fatalf("tx %d info mismatch\nserial=%v\nparallel=%v", i, serialInfos[i], parallelInfos[i])
		}
	}
	if serial, parallel := serialState.DynamicProperties().PublicNetUsage(), parallelState.DynamicProperties().PublicNetUsage(); serial != parallel {
		t.Fatalf("public net usage serial=%d parallel=%d", serial, parallel)
	}
	if serial, parallel := serialState.DynamicProperties().TotalTransactionCost(), parallelState.DynamicProperties().TotalTransactionCost(); serial != parallel {
		t.Fatalf("transaction cost serial=%d parallel=%d", serial, parallel)
	}
	serialRoot, err := serialState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	parallelRoot, err := parallelState.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if serialRoot != parallelRoot {
		t.Fatalf("state roots differ: serial=%x parallel=%x", serialRoot, parallelRoot)
	}
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

func TestTransactionInfoSlotOwnsContractAddress(t *testing.T) {
	tx := makeTestTriggerTx(1, testProcessorAddr(2), nil)
	firstAddress := testProcessorAddr(3)
	secondAddress := testProcessorAddr(4)
	sharedResultAddress := firstAddress.Bytes()
	result := &actuator.Result{ContractAddress: sharedResultAddress}
	var firstSlot, secondSlot transactionInfoSlot
	firstInfo := firstSlot.build(tx, result, 1, 3_000, false)

	copy(sharedResultAddress, secondAddress.Bytes())
	secondInfo := secondSlot.build(tx, result, 1, 3_000, false)
	if !bytes.Equal(firstInfo.ContractAddress, firstAddress.Bytes()) {
		t.Fatalf("first slot contract address = %x, want %x", firstInfo.ContractAddress, firstAddress)
	}
	if !bytes.Equal(secondInfo.ContractAddress, secondAddress.Bytes()) {
		t.Fatalf("second slot contract address = %x, want %x", secondInfo.ContractAddress, secondAddress)
	}
}

func TestProcessBlock_CanDiscardTransactionInfosAfterValidation(t *testing.T) {
	statedb := newTestState(t)
	dynProps := state.NewDynamicProperties()

	owner := testProcessorAddr(1)
	receiver := testProcessorAddr(2)
	witness := testProcessorAddr(0xff)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 10_000_000)
	statedb.CreateAccount(receiver, corepb.AccountType_Normal)
	statedb.CreateAccount(witness, corepb.AccountType_Normal)
	if _, err := statedb.Commit(); err != nil {
		t.Fatal(err)
	}

	tx := makeTestTransferTx(1, 2, 1_000_000)
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number:         1,
			Timestamp:      3000,
			WitnessAddress: witness.Bytes(),
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	batch := new(transactionInfoBatch)
	txInfos, _, err := processBlock(
		statedb, dynProps, block, nil, nil, 0,
		params.DefaultBlockNumForEnergyLimit, false, tcommon.Hash{}, nil, nil,
		nil, nil, batch, false, -1, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if txInfos != nil {
		t.Fatalf("discarded transaction infos = %+v, want nil", txInfos)
	}
	if len(batch.slots) != 1 {
		t.Fatalf("execution scratch slots = %d, want 1", len(batch.slots))
	}
	if got := statedb.GetBalance(owner); got != 9_000_000 {
		t.Fatalf("owner balance = %d, want 9000000", got)
	}
	if got := statedb.GetBalance(receiver); got != 1_000_000 {
		t.Fatalf("receiver balance = %d, want 1000000", got)
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
