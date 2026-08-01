package core

import (
	"encoding/binary"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestCompareDiscardShadowInfoSplitsEnergyFields(t *testing.T) {
	tests := []struct {
		name string
		set  func(*corepb.ResourceReceipt)
		want discardShadowMismatch
	}{
		{"usage", func(receipt *corepb.ResourceReceipt) { receipt.EnergyUsage = 1 }, discardShadowMismatchEnergyUsage},
		{"fee", func(receipt *corepb.ResourceReceipt) { receipt.EnergyFee = 1 }, discardShadowMismatchEnergyFee},
		{"origin", func(receipt *corepb.ResourceReceipt) { receipt.OriginEnergyUsage = 1 }, discardShadowMismatchOriginEnergy},
		{"total", func(receipt *corepb.ResourceReceipt) { receipt.EnergyUsageTotal = 1 }, discardShadowMismatchEnergyTotal},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			shadow := &corepb.TransactionInfo{Receipt: &corepb.ResourceReceipt{}}
			canonical := &corepb.TransactionInfo{Receipt: &corepb.ResourceReceipt{}}
			test.set(shadow.Receipt)
			mismatch := compareDiscardShadowInfo(shadow, canonical)
			want := discardShadowMismatchReceipt | discardShadowMismatchReceiptCore |
				discardShadowMismatchReceiptEnergy | test.want
			if mismatch != want {
				t.Fatalf("mismatch = %#x, want %#x", mismatch, want)
			}
		})
	}
}

func TestCompareDiscardShadowInfoHandlesMissingDiagnosticData(t *testing.T) {
	canonical := &corepb.TransactionInfo{Receipt: &corepb.ResourceReceipt{EnergyUsage: 1}}
	if got := compareDiscardShadowInfo(nil, canonical); got != discardShadowMismatchOtherField {
		t.Fatalf("nil shadow mismatch = %#x, want %#x", got, discardShadowMismatchOtherField)
	}
	if got := compareDiscardShadowInfo(&corepb.TransactionInfo{}, canonical); got == 0 || got&discardShadowMismatchReceipt == 0 {
		t.Fatalf("nil receipt mismatch = %#x, want receipt mismatch", got)
	}
}

func TestClassifyDiscardShadowApplyMismatch(t *testing.T) {
	fieldKey := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		AccountField: state.TransactionAccountFieldBalance,
	}
	dynamicKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "fee"}
	accountKey := state.TransactionAccessKey{Kind: state.TransactionAccessAccount}
	expected := state.TransactionWriteSet{
		fieldKey:   {Exists: true, Commutative: true, Value: []byte{1}},
		dynamicKey: {Exists: true, Value: []byte{3}},
	}
	applied := state.TransactionWriteSet{
		fieldKey:   {Exists: false, Value: []byte{2}},
		accountKey: {Exists: true, Value: []byte{4}},
	}
	want := discardShadowApplyMismatchMissing |
		discardShadowApplyMismatchExtra |
		discardShadowApplyMismatchPresence |
		discardShadowApplyMismatchCommutative |
		discardShadowApplyMismatchValue |
		discardShadowApplyMismatchAccount |
		discardShadowApplyMismatchAccountField |
		discardShadowApplyMismatchDynamic
	if got := classifyDiscardShadowApplyMismatch(applied, expected); got != want {
		t.Fatalf("mismatch = %#x, want %#x", got, want)
	}
}

func TestDiscardKVOverlayIsolatesWrites(t *testing.T) {
	parent := ethrawdb.NewMemoryDatabase()
	if err := parent.Put([]byte("stable"), []byte("parent")); err != nil {
		t.Fatal(err)
	}
	var recorder state.TransactionAccessRecorder
	recorder.Reset(8)
	overlay := discardKVOverlay{parent: parent, recorder: &recorder}
	if err := overlay.Put([]byte("stable"), []byte("worker")); err != nil {
		t.Fatal(err)
	}
	if err := overlay.Put([]byte("new"), []byte("value")); err != nil {
		t.Fatal(err)
	}
	if got, err := overlay.Get([]byte("stable")); err != nil || string(got) != "worker" {
		t.Fatalf("overlay stable = %q, %v", got, err)
	}
	if got, err := parent.Get([]byte("stable")); err != nil || string(got) != "parent" {
		t.Fatalf("parent stable = %q, %v", got, err)
	}
	if exists, err := parent.Has([]byte("new")); err != nil || exists {
		t.Fatalf("worker write escaped to parent: exists=%v err=%v", exists, err)
	}
	if err := overlay.Delete([]byte("stable")); err != nil {
		t.Fatal(err)
	}
	if exists, err := overlay.Has([]byte("stable")); err != nil || exists {
		t.Fatalf("overlay delete = exists:%v err:%v", exists, err)
	}
	if exists, err := parent.Has([]byte("stable")); err != nil || !exists {
		t.Fatalf("overlay delete escaped to parent: exists=%v err=%v", exists, err)
	}
	writes, known, err := newTestState(t).CaptureTransactionWriteSet(0, &recorder, state.NewDynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture overlay writes: known=%v err=%v", known, err)
	}
	deletedKey := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "stable"}
	if value, ok := writes[deletedKey]; !ok || value.Exists {
		t.Fatalf("overlay delete write = %+v ok=%v", value, ok)
	}
	putKey := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "new"}
	if value, ok := writes[putKey]; !ok || !value.Exists || string(value.Value) != "value" {
		t.Fatalf("overlay put write = %+v ok=%v", value, ok)
	}
	overlay.reset()
	if got, err := overlay.Get([]byte("stable")); err != nil || string(got) != "parent" {
		t.Fatalf("reset overlay stable = %q, %v", got, err)
	}
}

func TestAsyncRetryFrozenRawViewRejectsLiveFallback(t *testing.T) {
	parent := ethrawdb.NewMemoryDatabase()
	if err := parent.Put([]byte("stable"), []byte("at-boundary")); err != nil {
		t.Fatal(err)
	}
	source := &discardShadowPreexecution{
		results: []discardShadowTaskResult{
			{reads: state.TransactionReadSet{Reads: []state.TransactionRead{{
				Key: state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "stable"}, Mode: state.TransactionAccessRead,
			}}}},
			{reads: state.TransactionReadSet{Reads: []state.TransactionRead{{
				Key: state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "missing"}, Mode: state.TransactionAccessRead,
			}}}},
		},
		resultByTx: []int{0, 1},
		senderNext: []int{1, -1},
	}
	retry := &discardShadowSenderRetry{source: source}
	runner := newDiscardShadowRetryRunner(nil)
	runner.prefixRaw.parent = parent
	var recorder state.TransactionAccessRecorder
	recorder.Reset(8)
	runner.prefixRaw.recorder = &recorder
	frozen, keys, err := retry.freezeAsyncRawView(runner, 0)
	if err != nil {
		t.Fatal(err)
	}
	if keys != 2 {
		t.Fatalf("frozen keys = %d, want 2", keys)
	}
	if err := parent.Put([]byte("stable"), []byte("later-live-value")); err != nil {
		t.Fatal(err)
	}
	if got, err := frozen.Get([]byte("stable")); err != nil || string(got) != "at-boundary" {
		t.Fatalf("frozen stable = %q, %v", got, err)
	}
	if exists, err := frozen.Has([]byte("missing")); err != nil || exists {
		t.Fatalf("frozen absent key = exists:%v err:%v", exists, err)
	}
	if _, err := frozen.Get([]byte("uncaptured")); !errors.Is(err, errDiscardShadowFrozenRawMiss) {
		t.Fatalf("uncaptured read error = %v", err)
	}
	if frozen.misses != 1 {
		t.Fatalf("frozen misses = %d, want 1", frozen.misses)
	}
}

func TestAsyncRetryRejectionMetricsPreserveConflictClasses(t *testing.T) {
	retry := new(discardShadowSenderRetry)
	retry.recordAsyncRetryRejection(discardShadowReadVersionResult{
		readConflict: true,
		sender:       true,
		barrier:      true,
		unsupported:  true,
		deltaInvalid: true,
	})
	retry.recordAsyncRetryRejection(discardShadowReadVersionResult{readConflict: true})
	retry.recordAsyncRetryRejection(discardShadowReadVersionResult{publishable: true})
	if retry.stats.actualRejected != 2 || retry.stats.actualReadConflict != 2 ||
		retry.stats.actualSender != 1 || retry.stats.actualBarrier != 1 ||
		retry.stats.actualUnsupported != 1 || retry.stats.actualDeltaInvalid != 1 {
		t.Fatalf("actual rejection stats = %+v", retry.stats)
	}
}

func TestAsyncRetryFailedResultNeverBecomesAvailable(t *testing.T) {
	retry := &discardShadowSenderRetry{
		results:      make([]discardShadowTaskResult, 1),
		available:    make([]bool, 1),
		selectedOK:   make([]bool, 1),
		incarnations: []uint32{1},
	}
	retry.consumeAsyncEvent(discardShadowAsyncRetryEvent{result: &discardShadowTaskResult{
		txIndex: 0, incarnation: 1, err: errors.New("speculative execution failed"),
	}}, 0)
	if retry.available[0] {
		t.Fatal("failed async result became available")
	}
	if retry.stats.actualExecuted != 1 || retry.stats.actualReady != 1 || retry.stats.actualErrors != 1 {
		t.Fatalf("async failure stats = %+v", retry.stats)
	}
}

func TestAsyncRetryReservationsBoundConcurrentExecution(t *testing.T) {
	const transactionCount = 4
	source := &discardShadowPreexecution{
		senderTasks:  make([]discardShadowSenderChainTask, transactionCount),
		senderTaskOK: make([]bool, transactionCount),
		senderNext:   []int{1, 2, 3, -1},
	}
	for txIndex := 0; txIndex < transactionCount; txIndex++ {
		source.senderTasks[txIndex] = discardShadowSenderChainTask{txIndex: txIndex}
		source.senderTaskOK[txIndex] = true
	}
	retry := &discardShadowSenderRetry{
		source:             source,
		available:          make([]bool, transactionCount),
		selectedOK:         make([]bool, transactionCount),
		selectedAsyncReady: make([]bool, transactionCount),
		incarnations:       make([]uint32, transactionCount),
		asyncScheduled:     discardShadowRetryMaxExecutions - 1,
	}
	tasks := retry.invalidateAsyncSuffix(0)
	if len(tasks) != 1 {
		t.Fatalf("reserved tasks = %d, want 1", len(tasks))
	}
	retry.asyncScheduled += int64(len(tasks))
	if tasks := retry.invalidateAsyncSuffix(1); len(tasks) != 0 {
		t.Fatalf("tasks exceeded global execution reservation: %d", len(tasks))
	}
}

func TestAsyncRetrySnapshotsOnlySuffixReadVersions(t *testing.T) {
	account := testProcessorAddr(1)
	fieldAccount := testProcessorAddr(2)
	readKey := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "read"}
	unreadKey := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "unread"}
	fieldKey := state.TransactionAccountFieldKey{Address: fieldAccount, Field: state.TransactionAccountFieldBalance}
	versioned := &versionedAccessShadow{
		versions:             map[state.TransactionAccessKey]int{readKey: 3, unreadKey: 4},
		accountAnyVersions:   map[tcommon.Address]int{account: 5},
		accountFullVersions:  map[tcommon.Address]int{fieldAccount: 6},
		accountFieldVersions: map[state.TransactionAccountFieldKey]int{fieldKey: 7},
	}
	pre := &discardShadowPreexecution{
		results: []discardShadowTaskResult{{reads: state.TransactionReadSet{Reads: []state.TransactionRead{
			{Key: readKey, Mode: state.TransactionAccessRead},
			{Key: state.TransactionAccessKey{Kind: state.TransactionAccessAccount, Address: account}, Mode: state.TransactionAccessRead},
			{Key: state.TransactionAccessKey{Kind: state.TransactionAccessAccountField, Address: fieldAccount, AccountField: state.TransactionAccountFieldBalance}, Mode: state.TransactionAccessRead},
		}}}},
		resultByTx: []int{0},
		senderNext: []int{-1},
	}
	view, cells := snapshotDiscardShadowVersionView(versioned, pre, 0)
	if cells != 4 {
		t.Fatalf("frozen version cells = %d, want 4", cells)
	}
	if previous, ok := view.typedPreviousVersion(readKey, 10); !ok || previous != 3 {
		t.Fatalf("read-key version = %d, %v", previous, ok)
	}
	if previous, ok := view.typedPreviousVersion(state.TransactionAccessKey{Kind: state.TransactionAccessAccount, Address: account}, 10); !ok || previous != 5 {
		t.Fatalf("account version = %d, %v", previous, ok)
	}
	if previous, ok := view.typedPreviousVersion(state.TransactionAccessKey{Kind: state.TransactionAccessAccountField, Address: fieldAccount, AccountField: state.TransactionAccountFieldBalance}, 10); !ok || previous != 7 {
		t.Fatalf("field version = %d, %v", previous, ok)
	}
	if _, ok := view.typedPreviousVersion(unreadKey, 10); ok {
		t.Fatal("unread version leaked into compact retry view")
	}
}

func TestClassifyDiscardShadowApplyUnsupported(t *testing.T) {
	writes := state.TransactionWriteSet{
		{Kind: state.TransactionAccessAccount, Address: testProcessorAddr(1)}:                                                                 {},
		{Kind: state.TransactionAccessAccountKVGeneration, Address: testProcessorAddr(1)}:                                                     {},
		{Kind: state.TransactionAccessAccountField, Address: testProcessorAddr(1), AccountField: state.TransactionAccountFieldFrozenResource}: {},
	}
	want := discardShadowApplyUnsupportedAccount | discardShadowApplyUnsupportedGeneration | discardShadowApplyUnsupportedField
	if got := classifyDiscardShadowApplyUnsupported(writes); got != want {
		t.Fatalf("unsupported classes = %#x, want %#x", got, want)
	}
}

func TestDiscardShadowWorkerMatchesAndRevertsTransfer(t *testing.T) {
	canonical := newTestState(t)
	owner := testProcessorAddr(1)
	recipient := testProcessorAddr(2)
	canonical.CreateAccount(owner, corepb.AccountType_Normal)
	canonical.AddBalance(owner, 10_000_000)
	canonical.CreateAccount(recipient, corepb.AccountType_Normal)
	if _, err := canonical.Commit(); err != nil {
		t.Fatal(err)
	}

	workerState, err := canonical.Copy()
	if err != nil {
		t.Fatal(err)
	}
	workerState.SetDynamicProperties(canonical.DynamicProperties().Copy())
	tx := makeTestTransferTx(1, 2, 1_000_000)
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number:    int64(discardShadowSampleInterval),
			Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})

	var accessRecorder state.TransactionAccessRecorder
	accessRecorder.Reset(16)
	canonical.BeginBalanceTrace(int64(block.Number()), block.Hash().Bytes(), block.Timestamp())
	canonical.BeginBalanceTraceTransaction(tx.Hash().Bytes(), tx.ContractType().String())
	canonical.SetTransactionAccessRecorder(&accessRecorder)
	canonical.DynamicProperties().SetTransactionAccessRecorder(&accessRecorder)
	journalMark := canonical.DomainChangeJournalMark()
	result, err := applyTransaction(
		canonical,
		canonical.DynamicProperties(),
		tx,
		0,
		true,
		0,
		block.Timestamp(),
		block.Number(),
		nil,
		nil,
		0,
		[32]byte{},
		[21]byte{},
		true,
		false,
		true,
		forks.NewVersionPassCache(),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	canonical.SetTransactionAccessRecorder(nil)
	canonical.DynamicProperties().SetTransactionAccessRecorder(nil)
	canonical.FinalizeTransaction()
	canonical.EndBalanceTraceTransaction(balanceTraceTransactionStatus(result))
	canonicalBalanceTrace := canonical.CopyLastBalanceTraceTransaction(tx.Hash().Bytes())
	canonicalWriteSet, known, err := canonical.CaptureTransactionWriteSet(journalMark, &accessRecorder, canonical.DynamicProperties())
	if err != nil || !known {
		t.Fatalf("capture canonical transfer writes: known=%v err=%v", known, err)
	}
	canonicalInfo := buildTransactionInfo(tx, result, block.Number(), block.Timestamp(), canonical.DynamicProperties().AllowTransactionFeePool())
	worker := discardShadowWorker{
		state:     workerState,
		dynProps:  workerState.DynamicProperties(),
		forkCache: forks.NewVersionPassCache().BlockScope(),
	}
	cfg := discardShadowRunConfig{
		block:                  block,
		transactions:           []*types.Transaction{tx},
		canonicalInfos:         []*corepb.TransactionInfo{canonicalInfo},
		canonicalBalanceTraces: []*contractpb.TransactionBalanceTrace{canonicalBalanceTrace},
		canonicalWriteSets:     []state.TransactionWriteSet{canonicalWriteSet},
		captureBalanceTrace:    true,
		genesisTimestamp:       0,
	}
	preShadow := &discardShadowBlock{base: workerState}
	pre := preShadow.preexecuteTransfers(cfg)
	var versioned versionedAccessShadow
	versioned.Prepare(1)
	versioned.EnableWriteSetCapture(1)
	versioned.transactionSupported[0] = true
	versioned.transactionWritesOK[0] = true
	versioned.transactionWriteSets[0] = canonicalWriteSet
	pre.validateReadVersion(0, tx, &versioned)
	preStats := preShadow.finishTransferPreexecution(pre, &versioned, cfg)
	if preStats.transfers != 1 || preStats.executed != 1 || preStats.candidates != 1 ||
		preStats.infoMatches != 1 || preStats.writeMatches != 1 || preStats.applyMatches != 1 ||
		preStats.validated != 1 || preStats.infoMismatches != 0 || preStats.writeMismatches != 0 ||
		preStats.applyMismatches != 0 || preStats.applyUnsupported != 0 || preStats.orderedCandidates != 1 ||
		preStats.orderedMatches != 1 || preStats.orderedMismatches != 0 || preStats.orderedErrors != 0 || preStats.errors != 0 ||
		preStats.readCandidates != 1 || preStats.readPublishable != 1 || preStats.readDAGMatches != 1 || preStats.readDAGMismatches != 0 ||
		preStats.balanceMatches != 1 || preStats.balanceMismatches != 0 {
		t.Fatalf("transfer preexecutor stats = %+v", preStats)
	}
	got := worker.execute(0, cfg)
	if got.err != nil || !got.matched || got.writeSetErr != nil || !got.writeSetMatch || !got.applyEligible || got.applyErr != nil || !got.applyMatch {
		t.Fatalf("discard worker = info-matched:%v writes-matched:%v apply-eligible:%v apply-matched:%v err:%v write-err:%v apply-err:%v", got.matched, got.writeSetMatch, got.applyEligible, got.applyMatch, got.err, got.writeSetErr, got.applyErr)
	}
	if balance := workerState.GetBalance(owner); balance != 10_000_000 {
		t.Fatalf("discard worker owner balance = %d, want 10000000", balance)
	}
	if balance := workerState.GetBalance(recipient); balance != 0 {
		t.Fatalf("discard worker recipient balance = %d, want 0", balance)
	}
	ordered := (&discardShadowBlock{base: workerState}).verifyOrderedApply([]discardShadowTaskResult{got}, cfg)
	if ordered.candidates != 1 || ordered.matches != 1 || ordered.mismatches != 0 || ordered.errors != 0 {
		t.Fatalf("ordered publisher stats = %+v", ordered)
	}
}

func TestVersionedShadowValidatesFrozenWorkerReadVersions(t *testing.T) {
	owner := testProcessorAddr(1)
	key := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		Address:      owner,
		AccountField: state.TransactionAccountFieldBalance,
	}
	deltaKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "transaction_fee_pool"}
	base := func() versionedAccessShadow {
		var versioned versionedAccessShadow
		versioned.Prepare(2)
		versioned.transactionSupported[0] = true
		versioned.transactionSupported[1] = true
		return versioned
	}

	clean := base()
	if decision := clean.validateBlockStartReadSet(1, nil, discardShadowTaskResult{reads: state.TransactionReadSet{
		Reads: []state.TransactionRead{{Key: key, Mode: state.TransactionAccessRead}},
	}}); !decision.publishable {
		t.Fatalf("clean read unexpectedly invalid: %+v", decision)
	}

	conflicted := base()
	conflicted.accountFieldVersions[state.TransactionAccountFieldKey{Address: owner, Field: state.TransactionAccountFieldBalance}] = 0
	if decision := conflicted.validateBlockStartReadSet(1, nil, discardShadowTaskResult{reads: state.TransactionReadSet{
		Reads: []state.TransactionRead{{Key: key, Mode: state.TransactionAccessRead}},
	}}); decision.publishable || !decision.readConflict {
		t.Fatalf("ordinary stale read accepted: %+v", decision)
	}

	delta := base()
	delta.versions[deltaKey] = 0
	validDelta := discardShadowTaskResult{
		reads:  state.TransactionReadSet{Reads: []state.TransactionRead{{Key: deltaKey, Mode: state.TransactionAccessCommutativeRead}}},
		writes: state.TransactionWriteSet{deltaKey: {Exists: true, Commutative: true, Value: make([]byte, 8)}},
	}
	if decision := delta.validateBlockStartReadSet(1, nil, validDelta); !decision.publishable || decision.readConflict || decision.deltaInvalid {
		t.Fatalf("ordered delta unexpectedly invalid: %+v", decision)
	}
	validDelta.writes = nil
	if decision := delta.validateBlockStartReadSet(1, nil, validDelta); decision.publishable || !decision.deltaInvalid {
		t.Fatalf("commutative read without delta accepted: %+v", decision)
	}

	barrier := base()
	barrier.lastBarrierTx = 0
	if decision := barrier.validateBlockStartReadSet(1, nil, discardShadowTaskResult{}); decision.publishable || !decision.barrier {
		t.Fatalf("unknown predecessor barrier accepted: %+v", decision)
	}

	sender := base()
	sender.lastSenderTx[owner] = 0
	if decision := sender.validateBlockStartReadSet(1, makeTestTransferTx(1, 2, 1), discardShadowTaskResult{}); decision.publishable || !decision.sender {
		t.Fatalf("same-sender block-start result accepted: %+v", decision)
	}

	forwarded := base()
	forwarded.accountFieldVersions[state.TransactionAccountFieldKey{Address: owner, Field: state.TransactionAccountFieldBalance}] = 0
	forwarded.lastSenderTx[owner] = 0
	forwardedResult := discardShadowTaskResult{
		reads: state.TransactionReadSet{Reads: []state.TransactionRead{{
			Key: key, Mode: state.TransactionAccessRead, ExpectedWriter: 0, HasExpectedWriter: true,
		}}},
		senderPredecessor: 0,
		senderVersioned:   true,
	}
	if decision := forwarded.validateBlockStartReadSet(1, makeTestTransferTx(1, 2, 1), forwardedResult); !decision.publishable || decision.readConflict || decision.sender {
		t.Fatalf("matching sender-chain version rejected: %+v", decision)
	}

	var stale versionedAccessShadow
	stale.Prepare(3)
	stale.accountFieldVersions[state.TransactionAccountFieldKey{Address: owner, Field: state.TransactionAccountFieldBalance}] = 1
	stale.lastSenderTx[owner] = 1
	if decision := stale.validateBlockStartReadSet(2, makeTestTransferTx(1, 2, 1), forwardedResult); decision.publishable || !decision.readConflict || !decision.sender {
		t.Fatalf("intervening sender-chain writer accepted: %+v", decision)
	}
}

func TestSenderRetryReusesAndRefreshesSettledPrefix(t *testing.T) {
	live := newTestState(t)
	owner := testProcessorAddr(1)
	live.CreateAccount(owner, corepb.AccountType_Normal)
	live.AddBalance(owner, 10_000_000)
	if _, err := live.Commit(); err != nil {
		t.Fatal(err)
	}

	var versioned versionedAccessShadow
	versioned.Prepare(3)
	versioned.EnableWriteSetCapture(3)
	retry := &discardShadowSenderRetry{runner: newDiscardShadowRetryRunner(nil)}
	if !retry.ensureSettledPrefix(0, live, live.DynamicProperties(), &versioned, discardShadowRunConfig{}) {
		t.Fatal("initialize settled prefix")
	}
	if retry.stats.prefixRefreshes != 1 || retry.stats.prefixReuses != 0 || retry.runner.settledThrough != 0 {
		t.Fatalf("initial prefix stats = %+v settled=%d", retry.stats, retry.runner.settledThrough)
	}

	live.AddBalance(owner, 2_000_000)
	balanceValue := make([]byte, 8)
	binary.BigEndian.PutUint64(balanceValue, 12_000_000)
	versioned.transactionWritesOK[1] = true
	versioned.transactionWriteSets[1] = state.TransactionWriteSet{
		{
			Kind:         state.TransactionAccessAccountField,
			Address:      owner,
			AccountField: state.TransactionAccountFieldBalance,
		}: {Exists: true, Value: balanceValue},
	}
	if !retry.ensureSettledPrefix(1, live, live.DynamicProperties(), &versioned, discardShadowRunConfig{}) {
		t.Fatal("advance settled prefix")
	}
	if got := retry.runner.worker.state.GetBalance(owner); got != 12_000_000 {
		t.Fatalf("advanced prefix balance = %d, want 12000000", got)
	}
	if retry.stats.prefixRefreshes != 1 || retry.stats.prefixReuses != 1 || retry.stats.prefixAdvances != 1 {
		t.Fatalf("reused prefix stats = %+v", retry.stats)
	}

	// Account-KV generation resets are intentionally outside the narrow
	// ordered applier. The reusable runner must refresh once from canonical
	// state instead of retaining a partially advanced prefix.
	live.AddBalance(owner, 3_000_000)
	versioned.transactionWritesOK[2] = true
	versioned.transactionWriteSets[2] = state.TransactionWriteSet{
		{Kind: state.TransactionAccessAccountKVGeneration, Address: owner}: {},
	}
	if !retry.ensureSettledPrefix(2, live, live.DynamicProperties(), &versioned, discardShadowRunConfig{}) {
		t.Fatal("refresh unsupported settled prefix")
	}
	if got := retry.runner.worker.state.GetBalance(owner); got != 15_000_000 {
		t.Fatalf("refreshed prefix balance = %d, want 15000000", got)
	}
	if retry.stats.prefixRefreshes != 2 || retry.stats.prefixReuses != 1 || retry.stats.prefixAdvances != 1 || retry.runner.settledThrough != 2 {
		t.Fatalf("refreshed prefix stats = %+v settled=%d", retry.stats, retry.runner.settledThrough)
	}
}

func TestProjectSenderRetryDeadline(t *testing.T) {
	versioned := &versionedAccessShadow{transactionDurations: []int64{100, 200, 300}}
	tests := []struct {
		name       string
		result     discardShadowTaskResult
		boundary   int
		ready      bool
		known      bool
		deadline   int64
		deltaNanos int64
	}{
		{
			name: "current transaction is necessarily late",
			result: discardShadowTaskResult{
				retryStartTx: 1, retryCompletionNanos: 50,
			},
			boundary: 1, known: true, deadline: 0, deltaNanos: 50,
		},
		{
			name: "future result reaches boundary",
			result: discardShadowTaskResult{
				retryStartTx: 0, retryCompletionNanos: 250,
			},
			boundary: 2, ready: true, known: true, deadline: 300, deltaNanos: 50,
		},
		{
			name: "future result misses boundary",
			result: discardShadowTaskResult{
				retryStartTx: 0, retryCompletionNanos: 350,
			},
			boundary: 2, known: true, deadline: 300, deltaNanos: 50,
		},
		{
			name: "missing canonical duration is unknown",
			result: discardShadowTaskResult{
				retryStartTx: 0, retryCompletionNanos: 50,
			},
			boundary: 4,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			ready, known, deadline, deltaNanos := projectSenderRetryDeadline(test.result, test.boundary, versioned)
			if ready != test.ready || known != test.known || deadline != test.deadline || deltaNanos != test.deltaNanos {
				t.Fatalf("projection = ready:%v known:%v deadline:%d delta:%d, want ready:%v known:%v deadline:%d delta:%d",
					ready, known, deadline, deltaNanos, test.ready, test.known, test.deadline, test.deltaNanos)
			}
		})
	}
}

func TestTransferSenderChainsFollowImmediateSenderPredecessor(t *testing.T) {
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1),
		makeTestTransferTx(3, 4, 1),
		makeTestTransferTx(1, 5, 1),
		makeTestTriggerTx(1, testProcessorAddr(9), nil),
		makeTestTransferTx(1, 6, 1),
	}
	chains := transferSenderChains(transactions)
	if len(chains) != 3 {
		t.Fatalf("sender chains = %v, want 3", chains)
	}
	var joined, afterUnsupported *discardShadowSenderChainTask
	for chainIndex := range chains {
		for taskIndex := range chains[chainIndex] {
			task := &chains[chainIndex][taskIndex]
			switch task.txIndex {
			case 2:
				joined = task
			case 4:
				afterUnsupported = task
			}
		}
	}
	if joined == nil || !joined.senderVersioned || joined.senderPredecessor != 0 {
		t.Fatalf("same-sender transfer was not chained: %+v", joined)
	}
	if afterUnsupported == nil || afterUnsupported.senderVersioned || afterUnsupported.senderPredecessor != 3 {
		t.Fatalf("chain did not break at non-transfer predecessor: %+v", afterUnsupported)
	}
}

func TestSenderChainPreexecutionRetainsSingleChainRetrySpare(t *testing.T) {
	canonical := newTestState(t)
	for _, id := range []byte{1, 2, 3} {
		canonical.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	canonical.AddBalance(testProcessorAddr(1), 10_000_000)
	if _, err := canonical.Commit(); err != nil {
		t.Fatal(err)
	}
	base, err := canonical.Copy()
	if err != nil {
		t.Fatal(err)
	}
	base.SetDynamicProperties(canonical.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(1, 3, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(discardShadowAsyncRetryOffset), Timestamp: 3_000}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	shadow := &discardShadowBlock{base: base, sampled: true}
	pre := shadow.preexecuteTransferSenderChainsWithRetryState(discardShadowRunConfig{
		block: block, transactions: transactions,
	}, true)
	if pre == nil || len(pre.retryStates) != 1 || pre.retryStates[0] == nil {
		t.Fatal("single-chain preexecution did not retain a retry spare")
	}
	if pre.retryStates[0] == shadow.base {
		t.Fatal("single-chain retry spare aliases the finish canary state")
	}
	if balance := pre.retryStates[0].GetBalance(testProcessorAddr(1)); balance != 10_000_000 {
		t.Fatalf("retry spare owner balance = %d, want block-start balance", balance)
	}
	retry := newDiscardShadowAsyncSenderRetry(pre, len(transactions))
	if retry == nil || len(retry.asyncRunners) != 1 || retry.asyncRunners[0].worker == nil || retry.stats.actualPrewarmed != 1 {
		t.Fatalf("prewarmed retry = %+v", retry)
	}
	if pre.retryStates != nil {
		t.Fatal("retry spare ownership was not transferred")
	}
}

func TestSenderChainPreexecutionRetainsIndependentRetryPool(t *testing.T) {
	canonical := newTestState(t)
	for id := byte(1); id <= 12; id++ {
		canonical.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	for _, owner := range []byte{1, 3, 5, 7} {
		canonical.AddBalance(testProcessorAddr(owner), 10_000_000)
	}
	if _, err := canonical.Commit(); err != nil {
		t.Fatal(err)
	}
	base, err := canonical.Copy()
	if err != nil {
		t.Fatal(err)
	}
	base.SetDynamicProperties(canonical.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000),
		makeTestTransferTx(3, 4, 1_000),
		makeTestTransferTx(5, 6, 1_000),
		makeTestTransferTx(7, 8, 1_000),
		makeTestTransferTx(1, 9, 2_000),
		makeTestTransferTx(3, 10, 2_000),
		makeTestTransferTx(5, 11, 2_000),
		makeTestTransferTx(7, 12, 2_000),
	}
	blockPB := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number: int64(discardShadowAsyncRetryOffset), Timestamp: 3_000,
	}}}
	for _, tx := range transactions {
		blockPB.Transactions = append(blockPB.Transactions, tx.Proto())
	}
	shadow := &discardShadowBlock{base: base, sampled: true}
	pre := shadow.preexecuteTransferSenderChainsWithRetryState(discardShadowRunConfig{
		block: types.NewBlockFromPB(blockPB), transactions: transactions,
	}, true)
	if pre == nil || len(pre.retryStates) != discardShadowWorkerCount-1 {
		t.Fatalf("prewarmed retry states = %d, want %d", len(pre.retryStates), discardShadowWorkerCount-1)
	}
	seen := make(map[*state.StateDB]struct{}, len(pre.retryStates))
	for _, retryState := range pre.retryStates {
		if retryState == nil || retryState == shadow.base {
			t.Fatal("retry pool contains missing or canonical finish state")
		}
		if _, duplicate := seen[retryState]; duplicate {
			t.Fatal("retry pool aliases a StateDB")
		}
		seen[retryState] = struct{}{}
		if balance := retryState.GetBalance(testProcessorAddr(1)); balance != 10_000_000 {
			t.Fatalf("retry pool state was not reverted: balance=%d", balance)
		}
	}
	retry := newDiscardShadowAsyncSenderRetry(pre, len(transactions))
	if retry == nil || len(retry.asyncRunners) != discardShadowWorkerCount-1 {
		t.Fatalf("async runner pool size = %d, want %d", len(retry.asyncRunners), discardShadowWorkerCount-1)
	}
	if retry.stats.actualPrewarmed != int64(len(retry.asyncRunners)) || retry.stats.actualCapacity != int64(len(retry.asyncRunners)) {
		t.Fatalf("runner pool stats = %+v", retry.stats)
	}
	for _, runner := range retry.asyncRunners {
		runner.busy = true
	}
	retry.asyncActive = len(retry.asyncRunners)
	if idle := retry.idleAsyncRunner(); idle != nil {
		t.Fatal("busy runner pool exposed an idle runner")
	}
	retry.consumeAsyncEvent(discardShadowAsyncRetryEvent{runner: retry.asyncRunners[1], done: true}, 0)
	if retry.asyncActive != len(retry.asyncRunners)-1 || retry.idleAsyncRunner() != retry.asyncRunners[1] {
		t.Fatal("returned runner ownership was not restored")
	}
}

func TestSenderChainPreexecutionForwardsTypedState(t *testing.T) {
	base := newTestState(t)
	owner := testProcessorAddr(1)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 10_000_000)
	for _, id := range []byte{2, 3} {
		base.CreateAccount(testProcessorAddr(id), corepb.AccountType_Normal)
	}
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	base.SetDynamicProperties(base.DynamicProperties().Copy())
	transactions := []*types.Transaction{
		makeTestTransferTx(1, 2, 1_000_000),
		makeTestTransferTx(1, 3, 2_000_000),
	}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowSampleInterval), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	shadow := &discardShadowBlock{base: base, sampled: true}
	pre := shadow.preexecuteTransferSenderChains(discardShadowRunConfig{
		block: block, transactions: transactions, retainInfos: true,
	})
	if pre == nil || pre.groups != 1 || len(pre.results) != 2 {
		t.Fatalf("sender-chain preexecution = %+v", pre)
	}
	second := pre.results[pre.resultByTx[1]]
	if second.err != nil || !second.senderVersioned || second.senderPredecessor != 0 {
		t.Fatalf("second sender-chain result = %+v", second)
	}
	balanceKey := state.TransactionAccessKey{
		Kind: state.TransactionAccessAccountField, Address: owner, AccountField: state.TransactionAccountFieldBalance,
	}
	balanceWrite, ok := second.writes[balanceKey]
	if !ok || len(balanceWrite.Value) != 8 || int64(binary.BigEndian.Uint64(balanceWrite.Value)) != 7_000_000 {
		t.Fatalf("forwarded owner balance = %+v", balanceWrite)
	}
	readVersioned := false
	for _, read := range second.reads.Reads {
		if read.Key == balanceKey && read.HasExpectedWriter && read.ExpectedWriter == 0 {
			readVersioned = true
			break
		}
	}
	if !readVersioned {
		t.Fatalf("second transfer did not retain owner balance version: %+v", second.reads)
	}
	if balance := base.GetBalance(owner); balance != 10_000_000 {
		t.Fatalf("sender-chain worker mutated base balance = %d", balance)
	}
}

func TestPreexecutionFreezesReadDecisionBeforeLaterWriter(t *testing.T) {
	owner := testProcessorAddr(1)
	key := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		Address:      owner,
		AccountField: state.TransactionAccountFieldBalance,
	}
	var versioned versionedAccessShadow
	versioned.Prepare(3)
	versioned.accountFieldVersions[state.TransactionAccountFieldKey{Address: owner, Field: state.TransactionAccountFieldBalance}] = 0
	pre := &discardShadowPreexecution{
		results: []discardShadowTaskResult{{
			txIndex: 1,
			reads: state.TransactionReadSet{Reads: []state.TransactionRead{{
				Key: key, Mode: state.TransactionAccessRead,
			}}},
		}},
		resultByTx:    []int{-1, 0, -1},
		readVersions:  make([]discardShadowReadVersionResult, 1),
		readValidated: make([]bool, 1),
	}
	pre.validateReadVersion(1, nil, &versioned)
	// Reproduce the production failure mode: a later transaction becomes the
	// latest writer for the same path. The decision for tx 1 must not be
	// recomputed from this lossy final map.
	versioned.accountFieldVersions[state.TransactionAccountFieldKey{Address: owner, Field: state.TransactionAccountFieldBalance}] = 2
	if !pre.readValidated[0] || pre.readVersions[0].publishable || !pre.readVersions[0].readConflict {
		t.Fatalf("frozen decision lost earlier writer: %+v", pre.readVersions[0])
	}
}

func TestSenderChainPublicationRequiresPublishedPredecessor(t *testing.T) {
	pre := &discardShadowPreexecution{
		results: []discardShadowTaskResult{
			{txIndex: 0},
			{txIndex: 1, senderVersioned: true, senderPredecessor: 0},
		},
		resultByTx:    []int{0, 1},
		readVersions:  []discardShadowReadVersionResult{{publishable: true}, {publishable: true}},
		readValidated: []bool{true, true},
		published:     make([]bool, 2),
	}
	if _, decision, found := pre.resultForTransaction(1); !found || decision.publishable || !decision.sender || !decision.predecessor {
		t.Fatalf("dependent result accepted before predecessor publication: found=%v decision=%+v", found, decision)
	}
	pre.markPublished(0)
	if _, decision, found := pre.resultForTransaction(1); !found || !decision.publishable || decision.sender || decision.predecessor {
		t.Fatalf("dependent result rejected after predecessor publication: found=%v decision=%+v", found, decision)
	}
}

func TestDiscardShadowBlockRunsIndependentTransfersConcurrently(t *testing.T) {
	canonical := newTestState(t)
	transactions := make([]*types.Transaction, discardShadowWorkerCount)
	for txIndex := range transactions {
		owner := testProcessorAddr(byte(txIndex + 1))
		recipient := testProcessorAddr(byte(txIndex + 11))
		canonical.CreateAccount(owner, corepb.AccountType_Normal)
		canonical.AddBalance(owner, 10_000_000)
		canonical.CreateAccount(recipient, corepb.AccountType_Normal)
		transactions[txIndex] = makeTestTransferTx(byte(txIndex+1), byte(txIndex+11), 1_000_000)
	}
	if _, err := canonical.Commit(); err != nil {
		t.Fatal(err)
	}
	base, err := canonical.Copy()
	if err != nil {
		t.Fatal(err)
	}
	base.SetDynamicProperties(canonical.DynamicProperties().Copy())

	blockPB := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number:    int64(discardShadowSampleInterval),
		Timestamp: 3_000,
	}}}
	canonicalInfos := make([]*corepb.TransactionInfo, len(transactions))
	for txIndex, tx := range transactions {
		blockPB.Transactions = append(blockPB.Transactions, tx.Proto())
		result, applyErr := applyTransaction(
			canonical, canonical.DynamicProperties(), tx, 0, true, 0, 3_000,
			discardShadowSampleInterval, nil, nil, 0, [32]byte{}, [21]byte{},
			true, false, true, forks.NewVersionPassCache(), nil,
		)
		if applyErr != nil {
			t.Fatal(applyErr)
		}
		canonicalInfos[txIndex] = buildTransactionInfo(tx, result, discardShadowSampleInterval, 3_000, canonical.DynamicProperties().AllowTransactionFeePool())
	}
	block := types.NewBlockFromPB(blockPB)
	var versioned versionedAccessShadow
	versioned.Prepare(len(transactions))
	for txIndex := range versioned.transactionSupported {
		versioned.transactionSupported[txIndex] = true
	}
	stats := (&discardShadowBlock{base: base}).run(&versioned, discardShadowRunConfig{
		block:            block,
		transactions:     transactions,
		canonicalInfos:   canonicalInfos,
		genesisTimestamp: 0,
	})
	if stats.candidates != discardShadowWorkerCount || stats.executed != discardShadowWorkerCount || stats.matches != discardShadowWorkerCount || stats.mismatches != 0 || stats.errors != 0 {
		t.Fatalf("discard shadow stats = %+v", stats)
	}
	for txIndex := range transactions {
		if got := base.GetBalance(testProcessorAddr(byte(txIndex + 1))); got != 10_000_000 {
			t.Fatalf("base owner %d balance = %d", txIndex, got)
		}
		if got := base.GetBalance(testProcessorAddr(byte(txIndex + 11))); got != 0 {
			t.Fatalf("base recipient %d balance = %d", txIndex, got)
		}
	}
}
