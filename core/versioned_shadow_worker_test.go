package core

import (
	"container/heap"
	"encoding/binary"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

func TestDiscardShadowAsyncRetryCohorts(t *testing.T) {
	tests := []struct {
		blockNum uint64
		want     bool
	}{
		{blockNum: 0, want: false},
		{blockNum: 1, want: false},
		{blockNum: 64, want: true},
		{blockNum: 128, want: true},
		{blockNum: 192, want: true},
		{blockNum: 256, want: false},
		{blockNum: 320, want: true},
	}
	for _, test := range tests {
		if got := useDiscardShadowAsyncRetry(test.blockNum); got != test.want {
			t.Fatalf("block %d async retry cohort = %t, want %t", test.blockNum, got, test.want)
		}
	}
}

func TestDiscardShadowAsyncRetryPublicationCohort(t *testing.T) {
	tests := []struct {
		blockNum uint64
		want     bool
	}{
		{blockNum: 0, want: false},
		{blockNum: 64, want: false},
		{blockNum: 128, want: false},
		{blockNum: 192, want: true},
		{blockNum: 256, want: false},
		{blockNum: 448, want: true},
	}
	for _, test := range tests {
		if got := useDiscardShadowAsyncRetryPublication(test.blockNum); got != test.want {
			t.Fatalf("block %d async retry publication cohort = %t, want %t", test.blockNum, got, test.want)
		}
	}
}

func TestVMSenderRetryObservationCohort(t *testing.T) {
	tests := []struct {
		blockNum uint64
		want     bool
	}{
		{blockNum: 0, want: false},
		{blockNum: 64, want: false},
		{blockNum: 192, want: false},
		{blockNum: 256, want: true},
		{blockNum: 512, want: true},
		{blockNum: 1024, want: true},
		{blockNum: 1088, want: false},
	}
	for _, test := range tests {
		if got := useVMSenderRetryObservation(test.blockNum); got != test.want {
			t.Fatalf("block %d VM retry observation cohort = %t, want %t", test.blockNum, got, test.want)
		}
	}
}

func TestVMSenderChainPublicationCohort(t *testing.T) {
	tests := []struct {
		blockNum uint64
		want     bool
	}{
		{blockNum: 0, want: false},
		{blockNum: 64, want: false},
		{blockNum: 1_024, want: true},
		{blockNum: 1_088, want: false},
		{blockNum: 2_048, want: true},
	}
	for _, test := range tests {
		if got := useVMSenderChainPublication(test.blockNum); got != test.want {
			t.Fatalf("block %d VM publication cohort = %t, want %t", test.blockNum, got, test.want)
		}
	}
}

func TestDiscardShadowRetryWriteCaptureProjectsReadHierarchy(t *testing.T) {
	fieldAddress := testProcessorAddr(1)
	fullAddress := testProcessorAddr(2)
	ignoredAddress := testProcessorAddr(3)
	dynamicKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "energy_fee"}
	fieldKey := state.TransactionAccessKey{
		Kind: state.TransactionAccessAccountField, Address: fieldAddress,
		AccountField: state.TransactionAccountFieldBalance,
	}
	fullKey := state.TransactionAccessKey{Kind: state.TransactionAccessAccount, Address: fullAddress}
	source := &discardShadowPreexecution{
		results: []discardShadowTaskResult{
			{txIndex: 1, reads: state.TransactionReadSet{Reads: []state.TransactionRead{
				{Key: fieldKey, Mode: state.TransactionAccessRead},
				{Key: fullKey, Mode: state.TransactionAccessRead},
				{Key: dynamicKey, Mode: state.TransactionAccessCommutativeRead},
			}}},
			{txIndex: 2, reads: state.TransactionReadSet{Reads: []state.TransactionRead{{
				Key:  state.TransactionAccessKey{Kind: state.TransactionAccessAccount, Address: ignoredAddress},
				Mode: state.TransactionAccessRead,
			}}}},
			{txIndex: 3, senderVersioned: true},
		},
		senderNext: []int{-1, 3, -1, -1},
	}
	include, fullTransactions, recorderOnly := newDiscardShadowRetryWriteCapture(source, 4)
	if include == nil {
		t.Fatal("retry write capture filter is nil")
	}
	if !recorderOnly {
		t.Fatal("account-only retry projection should use recorder fast path")
	}
	if !fullTransactions[1] || fullTransactions[2] || !fullTransactions[3] {
		t.Fatalf("full capture transactions = %v, want tx 1 and 3", fullTransactions)
	}
	tests := []struct {
		key  state.TransactionAccessKey
		want bool
	}{
		{key: fieldKey, want: true},
		{key: state.TransactionAccessKey{Kind: state.TransactionAccessAccount, Address: fieldAddress}, want: true},
		{key: state.TransactionAccessKey{Kind: state.TransactionAccessAccountField, Address: fieldAddress, AccountField: state.TransactionAccountFieldAllowance}, want: false},
		{key: fullKey, want: true},
		{key: state.TransactionAccessKey{Kind: state.TransactionAccessAccountField, Address: fullAddress, AccountField: state.TransactionAccountFieldAllowance}, want: true},
		{key: dynamicKey, want: false},
		{key: state.TransactionAccessKey{Kind: state.TransactionAccessAccount, Address: ignoredAddress}, want: false},
	}
	for _, test := range tests {
		if got := include(test.key); got != test.want {
			t.Fatalf("include(%+v) = %t, want %t", test.key, got, test.want)
		}
	}
}

func TestDiscardShadowRetryWriteCaptureUsesRecorderForStorageReads(t *testing.T) {
	source := &discardShadowPreexecution{
		results: []discardShadowTaskResult{{
			txIndex:         0,
			senderVersioned: true,
			reads: state.TransactionReadSet{Reads: []state.TransactionRead{{
				Key: state.TransactionAccessKey{
					Kind:       state.TransactionAccessStorage,
					Address:    testProcessorAddr(4),
					StorageKey: tcommon.Hash{31: 1},
				},
				Mode: state.TransactionAccessRead,
			}}},
		}},
		senderNext: []int{-1},
	}
	include, _, recorderOnly := newDiscardShadowRetryWriteCapture(source, 1)
	if include == nil {
		t.Fatal("retry write capture filter is nil")
	}
	if !recorderOnly {
		t.Fatal("storage projection should use the complete recorder path")
	}
}

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
	if retry.stats.actualExecutionErrs != 1 {
		t.Fatalf("async failure classification = %+v", retry.stats)
	}
}

func TestAsyncRetryResultErrorClassification(t *testing.T) {
	tests := []struct {
		name   string
		result discardShadowTaskResult
		check  func(discardShadowSenderRetryStats) bool
	}{
		{
			name: "input", result: discardShadowTaskResult{err: errors.New("input"), errorStage: discardShadowTaskErrorInput},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualInputErrors == 1 },
		},
		{
			name: "execution", result: discardShadowTaskResult{err: errors.New("execution"), errorStage: discardShadowTaskErrorExecution},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualExecutionErrs == 1 },
		},
		{
			name: "contract ret", result: discardShadowTaskResult{err: errors.New("contract ret"), errorStage: discardShadowTaskErrorContractRet},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualContractErrs == 1 },
		},
		{
			name: "forward", result: discardShadowTaskResult{err: errors.New("forward"), errorStage: discardShadowTaskErrorForward},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualForwardErrors == 1 },
		},
		{
			name: "missing info", result: discardShadowTaskResult{},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualMissingInfo == 1 },
		},
		{
			name: "write set", result: discardShadowTaskResult{info: new(corepb.TransactionInfo), writeSetErr: errors.New("writes")},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualWriteSetErrs == 1 },
		},
		{
			name: "apply unsupported", result: discardShadowTaskResult{info: new(corepb.TransactionInfo)},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualApplyRejects == 1 },
		},
		{
			name: "apply error", result: discardShadowTaskResult{info: new(corepb.TransactionInfo), applyEligible: true, applyErr: errors.New("apply")},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualApplyErrors == 1 },
		},
		{
			name: "apply mismatch", result: discardShadowTaskResult{info: new(corepb.TransactionInfo), applyEligible: true},
			check: func(stats discardShadowSenderRetryStats) bool { return stats.actualApplyMismatch == 1 },
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retry := new(discardShadowSenderRetry)
			retry.recordAsyncResultError(&test.result)
			if !test.check(retry.stats) {
				t.Fatalf("classification stats = %+v", retry.stats)
			}
		})
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
	tasks, _ := retry.invalidateAsyncSuffix(0, discardShadowRetryLookahead)
	if len(tasks) != 1 {
		t.Fatalf("reserved tasks = %d, want 1", len(tasks))
	}
	retry.asyncScheduled += int64(len(tasks))
	if tasks, _ := retry.invalidateAsyncSuffix(1, discardShadowRetryLookahead); len(tasks) != 0 {
		t.Fatalf("tasks exceeded global execution reservation: %d", len(tasks))
	}
}

func TestAsyncRetryPrioritizesBoundedSenderLookahead(t *testing.T) {
	const transactionCount = 7
	source := &discardShadowPreexecution{
		senderTasks:  make([]discardShadowSenderChainTask, transactionCount),
		senderTaskOK: make([]bool, transactionCount),
		senderNext:   make([]int, transactionCount),
	}
	for txIndex := 0; txIndex < transactionCount; txIndex++ {
		source.senderTasks[txIndex] = discardShadowSenderChainTask{txIndex: txIndex}
		source.senderTaskOK[txIndex] = true
		source.senderNext[txIndex] = txIndex + 1
	}
	source.senderNext[transactionCount-1] = -1
	retry := &discardShadowSenderRetry{
		source:             source,
		available:          make([]bool, transactionCount),
		selectedOK:         make([]bool, transactionCount),
		selectedAsyncReady: make([]bool, transactionCount),
		incarnations:       make([]uint32, transactionCount),
	}
	tasks, deferred := retry.invalidateAsyncSuffix(0, discardShadowRetryLookahead)
	if len(tasks) != int(discardShadowRetryLookahead) || deferred != transactionCount-int64(len(tasks)) {
		t.Fatalf("lookahead tasks=%d deferred=%d", len(tasks), deferred)
	}
	for txIndex := range retry.incarnations {
		if retry.incarnations[txIndex] != 1 {
			t.Fatalf("tx %d incarnation = %d, want 1", txIndex, retry.incarnations[txIndex])
		}
	}
	for taskIndex, task := range tasks {
		if task.txIndex != taskIndex {
			t.Fatalf("priority task %d tx = %d", taskIndex, task.txIndex)
		}
	}
}

func TestAsyncRetryCancelsSupersededSuffixBeforeExecution(t *testing.T) {
	const transactionCount = 7
	source := &discardShadowPreexecution{
		senderTasks:  make([]discardShadowSenderChainTask, transactionCount),
		senderTaskOK: make([]bool, transactionCount),
		senderNext:   make([]int, transactionCount),
	}
	for txIndex := 0; txIndex < transactionCount; txIndex++ {
		source.senderTasks[txIndex] = discardShadowSenderChainTask{txIndex: txIndex}
		source.senderTaskOK[txIndex] = true
		source.senderNext[txIndex] = txIndex + 1
	}
	source.senderNext[transactionCount-1] = -1
	retry := newDiscardShadowAsyncSenderRetry(source, transactionCount)
	if retry == nil {
		t.Fatal("missing async retry")
	}
	tasks, _ := retry.invalidateAsyncSuffix(0, discardShadowRetryLookahead)
	if len(tasks) != int(discardShadowRetryLookahead) {
		t.Fatalf("initial tasks = %d", len(tasks))
	}
	for _, task := range tasks {
		if !retry.asyncTaskCurrent(task) {
			t.Fatalf("initial task %d was already superseded", task.txIndex)
		}
	}
	if invalidated, _ := retry.invalidateAsyncSuffix(1, 0); len(invalidated) != 0 {
		t.Fatalf("invalidation unexpectedly scheduled %d tasks", len(invalidated))
	}
	if !retry.asyncTaskCurrent(tasks[0]) {
		t.Fatal("prefix task was superseded by descendant invalidation")
	}
	for _, task := range tasks[1:] {
		if retry.asyncTaskCurrent(task) {
			t.Fatalf("descendant task %d remained current", task.txIndex)
		}
	}
	retry.asyncScheduled = int64(len(tasks))
	retry.consumeAsyncEvent(discardShadowAsyncRetryEvent{done: true, superseded: int64(len(tasks) - 1)}, 0)
	if retry.asyncScheduled != 1 || retry.stats.actualSuperseded != int64(len(tasks)-1) {
		t.Fatalf("reservation=%d superseded=%d", retry.asyncScheduled, retry.stats.actualSuperseded)
	}
}

func TestAsyncRetryQueueOrdersLowestTransactionFirst(t *testing.T) {
	queue := new(discardShadowAsyncRetryQueue)
	for _, txIndex := range []int{11, 3, 7} {
		heap.Push(queue, &discardShadowAsyncRetryRequest{
			txIndex: txIndex,
			tasks:   []discardShadowAsyncRetryTask{{txIndex: txIndex, incarnation: 1}},
		})
	}
	for _, want := range []int{3, 7, 11} {
		request := heap.Pop(queue).(*discardShadowAsyncRetryRequest)
		if request.txIndex != want {
			t.Fatalf("queue tx = %d, want %d", request.txIndex, want)
		}
	}
}

func TestAsyncRetryQueueFreezesBusyBoundaryInputs(t *testing.T) {
	parent := ethrawdb.NewMemoryDatabase()
	if err := parent.Put([]byte("stable"), []byte("at-boundary")); err != nil {
		t.Fatal(err)
	}
	source := &discardShadowPreexecution{
		results: []discardShadowTaskResult{{reads: state.TransactionReadSet{Reads: []state.TransactionRead{{
			Key: state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "stable"}, Mode: state.TransactionAccessRead,
		}}}}},
		resultByTx:   []int{0, -1},
		senderTasks:  []discardShadowSenderChainTask{{txIndex: 0}, {txIndex: 1}},
		senderTaskOK: []bool{true, true},
		senderNext:   []int{1, -1},
	}
	retry := newDiscardShadowAsyncSenderRetry(source, 2)
	if retry == nil {
		t.Fatal("missing async retry")
	}
	for _, runner := range retry.asyncRunners {
		runner.busy = true
	}
	retry.asyncActive = len(retry.asyncRunners)
	canonical := newTestState(t)
	var versioned versionedAccessShadow
	versioned.Prepare(2)
	retryCfg := discardShadowRunConfig{
		db: parent, transactions: []*types.Transaction{nil, nil},
	}
	retry.enqueueAsyncRetry(0, canonical, canonical.DynamicProperties(), &versioned, retryCfg)
	if retry.asyncQueue.Len() != 1 || retry.stats.actualQueueBusy != 1 || retry.stats.actualJobs != 0 {
		t.Fatalf("queued busy retry = queue:%d stats:%+v", retry.asyncQueue.Len(), retry.stats)
	}
	if err := parent.Put([]byte("stable"), []byte("later")); err != nil {
		t.Fatal(err)
	}
	request := retry.asyncQueue[0]
	if got, err := request.frozenRaw.Get([]byte("stable")); err != nil || string(got) != "at-boundary" {
		t.Fatalf("queued frozen value = %q, %v", got, err)
	}
	if request.dynProps == nil || retry.asyncScheduled != 2 || retry.stats.actualQueueMaxDepth != 1 {
		t.Fatalf("queued request snapshot/reservation = %+v stats:%+v", request, retry.stats)
	}
	if tasks, _ := retry.invalidateAsyncSuffix(1, 0); len(tasks) != 0 {
		t.Fatalf("descendant invalidation scheduled %d tasks", len(tasks))
	}
	retry.dispatchAsyncRetryQueue(1, &versioned, retryCfg)
	if retry.asyncQueue.Len() != 0 || retry.asyncScheduled != 0 || retry.stats.actualQueueDropped != 2 || retry.stats.actualSuperseded != 1 {
		t.Fatalf("queue cleanup = queue:%d scheduled:%d stats:%+v", retry.asyncQueue.Len(), retry.asyncScheduled, retry.stats)
	}
}

func TestAsyncRetryQueueDispatchesAfterRunnerReturns(t *testing.T) {
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
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowAsyncRetryFirstOffset), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{transactions[0].Proto(), transactions[1].Proto()},
	})
	retryCfg := discardShadowRunConfig{block: block, transactions: transactions}
	shadow := &discardShadowBlock{base: base, sampled: true}
	pre := shadow.preexecuteTransferSenderChainsWithRetryState(retryCfg, true)
	retry := newDiscardShadowAsyncSenderRetry(pre, len(transactions))
	if retry == nil || len(retry.asyncRunners) != 1 {
		t.Fatalf("async retry pool = %+v", retry)
	}
	runner := retry.asyncRunners[0]
	runner.busy = true
	retry.asyncActive = 1
	var versioned versionedAccessShadow
	versioned.Prepare(len(transactions))
	retry.enqueueAsyncRetry(0, canonical, canonical.DynamicProperties(), &versioned, retryCfg)
	if retry.asyncQueue.Len() != 1 || retry.stats.actualQueueBusy != 1 || retry.stats.actualJobs != 0 {
		t.Fatalf("busy enqueue = queue:%d stats:%+v", retry.asyncQueue.Len(), retry.stats)
	}
	runner.busy = false
	retry.asyncActive = 0
	retry.dispatchAsyncRetryQueue(0, &versioned, retryCfg)
	if retry.asyncQueue.Len() != 0 || retry.stats.actualQueueDequeued != 1 || retry.stats.actualJobs != 1 {
		t.Fatalf("returned-runner dispatch = queue:%d stats:%+v", retry.asyncQueue.Len(), retry.stats)
	}
	retry.drainAsyncEvents(len(transactions), true)
	if retry.asyncActive != 0 || retry.stats.actualExecuted != 2 || retry.stats.actualErrors != 0 {
		t.Fatalf("queued execution = active:%d stats:%+v", retry.asyncActive, retry.stats)
	}
	if balance := canonical.GetBalance(testProcessorAddr(1)); balance != 10_000_000 {
		t.Fatalf("queued shadow mutated canonical balance: %d", balance)
	}
}

func TestAsyncRetryWorkerReportsPrefixFailure(t *testing.T) {
	runner := newDiscardShadowRetryRunner(newTestState(t))
	retry := &discardShadowSenderRetry{
		async:          true,
		asyncRunners:   []*discardShadowRetryRunner{runner},
		asyncEvents:    make(chan discardShadowAsyncRetryEvent, 2),
		asyncScheduled: 1,
	}
	request := &discardShadowAsyncRetryRequest{
		txIndex:   1,
		tasks:     []discardShadowAsyncRetryTask{{txIndex: 1, incarnation: 1}},
		frozenRaw: &discardShadowFrozenKV{values: make(map[string][]byte), present: make(map[string]bool)},
		dynProps:  state.NewDynamicProperties(),
	}
	var versioned versionedAccessShadow
	versioned.Prepare(2)
	retry.launchAsyncRetryRequest(runner, request, &versioned, discardShadowRunConfig{
		transactions: []*types.Transaction{nil, nil},
	})
	retry.drainAsyncEvents(2, true)
	if retry.asyncActive != 0 || runner.busy {
		t.Fatalf("failed prefix retained runner ownership: active=%d busy=%t", retry.asyncActive, runner.busy)
	}
	if retry.asyncScheduled != 0 || retry.stats.actualQueueDropped != 1 || retry.stats.actualExecuted != 0 {
		t.Fatalf("failed prefix reservation = scheduled:%d stats:%+v", retry.asyncScheduled, retry.stats)
	}
	if retry.stats.sharedStateJobs != 1 || retry.stats.sharedStateErrors != 1 ||
		retry.stats.actualErrors != 1 || retry.stats.actualInputErrors != 1 || retry.stats.errors != 1 {
		t.Fatalf("failed shared-state metrics = %+v", retry.stats)
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

func TestSharedVersionValuesUseStrictFloorAndSkipDeltas(t *testing.T) {
	key := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		Address:      testProcessorAddr(1),
		AccountField: state.TransactionAccountFieldBalance,
	}
	deltaKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "burn_trx_amount"}
	values := newTransactionVersionedValues(1)
	values.install(1, state.TransactionWriteSet{
		key:      {Exists: true, Value: []byte("one")},
		deltaKey: {Exists: true, Commutative: true, Value: make([]byte, 8)},
	})
	values.install(4, state.TransactionWriteSet{
		key: {Exists: true, Value: []byte("four")},
	})
	if _, _, ok := values.read(key, 1); ok {
		t.Fatal("floor read included writer at the same transaction index")
	}
	if value, writer, ok := values.read(key, 4); !ok || writer != 1 || string(value.Value) != "one" {
		t.Fatalf("floor before tx4 = value:%q writer:%d ok:%v", value.Value, writer, ok)
	}
	if value, writer, ok := values.read(key, 5); !ok || writer != 4 || string(value.Value) != "four" {
		t.Fatalf("floor before tx5 = value:%q writer:%d ok:%v", value.Value, writer, ok)
	}
	if _, _, ok := values.read(deltaKey, 5); ok {
		t.Fatal("commutative delta was exposed as an absolute shared value")
	}
	stats := values.stats()
	if stats.versions != 2 || stats.cells != 1 || stats.commutativeSkipped != 1 || stats.reads != 4 || stats.hits != 2 || stats.misses != 2 {
		t.Fatalf("shared version stats = %+v", stats)
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

func TestVMSenderChainsFollowImmediateSenderPredecessor(t *testing.T) {
	transactions := []*types.Transaction{
		makeTestTriggerTx(1, testProcessorAddr(8), nil),
		makeTestTriggerTx(3, testProcessorAddr(8), nil),
		makeTestTriggerTx(1, testProcessorAddr(8), nil),
		makeTestTransferTx(1, 2, 1),
		makeTestTriggerTx(1, testProcessorAddr(8), nil),
	}
	chains := vmSenderChains(transactions)
	if len(chains) != 3 {
		t.Fatalf("VM sender chains = %v, want 3", chains)
	}
	var joined, afterTransfer *discardShadowSenderChainTask
	for chainIndex := range chains {
		for taskIndex := range chains[chainIndex] {
			task := &chains[chainIndex][taskIndex]
			switch task.txIndex {
			case 2:
				joined = task
			case 4:
				afterTransfer = task
			}
		}
	}
	if joined == nil || !joined.senderVersioned || joined.senderPredecessor != 0 {
		t.Fatalf("same-sender VM transaction was not chained: %+v", joined)
	}
	if afterTransfer == nil || afterTransfer.senderVersioned || afterTransfer.senderPredecessor != 3 {
		t.Fatalf("VM chain did not break at transfer predecessor: %+v", afterTransfer)
	}
}

func TestVMSenderChainReadinessAllowsEnergyReceipt(t *testing.T) {
	result := &discardShadowTaskResult{
		info:          &corepb.TransactionInfo{Receipt: &corepb.ResourceReceipt{EnergyUsage: 1}},
		writes:        state.TransactionWriteSet{},
		applyEligible: true,
		applyMatch:    true,
	}
	if !preexecutedResultReady(result) {
		t.Fatal("generic sender-chain readiness rejected an energy-bearing VM result")
	}
	if preexecutedTransferReady(result) {
		t.Fatal("transfer publication readiness accepted an energy-bearing result")
	}
}

func TestVMSenderChainFinishValidatesEnergyResult(t *testing.T) {
	tx := makeTestTriggerTx(1, testProcessorAddr(8), nil)
	info := &corepb.TransactionInfo{Receipt: &corepb.ResourceReceipt{EnergyUsage: 1}}
	writes := state.TransactionWriteSet{
		{Kind: state.TransactionAccessRawKV, LogicalKey: "vm-result"}: {Exists: true, Value: []byte("ok")},
	}
	pre := &discardShadowPreexecution{
		results: []discardShadowTaskResult{{
			txIndex: 0, info: info, writes: writes, applyEligible: true, applyMatch: true,
		}},
		resultByTx:    []int{0},
		readVersions:  []discardShadowReadVersionResult{{publishable: true}},
		readValidated: []bool{true},
		published:     make([]bool, 1),
		groups:        1,
	}
	versioned := &versionedAccessShadow{
		transactionWritesOK: []bool{true},
		transactionWriteSets: []state.TransactionWriteSet{
			writes,
		},
	}
	stats := (&discardShadowBlock{}).finishVMSenderChains(pre, versioned, discardShadowRunConfig{
		transactions:   []*types.Transaction{tx},
		canonicalInfos: []*corepb.TransactionInfo{info},
	})
	if stats.candidates != 1 || stats.validated != 1 || stats.errors != 0 || !pre.published[0] {
		t.Fatalf("VM sender-chain finish = %+v, published=%v", stats, pre.published)
	}
}

func TestVMSenderChainFinishClassifiesWriteMismatch(t *testing.T) {
	publicNetKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_usage"}
	rawKey := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "vm-result"}
	encodeInt := func(value uint64) state.TransactionWriteValue {
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, value)
		return state.TransactionWriteValue{Exists: true, Value: encoded}
	}
	tests := []struct {
		name            string
		workerWrites    state.TransactionWriteSet
		canonicalWrites state.TransactionWriteSet
		publicNetValid  bool
		publicNetOnly   int64
		other           int64
	}{
		{
			name: "public bandwidth only",
			workerWrites: state.TransactionWriteSet{
				publicNetKey: encodeInt(10), rawKey: {Exists: true, Value: []byte("same")},
			},
			canonicalWrites: state.TransactionWriteSet{
				publicNetKey: encodeInt(20), rawKey: {Exists: true, Value: []byte("same")},
			},
			publicNetValid: true,
			publicNetOnly:  1,
		},
		{
			name: "other state",
			workerWrites: state.TransactionWriteSet{
				rawKey: {Exists: true, Value: []byte("worker")},
			},
			canonicalWrites: state.TransactionWriteSet{
				rawKey: {Exists: true, Value: []byte("canonical")},
			},
			other: 1,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			info := &corepb.TransactionInfo{Receipt: &corepb.ResourceReceipt{EnergyUsage: 1}}
			pre := &discardShadowPreexecution{
				results: []discardShadowTaskResult{{
					txIndex: 0, info: info, writes: test.workerWrites, applyEligible: true, applyMatch: true,
					publicNetValid: test.publicNetValid,
				}},
				resultByTx:    []int{0},
				readVersions:  []discardShadowReadVersionResult{{publishable: true}},
				readValidated: []bool{true},
				published:     make([]bool, 1),
				groups:        1,
			}
			versioned := &versionedAccessShadow{
				transactionWritesOK:  []bool{true},
				transactionWriteSets: []state.TransactionWriteSet{test.canonicalWrites},
			}
			stats := (&discardShadowBlock{}).finishVMSenderChains(pre, versioned, discardShadowRunConfig{
				transactions:   []*types.Transaction{makeTestTriggerTx(1, testProcessorAddr(8), nil)},
				canonicalInfos: []*corepb.TransactionInfo{info},
			})
			if stats.candidates != 1 || stats.validated != 0 || stats.writeMismatches != 1 ||
				stats.publicNetOnly != test.publicNetOnly || stats.otherWriteMismatch != test.other {
				t.Fatalf("VM write mismatch classification = %+v", stats)
			}
		})
	}
}

func TestVMPublicNetBoundaryProjectionMatchesSerialWrites(t *testing.T) {
	usageKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_usage"}
	timeKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_time"}
	rawKey := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "vm-result"}
	encodeInt := func(value int64) state.TransactionWriteValue {
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, uint64(value))
		return state.TransactionWriteValue{Exists: true, Value: encoded}
	}
	resultWrites := state.TransactionWriteSet{
		usageKey: encodeInt(150), timeKey: encodeInt(10), rawKey: {Exists: true, Value: []byte("same")},
	}
	pre := &discardShadowPreexecution{
		results: []discardShadowTaskResult{{
			txIndex: 0, info: &corepb.TransactionInfo{}, writes: resultWrites, applyEligible: true, applyMatch: true,
			publicNetValid: true,
			publicNet: state.PublicNetReservation{
				StartUsage: 50, StartTime: 5, RecoveredUsage: 50, ResourceTime: 10, Delta: 100, Limit: 1_000,
			},
		}},
		resultByTx:    []int{0},
		readVersions:  []discardShadowReadVersionResult{{publishable: true}},
		readValidated: []bool{true},
		publicNet:     make([]discardShadowPublicNetProjection, 1),
	}
	dynProps := state.NewDynamicProperties()
	dynProps.SetPublicNetLimit(1_000)
	dynProps.SetPublicNetUsage(200)
	dynProps.SetPublicNetTime(10)
	pre.projectPublicNetBoundary(0, dynProps)
	projection := pre.publicNet[0]
	if !projection.observed || !projection.admitted || !projection.rebased || projection.expectedUsage != 300 || projection.expectedTimeSet {
		t.Fatalf("VM public-net projection = %+v", projection)
	}
	canonical := state.TransactionWriteSet{
		usageKey: encodeInt(300), rawKey: {Exists: true, Value: []byte("same")},
	}
	if !equalProjectedPublicNetWriteSet(resultWrites, canonical, projection) {
		t.Fatal("projected public-net write set did not match serial no-op timestamp")
	}
	canonical[timeKey] = encodeInt(10)
	if equalProjectedPublicNetWriteSet(resultWrites, canonical, projection) {
		t.Fatal("projection accepted a serial timestamp write that should be absent")
	}
	dynProps.SetPublicNetTime(9)
	pre.publicNet[0] = discardShadowPublicNetProjection{}
	pre.projectPublicNetBoundary(0, dynProps)
	projection = pre.publicNet[0]
	if !projection.admitted || !projection.expectedTimeSet || projection.expectedTime != 10 {
		t.Fatalf("timestamp-changing projection = %+v", projection)
	}
	canonical = state.TransactionWriteSet{
		usageKey: encodeInt(projection.expectedUsage), timeKey: encodeInt(10), rawKey: {Exists: true, Value: []byte("same")},
	}
	if !equalProjectedPublicNetWriteSet(resultWrites, canonical, projection) {
		t.Fatal("projected public-net write set did not match serial timestamp update")
	}

	dynProps.SetPublicNetLimit(999)
	pre.publicNet[0] = discardShadowPublicNetProjection{}
	pre.projectPublicNetBoundary(0, dynProps)
	if rejected := pre.publicNet[0]; !rejected.observed || rejected.admitted {
		t.Fatalf("limit change was not rejected: %+v", rejected)
	}
}

func TestSenderChainWriteMismatchMasks(t *testing.T) {
	balanceKey := state.TransactionAccessKey{
		Kind: state.TransactionAccessAccountField, Address: testProcessorAddr(1),
		AccountField: state.TransactionAccountFieldBalance,
	}
	publicNetKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_usage"}
	rawKey := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "raw"}
	result := discardShadowTaskResult{writes: state.TransactionWriteSet{
		balanceKey:   {Exists: true, Value: []byte{1}},
		publicNetKey: {Exists: true, Value: []byte{2}},
	}}
	canonical := state.TransactionWriteSet{
		balanceKey: {Exists: true, Value: []byte{3}},
		rawKey:     {Exists: true, Value: []byte{4}},
	}
	kinds, fields, shape := senderChainWriteMismatchMasks(result, canonical)
	wantKinds := int64(1)<<state.TransactionAccessAccountField |
		int64(1)<<state.TransactionAccessDynamicInt |
		int64(1)<<state.TransactionAccessRawKV
	if kinds != wantKinds {
		t.Fatalf("mismatch kind mask = %#x, want %#x", kinds, wantKinds)
	}
	if want := int64(1) << state.TransactionAccountFieldBalance; fields != want {
		t.Fatalf("mismatch account-field mask = %#x, want %#x", fields, want)
	}
	wantShape := senderChainMismatchMissing | senderChainMismatchExtra | senderChainMismatchValue
	if shape != wantShape {
		t.Fatalf("mismatch shape mask = %#x, want %#x", shape, wantShape)
	}
}

func TestPublicNetWriteOverrideMatchesSerialWritePresence(t *testing.T) {
	usageKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_usage"}
	timeKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_time"}
	encodeInt := func(value int64) state.TransactionWriteValue {
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, uint64(value))
		return state.TransactionWriteValue{Exists: true, Value: encoded}
	}
	dynProps := state.NewDynamicProperties()
	dynProps.SetPublicNetLimit(1_000)
	dynProps.SetPublicNetUsage(200)
	dynProps.SetPublicNetTime(10)
	result := &discardShadowTaskResult{
		writes: state.TransactionWriteSet{
			usageKey: encodeInt(150),
			timeKey:  encodeInt(10),
		},
		publicNetValid: true,
		publicNet: state.PublicNetReservation{
			StartUsage: 50, StartTime: 5, RecoveredUsage: 50, ResourceTime: 10, Delta: 100, Limit: 1_000,
		},
	}
	override, admitted := overridePublicNetReservation(result, dynProps)
	if !admitted || !override.rebased {
		t.Fatalf("public-net no-op-time override admitted=%t rebased=%t", admitted, override.rebased)
	}
	usage, ok := publicNetReservationWriteValue(result.writes, "public_net_usage")
	if !ok || usage != 300 {
		t.Fatalf("ordered public-net usage = %d present=%t, want 300/true", usage, ok)
	}
	if _, timeWritten := result.writes[timeKey]; timeWritten {
		t.Fatal("ordered public-net override retained a serial no-op time write")
	}
	override.restore()
	if usage, ok := publicNetReservationWriteValue(result.writes, "public_net_usage"); !ok || usage != 150 {
		t.Fatalf("restored public-net usage = %d present=%t, want 150/true", usage, ok)
	}
	if resourceTime, ok := publicNetReservationWriteValue(result.writes, "public_net_time"); !ok || resourceTime != 10 {
		t.Fatalf("restored public-net time = %d present=%t, want 10/true", resourceTime, ok)
	}

	delete(result.writes, timeKey)
	result.publicNet.StartUsage = 200
	result.publicNet.StartTime = 10
	result.publicNet.RecoveredUsage = 200
	dynProps.SetPublicNetTime(9)
	override, admitted = overridePublicNetReservation(result, dynProps)
	if !admitted {
		t.Fatal("public-net time-changing override was rejected")
	}
	if resourceTime, ok := publicNetReservationWriteValue(result.writes, "public_net_time"); !ok || resourceTime != 10 {
		t.Fatalf("ordered public-net time = %d present=%t, want 10/true", resourceTime, ok)
	}
	override.restore()
	if _, timeWritten := result.writes[timeKey]; timeWritten {
		t.Fatal("restore retained a time key absent from the worker result")
	}
}

func TestVMBlockEnergyBoundaryProjectionMatchesSerialAccumulator(t *testing.T) {
	stats := forkStatsMem{}
	passVersion3_6_5(stats, 27)
	dynProps := state.NewDynamicProperties()
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetBlockEnergyUsage(40)
	result := &actuator.Result{
		EnergyUsageTotal:  1_000,
		EnergyUsed:        600,
		OriginEnergyUsage: 100,
	}
	pre := &discardShadowPreexecution{
		results: []discardShadowTaskResult{{
			txIndex: 0,
			info: &corepb.TransactionInfo{Receipt: &corepb.ResourceReceipt{
				EnergyUsageTotal:  result.EnergyUsageTotal,
				EnergyUsage:       result.EnergyUsed,
				OriginEnergyUsage: result.OriginEnergyUsage,
			}},
			applyEligible: true,
			applyMatch:    true,
		}},
		resultByTx:    []int{0},
		readVersions:  []discardShadowReadVersionResult{{publishable: true}},
		readValidated: []bool{true},
		published:     make([]bool, 1),
		blockEnergy:   make([]discardShadowBlockEnergyProjection, 1),
	}

	pre.projectBlockEnergyBoundary(0, dynProps, stats, 0, nil)
	projection := pre.blockEnergy[0]
	if !projection.observed || projection.baseline != 40 || projection.expected != 1_040 || projection.validated {
		t.Fatalf("VM block-energy projection = %+v", projection)
	}
	baseline, expected, admitted := pre.blockEnergyBoundaryForPublication(0, dynProps)
	if !admitted || baseline != 40 || expected != 1_040 {
		t.Fatalf("VM block-energy publication boundary = %d/%d admitted=%t", baseline, expected, admitted)
	}
	dynProps.SetBlockEnergyUsage(41)
	if _, _, admitted := pre.blockEnergyBoundaryForPublication(0, dynProps); admitted {
		t.Fatal("VM block-energy publication admitted a changed canonical baseline")
	}
	dynProps.SetBlockEnergyUsage(40)
	accumulateBlockEnergyUsage(dynProps, stats, 0, result, nil)
	pre.validateBlockEnergyBoundary(0, dynProps)
	projection = pre.blockEnergy[0]
	if !projection.validated || !projection.match || dynProps.BlockEnergyUsage() != 1_040 {
		t.Fatalf("VM block-energy validation = %+v, canonical=%d", projection, dynProps.BlockEnergyUsage())
	}
	versioned := &versionedAccessShadow{
		transactionWritesOK:  []bool{true},
		transactionWriteSets: []state.TransactionWriteSet{{}},
	}
	finishStats := (&discardShadowBlock{}).finishVMSenderChains(pre, versioned, discardShadowRunConfig{
		transactions:   []*types.Transaction{makeTestTriggerTx(1, testProcessorAddr(8), nil)},
		canonicalInfos: []*corepb.TransactionInfo{pre.results[0].info},
	})
	if finishStats.blockEnergyCandidates != 1 || finishStats.blockEnergyObserved != 1 ||
		finishStats.blockEnergyMatches != 1 || finishStats.blockEnergyMismatches != 0 || finishStats.blockEnergyMissing != 0 {
		t.Fatalf("VM block-energy finish stats = %+v", finishStats)
	}
}

func TestSenderChainAdvanceForwardsRawKV(t *testing.T) {
	workerState := newTestState(t)
	worker := discardShadowWorker{
		state:    workerState,
		dynProps: workerState.DynamicProperties(),
	}
	key := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "sender-chain-raw"}
	writes := state.TransactionWriteSet{
		key: {Exists: true, Value: []byte("forwarded")},
	}
	if err := worker.advanceSenderChain(writes); err == nil {
		t.Fatal("transfer sender-chain path unexpectedly accepted raw KV forwarding")
	}
	if err := worker.advanceSenderChainWrites(writes, true); err != nil {
		t.Fatal(err)
	}
	got, err := worker.db.Get([]byte(key.LogicalKey))
	if err != nil || string(got) != "forwarded" {
		t.Fatalf("forwarded raw value = %q, %v", got, err)
	}
	worker.db.reset()
	got, err = worker.db.Get([]byte(key.LogicalKey))
	if err != nil || string(got) != "forwarded" {
		t.Fatalf("transaction reset lost forwarded raw value = %q, %v", got, err)
	}
	if err := worker.advanceSenderChainWrites(state.TransactionWriteSet{
		key: {Exists: false},
	}, true); err != nil {
		t.Fatal(err)
	}
	if exists, err := worker.db.Has([]byte(key.LogicalKey)); err != nil || exists {
		t.Fatalf("forwarded raw tombstone = exists:%v err:%v", exists, err)
	}
	worker.db.resetForwarded()
	if _, err := worker.db.Get([]byte(key.LogicalKey)); err == nil {
		t.Fatal("chain reset retained forwarded raw value")
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
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: int64(discardShadowAsyncRetryFirstOffset), Timestamp: 3_000}},
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
	if retry == nil || len(retry.asyncRunners) != 1 || retry.asyncRunners[0].blockBase == nil || retry.asyncRunners[0].worker != nil || retry.stats.actualPrewarmed != 1 {
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
		Number: int64(discardShadowAsyncRetryFirstOffset), Timestamp: 3_000,
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
