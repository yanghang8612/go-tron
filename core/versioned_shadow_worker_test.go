package core

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"errors"
	"maps"
	"reflect"
	"strings"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/tronprotocol/go-tron/actuator"
	tcommon "github.com/tronprotocol/go-tron/common"
	gtronlog "github.com/tronprotocol/go-tron/common/log"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	"github.com/tronprotocol/go-tron/params"
	corepb "github.com/tronprotocol/go-tron/proto/core"
	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

var senderChainPreexecutionBenchmarkSink *discardShadowPreexecution

func BenchmarkSenderChainPreexecutionIndependentChains(b *testing.B) {
	base := newTestState(b)
	const transactionCount = 64
	transactions := make([]*types.Transaction, 0, transactionCount)
	blockPB := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number: 1, Timestamp: 3_000,
	}}}
	for i := 0; i < transactionCount; i++ {
		owner := byte(i + 1)
		recipient := byte(i + 1 + transactionCount)
		base.CreateAccount(testProcessorAddr(owner), corepb.AccountType_Normal)
		base.AddBalance(testProcessorAddr(owner), 10_000_000)
		base.CreateAccount(testProcessorAddr(recipient), corepb.AccountType_Normal)
		tx := makeTestTransferTx(owner, recipient, 1_000)
		transactions = append(transactions, tx)
		blockPB.Transactions = append(blockPB.Transactions, tx.Proto())
	}
	if _, err := base.Commit(); err != nil {
		b.Fatal(err)
	}
	base.SetDynamicProperties(base.DynamicProperties().Copy())
	block := types.NewBlockFromPB(blockPB)
	chains := transferSenderChains(transactions)
	if len(chains) != transactionCount {
		b.Fatalf("sender chains = %d, want %d", len(chains), transactionCount)
	}
	shadow := &discardShadowBlock{base: base}
	cfg := discardShadowRunConfig{block: block, transactions: transactions}

	b.ReportAllocs()
	b.ReportMetric(float64(len(chains)), "chains/op")
	b.ResetTimer()
	for b.Loop() {
		senderChainPreexecutionBenchmarkSink = shadow.preexecuteSenderChainsWithRetryState(
			cfg, chains, preexecutedTransferReady, false, false,
		)
	}
}

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
		{blockNum: 64, want: true},
		{blockNum: 128, want: false},
		{blockNum: 192, want: false},
		{blockNum: 256, want: true},
		{blockNum: 320, want: true},
		{blockNum: 512, want: true},
		{blockNum: 1024, want: true},
		{blockNum: 1088, want: true},
	}
	for _, test := range tests {
		if got := useVMSenderRetryObservation(test.blockNum); got != test.want {
			t.Fatalf("block %d VM retry observation cohort = %t, want %t", test.blockNum, got, test.want)
		}
	}
}

func TestVMSenderRetryPublicationCohort(t *testing.T) {
	tests := []struct {
		blockNum uint64
		want     bool
	}{
		{blockNum: 0, want: false},
		{blockNum: 64, want: true},
		{blockNum: 128, want: false},
		{blockNum: 192, want: false},
		{blockNum: 256, want: true},
		{blockNum: 320, want: true},
		{blockNum: 512, want: true},
		{blockNum: 768, want: true},
		{blockNum: 1_024, want: false},
		{blockNum: 1_280, want: true},
		{blockNum: 1_536, want: true},
		{blockNum: 1_792, want: true},
		{blockNum: 2_048, want: false},
		{blockNum: 2_304, want: true},
	}
	for _, test := range tests {
		if got := useVMSenderRetryPublication(test.blockNum); got != test.want {
			t.Fatalf("block %d VM retry publication cohort = %t, want %t", test.blockNum, got, test.want)
		}
	}
}

func TestVMSenderRetryPublicationStaysDisjointFromTransferPublisher(t *testing.T) {
	for blockNum := uint64(0); blockNum < 4*vmSenderRetryPublishInterval; blockNum += discardShadowSampleInterval {
		if useVMSenderRetryPublication(blockNum) && useDiscardShadowAsyncRetryPublication(blockNum) {
			t.Fatalf("block %d enables both VM and Transfer retry publishers", blockNum)
		}
	}
}

func TestVMSenderChainPublicationStaysDisjointFromRetryPublishers(t *testing.T) {
	for blockNum := uint64(0); blockNum < 4*vmSenderRetryPublishInterval; blockNum++ {
		if !useVMSenderChainPublication(blockNum) {
			continue
		}
		if useVMSenderRetryPublication(blockNum) {
			t.Fatalf("block %d enables both VM block-start and VM retry publishers", blockNum)
		}
		if useDiscardShadowAsyncRetryPublication(blockNum) {
			t.Fatalf("block %d enables both VM block-start and Transfer retry publishers", blockNum)
		}
	}
}

func TestVMSenderChainPublicationCohort(t *testing.T) {
	tests := []struct {
		blockNum uint64
		want     bool
	}{
		{blockNum: 0, want: false},
		{blockNum: 8, want: false},
		{blockNum: 24, want: false},
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

func TestPreexecutedVMEntryCodeFingerprintRejectsMismatchedBase(t *testing.T) {
	contractAddr := testProcessorAddr(0x71)
	tx := makeTestTriggerTx(1, contractAddr, nil)

	workerState := newTestState(t)
	workerState.CreateAccount(contractAddr, corepb.AccountType_Contract)
	addr, hash, ok := captureVMEntryCodeFingerprint(workerState, tx)
	if !ok || addr != contractAddr {
		t.Fatalf("worker fingerprint = %s/%t, want %s/true", addr.Hex(), ok, contractAddr.Hex())
	}
	preResult := &discardShadowTaskResult{
		vmEntryCodeAddress: addr,
		vmEntryCodeHash:    hash,
		hasVMEntryCodeHash: true,
	}
	if !preexecutedVMEntryCodeMatches(workerState, preResult) {
		t.Fatal("unchanged worker entry code did not match")
	}

	canonicalState := newTestState(t)
	canonicalState.CreateAccount(contractAddr, corepb.AccountType_Contract)
	canonicalState.SetCode(contractAddr, []byte{0x00}) // STOP
	if preexecutedVMEntryCodeMatches(canonicalState, preResult) {
		t.Fatal("empty-code speculative base matched canonical contract bytecode")
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
	if !fullTransactions[1] || !fullTransactions[2] || !fullTransactions[3] {
		t.Fatalf("full capture transactions = %v, want every publication candidate tx 1, 2, and 3", fullTransactions)
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

func TestDiscardShadowRetryWriteCaptureCoversUnretainedAsyncSuffix(t *testing.T) {
	source := &discardShadowPreexecution{
		// The first sender task was retained as an insufficient-balance result;
		// the block-start worker stopped before tx 2. A canonical credit can make
		// an async incarnation of both tasks publishable later in the block.
		results:      []discardShadowTaskResult{{txIndex: 1, err: errors.New("insufficient balance")}},
		senderTaskOK: []bool{false, true, true},
		senderNext:   []int{-1, 2, -1},
	}
	_, fullTransactions, _ := newDiscardShadowRetryWriteCapture(source, 3)
	if fullTransactions[0] || !fullTransactions[1] || !fullTransactions[2] {
		t.Fatalf("async suffix full captures = %v, want tx 1 and 2", fullTransactions)
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
	frozen, keys, err := retry.freezeAsyncRawView(runner, 0, nil)
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
	if frozen.lastMissFamily != int64(rawdb.PhysicalKeyFamilyOther) || frozen.lastMissLength != int64(len("uncaptured")) {
		t.Fatalf("last miss family/length = %d/%d, want other/%d", frozen.lastMissFamily, frozen.lastMissLength, len("uncaptured"))
	}
	if got := uint64(frozen.lastMissPrefix); got != binary.BigEndian.Uint64([]byte("uncaptur")) {
		t.Fatalf("last miss prefix = %x, want %x", got, []byte("uncaptur"))
	}
	if got := uint64(frozen.lastMissSuffix); got != binary.BigEndian.Uint64([]byte("captured")) {
		t.Fatalf("last miss suffix = %x, want %x", got, []byte("captured"))
	}
}

func TestAsyncRetryFrozenRawViewIncludesTAPOSEnvelopeDependencies(t *testing.T) {
	parent := ethrawdb.NewMemoryDatabase()
	refBlockBytes := []byte{0x27, 0xed}
	key := rawdb.TaposRefStorageKeyFromReference(refBlockBytes)
	if err := parent.Put(key, []byte("boundary")); err != nil {
		t.Fatal(err)
	}
	tx := makeTestTransferTx(1, 2, 1)
	tx.Proto().RawData.RefBlockBytes = refBlockBytes
	retry := &discardShadowSenderRetry{source: &discardShadowPreexecution{
		resultByTx: []int{-1},
		senderNext: []int{-1},
	}}

	frozen, keys, err := retry.freezeAsyncRawViewFrom(parent, 0, []*types.Transaction{tx})
	if err != nil {
		t.Fatal(err)
	}
	if keys != 1 {
		t.Fatalf("frozen keys = %d, want one derived TAPOS key", keys)
	}
	if err := parent.Put(key, []byte("laterxxx")); err != nil {
		t.Fatal(err)
	}
	if got, err := frozen.Get(key); err != nil || string(got) != "boundary" {
		t.Fatalf("frozen TAPOS value = %q, %v; want boundary", got, err)
	}
}

type frozenRawBlockHashParent struct {
	ethdb.KeyValueStore
	hashes map[uint64]tcommon.Hash
}

func (db *frozenRawBlockHashParent) BlockHashByNumberStrict(number uint64) (tcommon.Hash, bool, error) {
	hash, ok := db.hashes[number]
	return hash, ok, nil
}

func TestAsyncRetryFrozenRawViewRetainsOnlyImmutableBlockHashCapability(t *testing.T) {
	want := tcommon.HexToHash("0000000000000000000000000000000000000000000000000000000000000042")
	parent := &frozenRawBlockHashParent{
		KeyValueStore: ethrawdb.NewMemoryDatabase(),
		hashes:        map[uint64]tcommon.Hash{42: want},
	}
	retry := &discardShadowSenderRetry{source: &discardShadowPreexecution{
		resultByTx: []int{-1},
		senderNext: []int{-1},
	}}

	frozen, keys, err := retry.freezeAsyncRawViewFrom(parent, 0, nil)
	if err != nil {
		t.Fatal(err)
	}
	if keys != 0 {
		t.Fatalf("frozen raw keys = %d, want 0", keys)
	}
	worker := discardShadowWorker{db: discardKVOverlay{parent: frozen}}
	reader, ok := worker.transactionDB().(rawdb.BlockHashReaderStrict)
	if !ok {
		t.Fatal("frozen execution DB did not retain the strict block-hash capability")
	}
	if got, found, err := reader.BlockHashByNumberStrict(42); err != nil || !found || got != want {
		t.Fatalf("immutable block hash = %x, %v, %v; want %x, true, nil", got, found, err, want)
	}
	if _, err := frozen.Get([]byte("uncaptured")); !errors.Is(err, errDiscardShadowFrozenRawMiss) {
		t.Fatalf("uncaptured raw read error = %v", err)
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

func TestAsyncRetryContractRetMismatchRemainsValidatableButNotPublishable(t *testing.T) {
	retry := &discardShadowSenderRetry{
		async:             true,
		publish:           true,
		ready:             preexecutedResultReady,
		results:           make([]discardShadowTaskResult, 1),
		available:         make([]bool, 1),
		selected:          make([]discardShadowTaskResult, 1),
		selectedOK:        make([]bool, 1),
		selectedPublished: make([]bool, 1),
		incarnations:      []uint32{1},
	}
	result := discardShadowTaskResult{
		txIndex: 0, incarnation: 1,
		info: new(corepb.TransactionInfo), applyEligible: true, applyMatch: true,
		contractRetMismatch: true, contractRetBlock: 123, contractRetExpected: 2,
		contractRetActual: 1, contractRetTxHash: 456,
	}
	retry.consumeAsyncEvent(discardShadowAsyncRetryEvent{result: &result}, 0)
	if !retry.available[0] {
		t.Fatal("contract-result mismatch was discarded before read-version validation")
	}
	if retry.resultReady(&retry.results[0]) {
		t.Fatal("contract-result mismatch became publish-ready")
	}
	if retry.stats.actualErrors != 0 || retry.stats.errors != 0 || retry.stats.actualRetMismatches != 1 {
		t.Fatalf("contract-result mismatch stats = %+v", retry.stats)
	}
	if retry.stats.actualContractBlock != 123 || retry.stats.actualContractTx != 0 ||
		retry.stats.actualContractStart != 0 || retry.stats.actualContractWant != 2 ||
		retry.stats.actualContractGot != 1 || retry.stats.actualContractHash != 456 {
		t.Fatalf("contract-result mismatch diagnostics = %+v", retry.stats)
	}
	retry.selected[0] = retry.results[0]
	retry.selectedOK[0] = true
	if _, ok := retry.selectedResultForPublication(0); ok {
		t.Fatal("contract-result mismatch was exposed to the ordered publisher")
	}
}

func TestAsyncRetryContractRetMismatchTerminalClassification(t *testing.T) {
	complete := func(incarnation uint32) discardShadowTaskResult {
		return discardShadowTaskResult{
			txIndex: 0, incarnation: incarnation,
			info: new(corepb.TransactionInfo), applyEligible: true, applyMatch: true,
			contractRetMismatch: true,
		}
	}
	tests := []struct {
		name     string
		result   discardShadowTaskResult
		boundary int
		check    func(discardShadowSenderRetryStats) bool
	}{
		{
			name: "stale incarnation", result: complete(2), boundary: 0,
			check: func(stats discardShadowSenderRetryStats) bool {
				return stats.actualRetStale == 1 && stats.actualStale == 1 && stats.actualErrors == 0
			},
		},
		{
			name: "late result", result: complete(1), boundary: 1,
			check: func(stats discardShadowSenderRetryStats) bool {
				return stats.actualRetLate == 1 && stats.actualLate == 1 && stats.actualErrors == 0
			},
		},
		{
			name: "invalid result", result: discardShadowTaskResult{
				txIndex: 0, incarnation: 1, contractRetMismatch: true,
			}, boundary: 0,
			check: func(stats discardShadowSenderRetryStats) bool {
				return stats.actualRetInvalid == 1 && stats.actualErrors == 1
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			retry := &discardShadowSenderRetry{
				ready:        preexecutedResultReady,
				results:      make([]discardShadowTaskResult, 1),
				available:    make([]bool, 1),
				selectedOK:   make([]bool, 1),
				incarnations: []uint32{1},
			}
			retry.consumeAsyncEvent(discardShadowAsyncRetryEvent{result: &test.result}, test.boundary)
			if retry.stats.actualRetMismatches != 1 || !test.check(retry.stats) {
				t.Fatalf("terminal mismatch stats = %+v", retry.stats)
			}
		})
	}
}

func TestAsyncRetryContractRetMismatchVersionClassification(t *testing.T) {
	readKey := state.TransactionAccessKey{Kind: state.TransactionAccessRawKV, LogicalKey: "ordered-write"}
	tests := []struct {
		name     string
		conflict bool
	}{
		{name: "version clean"},
		{name: "rejected by versions", conflict: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			source := &discardShadowPreexecution{
				resultByTx: []int{-1, -1},
				senderNext: []int{-1, -1},
			}
			retry := &discardShadowSenderRetry{
				source:            source,
				async:             true,
				publish:           true,
				ready:             preexecutedResultReady,
				results:           make([]discardShadowTaskResult, 2),
				available:         make([]bool, 2),
				selected:          make([]discardShadowTaskResult, 2),
				selectedOK:        make([]bool, 2),
				selectedPublished: make([]bool, 2),
				incarnations:      []uint32{0, 1},
			}
			result := discardShadowTaskResult{
				txIndex: 1, incarnation: 1,
				info: new(corepb.TransactionInfo), applyEligible: true, applyMatch: true,
				contractRetMismatch: true,
			}
			if test.conflict {
				result.reads = state.TransactionReadSet{Reads: []state.TransactionRead{{
					Key: readKey, Mode: state.TransactionAccessRead,
				}}}
			}
			retry.consumeAsyncEvent(discardShadowAsyncRetryEvent{result: &result}, 0)
			var versioned versionedAccessShadow
			versioned.Prepare(2)
			if test.conflict {
				versioned.versions[readKey] = 0
			}
			retry.observeAsyncBoundary(1, nil, nil, nil, &versioned, discardShadowRunConfig{})
			if retry.selectedOK[1] {
				t.Fatal("contract-result mismatch became a selected candidate")
			}
			if test.conflict {
				if retry.stats.actualRetRejected != 1 || retry.stats.actualRetClean != 0 {
					t.Fatalf("version-rejected stats = %+v", retry.stats)
				}
			} else if retry.stats.actualRetClean != 1 || retry.stats.actualRetRejected != 0 {
				t.Fatalf("version-clean stats = %+v", retry.stats)
			}
		})
	}
}

func TestAsyncRetryContractRetMismatchSupersededAfterArrivalIsStale(t *testing.T) {
	retry := &discardShadowSenderRetry{
		source: &discardShadowPreexecution{
			senderTasks:  []discardShadowSenderChainTask{{txIndex: 0}},
			senderTaskOK: []bool{true},
			senderNext:   []int{-1},
		},
		results:            []discardShadowTaskResult{{txIndex: 0, incarnation: 1, contractRetMismatch: true}},
		available:          []bool{true},
		selectedOK:         make([]bool, 1),
		selectedAsyncReady: make([]bool, 1),
		incarnations:       []uint32{1},
	}
	tasks, deferred := retry.invalidateAsyncSuffix(0, 1)
	if len(tasks) != 1 || deferred != 0 || tasks[0].incarnation != 2 {
		t.Fatalf("replacement tasks = %+v deferred=%d", tasks, deferred)
	}
	if retry.available[0] || retry.stats.actualRetStale != 1 {
		t.Fatalf("superseded mismatch state = available:%t stats:%+v", retry.available[0], retry.stats)
	}
	_, _ = retry.invalidateAsyncSuffix(0, 1)
	if retry.stats.actualRetStale != 1 {
		t.Fatalf("superseded mismatch counted twice: %+v", retry.stats)
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

	mixed := state.TransactionWriteSet{
		{Kind: state.TransactionAccessAccount, Address: testProcessorAddr(1)}: {},
		{Kind: state.TransactionAccessKind(0xff)}:                             {},
	}
	want = discardShadowApplyUnsupportedAccount | discardShadowApplyUnsupportedOther
	if got := classifyDiscardShadowApplyUnsupported(mixed); got != want {
		t.Fatalf("mixed known/unknown unsupported classes = %#x, want %#x", got, want)
	}

	supportedButRejected := state.TransactionWriteSet{
		{Kind: state.TransactionAccessStorage, Address: testProcessorAddr(1)}: {},
	}
	if got := classifyDiscardShadowApplyUnsupported(supportedButRejected); got != discardShadowApplyUnsupportedOther {
		t.Fatalf("supported-family validation rejection = %#x, want other", got)
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

	writeOnly := discardShadowTaskResult{writes: state.TransactionWriteSet{
		key: {Exists: true, Value: make([]byte, 8)},
	}}
	if decision := clean.validateBlockStartReadSet(1, nil, writeOnly); !decision.publishable || decision.readConflict {
		t.Fatalf("write-only path without predecessor rejected: %+v", decision)
	}
	if decision := conflicted.validateBlockStartReadSet(1, nil, writeOnly); decision.publishable || !decision.readConflict {
		t.Fatalf("write-only stale post-image accepted: %+v", decision)
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

func TestTransferBalanceOracleUsesCanonicalBoundary(t *testing.T) {
	statedb := newTestState(t)
	owner := testProcessorAddr(1)
	recipient := testProcessorAddr(2)
	statedb.CreateAccount(owner, corepb.AccountType_Normal)
	statedb.AddBalance(owner, 10_000_000)
	statedb.CreateAccount(recipient, corepb.AccountType_Normal)
	statedb.AddBalance(recipient, 500)
	tx := makeTestTransferTx(1, 2, 1_000)
	balanceWrite := func(value int64) state.TransactionWriteValue {
		var encoded [8]byte
		binary.BigEndian.PutUint64(encoded[:], uint64(value))
		return state.TransactionWriteValue{Exists: true, Value: encoded[:]}
	}
	result := &discardShadowTaskResult{
		info: &corepb.TransactionInfo{Fee: 30},
		writes: state.TransactionWriteSet{
			{Kind: state.TransactionAccessAccountField, Address: owner, AccountField: state.TransactionAccountFieldBalance}:     balanceWrite(9_998_970),
			{Kind: state.TransactionAccessAccountField, Address: recipient, AccountField: state.TransactionAccountFieldBalance}: balanceWrite(1_500),
		},
	}
	if ok, err := validateTransferBalancePostImages(statedb, tx, result, discardShadowRunConfig{}, 0); !ok || err != nil {
		t.Fatalf("canonical transfer balance post-images rejected: ok=%t err=%v", ok, err)
	}
	result.writes[state.TransactionAccessKey{
		Kind: state.TransactionAccessAccountField, Address: recipient, AccountField: state.TransactionAccountFieldBalance,
	}] = balanceWrite(1_499)
	if ok, err := validateTransferBalancePostImages(statedb, tx, result, discardShadowRunConfig{}, 0); ok || !errors.Is(err, errSpeculativePublicationAudit) {
		t.Fatalf("stale recipient balance post-image result: ok=%t err=%v", ok, err)
	}
	fallbacksBefore := parallelTransferBalanceOracleFallbacksCounter.Snapshot().Count()
	freshTx := makeTestTransferTx(1, 3, 1_000)
	if ok, err := validateTransferBalancePostImages(statedb, freshTx, result, discardShadowRunConfig{}, 0); ok || err != nil {
		t.Fatalf("fresh-recipient balance oracle result: ok=%t err=%v", ok, err)
	}
	if fallbacks := parallelTransferBalanceOracleFallbacksCounter.Snapshot().Count() - fallbacksBefore; fallbacks != 1 {
		t.Fatalf("fresh-recipient balance-oracle fallbacks = %d, want 1", fallbacks)
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
	if len(chains) != 2 {
		t.Fatalf("sender chains = %v, want 2", chains)
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
	if afterUnsupported != nil {
		t.Fatalf("post-VM transfer escaped the serial suffix: %+v", afterUnsupported)
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
	if len(chains) != 2 {
		t.Fatalf("VM sender chains = %v, want 2", chains)
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
	if afterTransfer != nil {
		t.Fatalf("post-transfer VM transaction escaped the serial suffix: %+v", afterTransfer)
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

func TestTransferCanonicalBoundarySerialVerificationRejectsStaleRecipientWrite(t *testing.T) {
	base := newTestState(t)
	owner := testProcessorAddr(1)
	recipient := testProcessorAddr(2)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.CreateAccount(recipient, corepb.AccountType_Normal)
	base.AddBalance(owner, 1_000_000)
	base.AddBalance(recipient, 50_000)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	tx := makeTestTransferTx(1, 2, 4_455)
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowSampleInterval), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	cfg := discardShadowRunConfig{
		block:                   block,
		db:                      ethrawdb.NewMemoryDatabase(),
		transactions:            []*types.Transaction{tx},
		energyLimitForkBlockNum: params.DefaultBlockNumForEnergyLimit,
		captureBalanceTrace:     true,
		retainInfos:             true,
	}
	shadow := prepareTransferExecutionBlock(base, base.DynamicProperties(), block.Number(), false)
	if shadow == nil {
		t.Fatal("missing sampled transfer execution base")
	}
	pre := shadow.preexecuteTransfers(cfg)
	if pre == nil || len(pre.results) != 1 || !preexecutedTransferReady(&pre.results[0]) {
		t.Fatalf("transfer preexecution unavailable: %+v", pre)
	}
	result := &pre.results[0]
	override, admitted := overridePublicNetReservation(result, base.DynamicProperties())
	if !admitted {
		t.Fatal("public-net reservation was not admitted")
	}
	defer override.restore()

	commitment := func() tcommon.Hash {
		copyState, copyErr := base.Copy()
		if copyErr != nil {
			t.Fatal(copyErr)
		}
		root, commitErr := copyState.Commit()
		if commitErr != nil {
			t.Fatal(commitErr)
		}
		return root
	}
	dynamicSnapshot := func() (map[string]int64, map[string]string) {
		copyDP := base.DynamicProperties().Copy()
		ints := copyDP.All()
		strings := make(map[string]string, len(copyDP.StringKeys()))
		for _, key := range copyDP.StringKeys() {
			value, _ := copyDP.GetString(key)
			strings[key] = value
		}
		return ints, strings
	}
	rootBefore := commitment()
	intsBefore, stringsBefore := dynamicSnapshot()
	journalBefore := base.DomainChangeJournalMark()

	var outerRecorder state.TransactionAccessRecorder
	outerRecorder.Reset(16)
	base.SetTransactionAccessRecorder(&outerRecorder)
	base.DynamicProperties().SetTransactionAccessRecorder(&outerRecorder)
	verification := verifyTransferResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg)
	if !verification.matched() {
		t.Fatalf("matching transfer boundary result was rejected: %+v", verification)
	}
	if verification.canonical == nil || verification.canonical == result {
		t.Fatal("Transfer oracle did not retain an independently executed canonical result")
	}
	canonicalWrites := cloneTransactionWriteSet(verification.canonical.writes)
	canonicalReads := cloneTransactionReadSet(verification.canonical.reads)
	canonicalInfo := proto.Clone(verification.canonical.info).(*corepb.TransactionInfo)
	canonicalTrace := proto.Clone(verification.canonical.balanceTrace).(*contractpb.TransactionBalanceTrace)
	sourceWrites := cloneTransactionWriteSet(result.writes)
	sourceReads := cloneTransactionReadSet(result.reads)
	sourceInfo := proto.Clone(result.info).(*corepb.TransactionInfo)
	sourceTrace := proto.Clone(result.balanceTrace).(*contractpb.TransactionBalanceTrace)
	for key, value := range result.writes {
		if len(value.Value) == 0 {
			continue
		}
		value.Value[0] ^= 0xff
		result.writes[key] = value
		break
	}
	result.reads.Unsupported = !result.reads.Unsupported
	result.info.Fee++
	result.balanceTrace.TransactionIdentifier[0] ^= 0xff
	if !state.EqualTransactionWriteSets(verification.canonical.writes, canonicalWrites) ||
		!reflect.DeepEqual(verification.canonical.reads, canonicalReads) ||
		!proto.Equal(verification.canonical.info, canonicalInfo) ||
		!proto.Equal(verification.canonical.balanceTrace, canonicalTrace) {
		t.Fatal("speculative source mutation escaped into the authoritative serial result")
	}
	result.writes = sourceWrites
	result.reads = sourceReads
	result.info = sourceInfo
	result.balanceTrace = sourceTrace
	_ = base.GetBalance(owner)
	_, _ = base.DynamicProperties().Get("transaction_fee")
	outerReads := outerRecorder.CaptureReadSet()
	seenOwnerBalance, seenTransactionFee := false, false
	for _, read := range outerReads.Reads {
		seenOwnerBalance = seenOwnerBalance || (read.Key.Kind == state.TransactionAccessAccountField &&
			read.Key.Address == owner && read.Key.AccountField == state.TransactionAccountFieldBalance)
		seenTransactionFee = seenTransactionFee || (read.Key.Kind == state.TransactionAccessDynamicInt && read.Key.LogicalKey == "transaction_fee")
	}
	if !seenOwnerBalance || !seenTransactionFee {
		t.Fatalf("boundary oracle did not restore outer recorder: %+v", outerReads.Reads)
	}
	base.SetTransactionAccessRecorder(nil)
	base.DynamicProperties().SetTransactionAccessRecorder(nil)
	rootAfter := commitment()
	intsAfter, stringsAfter := dynamicSnapshot()
	if rootAfter != rootBefore {
		t.Fatalf("Transfer oracle changed commitment root: before=%x after=%x", rootBefore, rootAfter)
	}
	if !maps.Equal(intsBefore, intsAfter) || !maps.Equal(stringsBefore, stringsAfter) {
		t.Fatalf("Transfer oracle changed dynamic properties")
	}
	if journalAfter := base.DomainChangeJournalMark(); journalAfter != journalBefore {
		t.Fatalf("Transfer oracle changed domain journal mark: before=%d after=%d", journalBefore, journalAfter)
	}
	if base.BalanceTraceActive() {
		t.Fatal("temporary Transfer oracle balance trace escaped its scope")
	}

	// The same ownership guarantees must hold when authoritative execution
	// rejects before producing a receipt/WriteSet.
	failingCfg := cfg
	failingCfg.transactions = []*types.Transaction{makeTestTransferTx(1, 2, 2_000_000)}
	outerRecorder.Reset(16)
	base.SetTransactionAccessRecorder(&outerRecorder)
	base.DynamicProperties().SetTransactionAccessRecorder(&outerRecorder)
	failedVerification := verifyTransferResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, failingCfg)
	if failedVerification.err == nil || failedVerification.matched() {
		t.Fatalf("insufficient-balance boundary execution unexpectedly succeeded: %+v", failedVerification)
	}
	_ = base.GetBalance(owner)
	_, _ = base.DynamicProperties().Get("transaction_fee")
	failedReads := outerRecorder.CaptureReadSet()
	seenOwnerBalance, seenTransactionFee = false, false
	for _, read := range failedReads.Reads {
		seenOwnerBalance = seenOwnerBalance || (read.Key.Kind == state.TransactionAccessAccountField &&
			read.Key.Address == owner && read.Key.AccountField == state.TransactionAccountFieldBalance)
		seenTransactionFee = seenTransactionFee || (read.Key.Kind == state.TransactionAccessDynamicInt && read.Key.LogicalKey == "transaction_fee")
	}
	if !seenOwnerBalance || !seenTransactionFee {
		t.Fatalf("failed boundary oracle did not restore outer recorder: %+v", failedReads.Reads)
	}
	base.SetTransactionAccessRecorder(nil)
	base.DynamicProperties().SetTransactionAccessRecorder(nil)
	if failedRoot := commitment(); failedRoot != rootBefore {
		t.Fatalf("failed Transfer oracle changed commitment root: before=%x after=%x", rootBefore, failedRoot)
	}
	if base.BalanceTraceActive() {
		t.Fatal("failed Transfer oracle leaked temporary balance trace")
	}

	// A production history-enabled block already contains earlier canonical
	// transactions. The in-place oracle must preserve that recorder exactly.
	priorID := []byte{0xaa}
	base.BeginBalanceTrace(int64(block.Number()), block.Hash().Bytes(), block.Timestamp())
	base.AppendBalanceTraceTransaction(&contractpb.TransactionBalanceTrace{
		TransactionIdentifier: priorID,
		Type:                  "TransferContract",
		Status:                "SUCCESS",
		Operation: []*contractpb.TransactionBalanceTrace_Operation{{
			Address: recipient.Bytes(),
			Amount:  1,
		}},
	})
	verification = verifyTransferResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg)
	if !verification.matched() {
		t.Fatalf("active-trace transfer boundary result was rejected: %+v", verification)
	}
	if !base.BalanceTraceActive() || base.CopyLastBalanceTraceTransaction(priorID) == nil {
		t.Fatal("Transfer oracle replaced or truncated the canonical balance trace")
	}
	base.ClearBalanceTrace()

	// Model the original dangerous trust boundary: a worker computes a wrong
	// fee and a self-consistent sender post-image. The fast balance oracle alone
	// accepts that pair; the authoritative boundary execution must reject both.
	ownerKey := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		Address:      owner,
		AccountField: state.TransactionAccountFieldBalance,
	}
	originalOwnerWrite, ownerWriteFound := result.writes[ownerKey]
	if !ownerWriteFound || len(originalOwnerWrite.Value) != 8 {
		t.Fatalf("transfer result has no encoded owner balance: %+v", result.writes)
	}
	originalFee := result.info.Fee
	result.info.Fee += 30
	wrongOwnerWrite := originalOwnerWrite
	wrongOwnerWrite.Value = append([]byte(nil), originalOwnerWrite.Value...)
	binary.BigEndian.PutUint64(wrongOwnerWrite.Value, binary.BigEndian.Uint64(wrongOwnerWrite.Value)-30)
	result.writes[ownerKey] = wrongOwnerWrite
	if ok, err := validateTransferBalancePostImages(base, tx, result, cfg, 0); !ok || err != nil {
		t.Fatalf("self-consistent wrong fee did not reach the independent serial oracle: ok=%t err=%v", ok, err)
	}
	verification = verifyTransferResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg)
	if verification.matched() || verification.infoMatch || verification.writeMatch || verification.err != nil {
		t.Fatalf("self-consistent wrong fee was not rejected by boundary execution: %+v", verification)
	}
	if canonical, err := validateTransferResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg, nil); canonical != nil || !errors.Is(err, errSpeculativePublicationAudit) {
		t.Fatalf("Transfer serial-oracle mismatch did not open safety path: canonical=%+v err=%v", canonical, err)
	}
	result.info.Fee = originalFee
	result.writes[ownerKey] = originalOwnerWrite

	originalReads := cloneTransactionReadSet(result.reads)
	if len(result.reads.Reads) == 0 {
		t.Fatal("transfer result has no reads to audit")
	}
	result.reads.Reads = result.reads.Reads[1:]
	verification = verifyTransferResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg)
	if !verification.matched() || verification.readMatch || !verification.infoMatch || !verification.writeMatch ||
		!verification.balanceMatch || verification.err != nil {
		t.Fatalf("omitted speculative read affected authoritative serial admission: %+v", verification)
	}
	result.reads = originalReads

	mutated := false
	for key, value := range result.writes {
		if key.Address != recipient || len(value.Value) == 0 {
			continue
		}
		value.Value = append([]byte(nil), value.Value...)
		value.Value[len(value.Value)-1] ^= 1
		result.writes[key] = value
		mutated = true
		break
	}
	if !mutated {
		t.Fatalf("transfer result has no recipient write: %+v", result.writes)
	}

	verification = verifyTransferResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg)
	if verification.matched() || verification.writeMatch || !verification.infoMatch || verification.err != nil {
		t.Fatalf("stale recipient write was not isolated as a boundary mismatch: %+v", verification)
	}
}

func TestTransferCanonicalBoundaryDetectsUnjournaledCommutativeLeak(t *testing.T) {
	base := newTestState(t)
	owner := testProcessorAddr(1)
	recipient := testProcessorAddr(2)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.CreateAccount(recipient, corepb.AccountType_Normal)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.AddBalance(owner, 1_000_000_000)
	base.AddBalance(recipient, 1_000_000)
	base.AddBalance(params.BlackholeAddress, 100)
	dynProps := base.DynamicProperties()
	// Force paid bandwidth through the legacy Blackhole balance delta. This
	// is the consensus path where a leaked direct-oracle increment followed by
	// publication of the commutative delta would otherwise double-charge.
	dynProps.Set("free_net_limit", 0)
	dynProps.SetPublicNetLimit(0)
	dynProps.Set("total_net_limit", 0)
	dynProps.Set("transaction_fee", 2)
	dynProps.SetAllowBlackHoleOptimization(false)
	dynProps.SetAllowTransactionFeePool(false)
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	tx := makeTestTransferTx(1, 2, 123_456)
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(discardShadowSampleInterval), Timestamp: 3_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	cfg := discardShadowRunConfig{
		block:                   block,
		db:                      ethrawdb.NewMemoryDatabase(),
		transactions:            []*types.Transaction{tx},
		energyLimitForkBlockNum: params.DefaultBlockNumForEnergyLimit,
		retainInfos:             true,
	}
	shadow := prepareTransferExecutionBlock(base, dynProps, block.Number(), false)
	if shadow == nil {
		t.Fatal("missing sampled transfer execution base")
	}
	pre := shadow.preexecuteTransfers(cfg)
	if pre == nil || len(pre.results) != 1 || !preexecutedTransferReady(&pre.results[0]) {
		t.Fatalf("transfer preexecution unavailable: %+v", pre)
	}
	result := &pre.results[0]
	blackholeKey := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		Address:      params.BlackholeAddress,
		AccountField: state.TransactionAccountFieldBalance,
	}
	if write, ok := result.writes[blackholeKey]; !ok || !write.Commutative {
		t.Fatalf("paid bandwidth did not produce a commutative Blackhole delta: %+v", result.writes)
	}
	override, admitted := overridePublicNetReservation(result, dynProps)
	if !admitted {
		t.Fatal("public-net reservation was not admitted")
	}
	defer override.restore()

	journalBefore := base.DomainChangeJournalMark()
	blackholeBefore := base.GetBalance(params.BlackholeAddress)
	restoreMismatchesBefore := parallelTransferSerialVerifyRestoreMismatchCounter.Snapshot().Count()
	cfg.canonicalOraclePostExecutionTestHook = func(family string, statedb *state.StateDB, _ *state.DynamicProperties) {
		if family != "Transfer" {
			t.Fatalf("oracle family = %q, want Transfer", family)
		}
		account := statedb.GetAccount(params.BlackholeAddress)
		if account == nil {
			t.Fatal("missing Blackhole account in fault hook")
		}
		// Deliberately violate StateDB ownership to model the exact class the
		// absolute-value restoration seal guards: this mutates the cached
		// account without appending a journal entry.
		account.SetBalance(account.Balance() + 1)
	}
	canonical, err := validateTransferResultAtCanonicalBoundary(base, dynProps, 0, result, cfg, nil)
	if canonical != nil || !errors.Is(err, errSpeculativePublicationAudit) || !errors.Is(err, errCanonicalOracleRestoration) {
		t.Fatalf("unjournaled oracle leak was not rejected: canonical=%+v err=%v", canonical, err)
	}
	if got := parallelTransferSerialVerifyRestoreMismatchCounter.Snapshot().Count() - restoreMismatchesBefore; got != 1 {
		t.Fatalf("Transfer restoration mismatches = %d, want 1", got)
	}
	if got := base.DomainChangeJournalMark(); got != journalBefore {
		t.Fatalf("fault injection unexpectedly changed journal mark: got %d want %d", got, journalBefore)
	}
	if got := base.GetBalance(params.BlackholeAddress); got != blackholeBefore {
		t.Fatalf("unjournaled leak was detected but not self-cleaned: got %d want %d", got, blackholeBefore)
	}
}

func TestVMCanonicalBoundaryCleansDirectAndIsolatedLeaks(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(false)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x8e)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.AddBalance(params.BlackholeAddress, 100)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	base.SetCode(contractAddr, []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x50, 0x00})
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	tx := makeTestTriggerTx(1, contractAddr, nil)
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	cfg := discardShadowRunConfig{
		block:                   block,
		db:                      ethrawdb.NewMemoryDatabase(),
		transactions:            []*types.Transaction{tx},
		energyLimitForkBlockNum: params.DefaultBlockNumForEnergyLimit,
		retainInfos:             true,
	}
	shadow := prepareTransferExecutionBlock(base, dynProps, block.Number(), false)
	if shadow == nil {
		t.Fatal("missing sampled VM execution base")
	}
	pre := shadow.preexecuteVMSenderChains(cfg, false)
	if pre == nil || len(pre.results) != 1 || !preexecutedResultReady(&pre.results[0]) {
		t.Fatalf("VM preexecution unavailable: %+v", pre)
	}
	result := &pre.results[0]
	blackholeKey := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		Address:      params.BlackholeAddress,
		AccountField: state.TransactionAccountFieldBalance,
	}
	if write, ok := result.writes[blackholeKey]; !ok || !write.Commutative {
		t.Fatalf("VM energy settlement did not produce a commutative Blackhole delta: %+v", result.writes)
	}
	override, admitted := overridePublicNetReservation(result, dynProps)
	if !admitted {
		t.Fatal("public-net reservation was not admitted")
	}
	defer override.restore()

	journalBefore := base.DomainChangeJournalMark()
	blackholeBefore := base.GetBalance(params.BlackholeAddress)
	totalNetWeightBefore := dynProps.TotalNetWeight()
	restoreMismatchesBefore := parallelVMSerialVerifyRestoreMismatchCounter.Snapshot().Count()
	cfg.canonicalOraclePostExecutionTestHook = func(family string, _ *state.StateDB, oracleDP *state.DynamicProperties) {
		if family != "VM" {
			t.Fatalf("oracle family = %q, want VM", family)
		}
		// Exercise the independent DP guard with a normal journaled mutation
		// outside the candidate's commutative carrier. SnapshotChanged must make
		// the leak observable and the outer snapshot must remove it.
		oracleDP.SetTotalNetWeight(oracleDP.TotalNetWeight() + 1)
	}
	canonical, err := validateVMResultAtCanonicalBoundary(base, dynProps, 0, result, cfg)
	if canonical != nil || !errors.Is(err, errSpeculativePublicationAudit) || !errors.Is(err, errCanonicalOracleRestoration) {
		t.Fatalf("journaled VM oracle leak was not rejected: canonical=%+v err=%v", canonical, err)
	}
	if got := parallelVMSerialVerifyRestoreMismatchCounter.Snapshot().Count() - restoreMismatchesBefore; got != 1 {
		t.Fatalf("VM restoration mismatches = %d, want 1", got)
	}
	if got := base.DomainChangeJournalMark(); got != journalBefore {
		t.Fatalf("VM fault injection unexpectedly changed journal mark: got %d want %d", got, journalBefore)
	}
	if got := base.GetBalance(params.BlackholeAddress); got != blackholeBefore {
		t.Fatalf("VM dynamic-property fault changed Blackhole balance: got %d want %d", got, blackholeBefore)
	}
	if got := dynProps.TotalNetWeight(); got != totalNetWeightBefore {
		t.Fatalf("journaled VM dynamic-property leak was detected but not rolled back: got %d want %d", got, totalNetWeightBefore)
	}

	// Now model a StateDB.Copy ownership regression. The isolated oracle has
	// already executed on its copy when this hook mutates the original cached
	// account without journaling. Its own guard must detect and repair the leak,
	// and VM admission must stop before the direct oracle starts.
	restoreMismatchesBefore = parallelVMSerialVerifyRestoreMismatchCounter.Snapshot().Count()
	cfg.canonicalOraclePostExecutionTestHook = func(string, *state.StateDB, *state.DynamicProperties) {
		t.Fatal("direct VM oracle ran after isolated restoration failure")
	}
	cfg.canonicalIsolatedOraclePostExecutionTestHook = func(statedb *state.StateDB, _ *state.DynamicProperties) {
		account := statedb.GetAccount(params.BlackholeAddress)
		if account == nil {
			t.Fatal("missing Blackhole account in isolated VM fault hook")
		}
		account.SetBalance(account.Balance() + 1)
	}
	canonical, err = validateVMResultAtCanonicalBoundary(base, dynProps, 0, result, cfg)
	if canonical != nil || !errors.Is(err, errSpeculativePublicationAudit) || !errors.Is(err, errCanonicalOracleRestoration) {
		t.Fatalf("isolated-copy VM oracle leak was not rejected: canonical=%+v err=%v", canonical, err)
	}
	if got := parallelVMSerialVerifyRestoreMismatchCounter.Snapshot().Count() - restoreMismatchesBefore; got != 1 {
		t.Fatalf("isolated VM restoration mismatches = %d, want 1", got)
	}
	if got := base.DomainChangeJournalMark(); got != journalBefore {
		t.Fatalf("isolated VM fault changed journal mark: got %d want %d", got, journalBefore)
	}
	if got := base.GetBalance(params.BlackholeAddress); got != blackholeBefore {
		t.Fatalf("isolated-copy leak was detected but not self-cleaned: got %d want %d", got, blackholeBefore)
	}
	if got := dynProps.TotalNetWeight(); got != totalNetWeightBefore {
		t.Fatalf("isolated VM fault changed dynamic properties: got %d want %d", got, totalNetWeightBefore)
	}
}

func TestCloneTransactionWriteSetOwnsAsyncHandoffMapAndValues(t *testing.T) {
	key := state.TransactionAccessKey{Kind: state.TransactionAccessAccountField, Address: testProcessorAddr(1)}
	original := state.TransactionWriteSet{key: {Exists: true, Value: []byte("canonical")}}
	handoff := cloneTransactionWriteSet(original)
	value := handoff[key]
	value.Value[0] = 'X'
	handoff[key] = value
	delete(handoff, key)

	if got := string(original[key].Value); got != "canonical" {
		t.Fatalf("handoff mutation changed worker-owned value: %q", got)
	}
	if len(original) != 1 {
		t.Fatalf("handoff mutation changed worker-owned map length: %d", len(original))
	}
}

func TestCanonicalPublicationWriteSetAuditFailsClosed(t *testing.T) {
	key := state.TransactionAccessKey{Kind: state.TransactionAccessAccountField, Address: testProcessorAddr(1)}
	expected := state.TransactionWriteSet{key: {Exists: true, Value: []byte("expected")}}
	versioned := &versionedAccessShadow{
		transactionWritesOK:  []bool{true},
		transactionWriteSets: []state.TransactionWriteSet{cloneTransactionWriteSet(expected)},
	}
	if err := validateCanonicalPublicationWriteSet("Transfer", 0, expected, versioned); err != nil {
		t.Fatalf("matching publication audit failed: %v", err)
	}
	versioned.transactionWriteSets[0][key] = state.TransactionWriteValue{Exists: true, Value: []byte("stale")}
	if err := validateCanonicalPublicationWriteSet("Transfer", 0, expected, versioned); err == nil {
		t.Fatal("publication mismatch was accepted")
	}
	versioned.transactionWritesOK[0] = false
	if err := validateCanonicalPublicationWriteSet("VM", 0, expected, versioned); err == nil {
		t.Fatal("missing canonical capture was accepted")
	}

	if err := validatePublishedRetryAudit("Transfer", discardShadowSenderRetryStats{
		publish: discardShadowAsyncPublishStats{published: 1, writeMatches: 1},
	}); err != nil {
		t.Fatalf("matching retry audit failed: %v", err)
	}
	if err := validatePublishedRetryAudit("VM", discardShadowSenderRetryStats{
		publish: discardShadowAsyncPublishStats{published: 1, writeMismatches: 1},
	}); err == nil {
		t.Fatal("retry post-publication mismatch was accepted")
	}
}

func TestBoundaryOracleCrossCheckRequiresAllConsumedOutputs(t *testing.T) {
	key := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		Address:      testProcessorAddr(1),
		AccountField: state.TransactionAccountFieldBalance,
	}
	base := &discardShadowTaskResult{
		info: &corepb.TransactionInfo{Fee: 7, InternalTransactions: []*corepb.InternalTransaction{{
			Hash: []byte{0x01}, Note: []byte("call"),
		}}},
		writes:       state.TransactionWriteSet{key: {Exists: true, Value: make([]byte, 8)}},
		reads:        state.TransactionReadSet{Reads: []state.TransactionRead{{Key: key, Mode: state.TransactionAccessRead}}},
		balanceTrace: &contractpb.TransactionBalanceTrace{TransactionIdentifier: []byte{1}},
	}
	clone := func() *discardShadowTaskResult {
		return &discardShadowTaskResult{
			info:         proto.Clone(base.info).(*corepb.TransactionInfo),
			writes:       cloneTransactionWriteSet(base.writes),
			reads:        cloneTransactionReadSet(base.reads),
			balanceTrace: proto.Clone(base.balanceTrace).(*contractpb.TransactionBalanceTrace),
		}
	}
	if check := compareBoundaryCanonicalResults(base, clone()); !check.matched() || !check.readMatch {
		t.Fatalf("equal canonical results rejected: %+v", check)
	}
	readDifferent := clone()
	readDifferent.reads.Reads = nil
	if check := compareBoundaryCanonicalResults(base, readDifferent); !check.matched() || check.readMatch {
		t.Fatalf("diagnostic-only read difference misclassified: %+v", check)
	}
	for _, test := range []struct {
		name   string
		mutate func(*discardShadowTaskResult)
	}{
		{name: "info", mutate: func(result *discardShadowTaskResult) { result.info.Fee++ }},
		{name: "internal_transaction", mutate: func(result *discardShadowTaskResult) {
			result.info.InternalTransactions[0].Note[0]++
		}},
		{name: "writes", mutate: func(result *discardShadowTaskResult) {
			value := result.writes[key]
			value.Value[7] = 1
			result.writes[key] = value
		}},
		{name: "balance_trace", mutate: func(result *discardShadowTaskResult) {
			result.balanceTrace.TransactionIdentifier[0]++
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			other := clone()
			test.mutate(other)
			if check := compareBoundaryCanonicalResults(base, other); check.matched() {
				t.Fatalf("%s mismatch accepted: %+v", test.name, check)
			}
		})
	}
	if check := compareBoundaryCanonicalResults(base, nil); check.err == nil || check.matched() {
		t.Fatalf("missing independent result accepted: %+v", check)
	}
}

func TestVMCanonicalBoundarySerialVerificationRejectsMutatedStorageWrite(t *testing.T) {
	base := newTestState(t)
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x84)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	base.SetCode(contractAddr, []byte{0x60, 0x00, 0x35, 0x60, 0x00, 0x55, 0x00})
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}

	input := make([]byte, tcommon.HashLength)
	input[len(input)-1] = 9
	tx := makeTestTriggerTx(1, contractAddr, input)
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	cfg := discardShadowRunConfig{
		block:                   block,
		db:                      ethrawdb.NewMemoryDatabase(),
		transactions:            []*types.Transaction{tx},
		energyLimitForkBlockNum: params.DefaultBlockNumForEnergyLimit,
		retainInfos:             true,
	}
	shadow := prepareTransferExecutionBlock(base, base.DynamicProperties(), block.Number(), false)
	if shadow == nil {
		t.Fatal("missing sampled VM execution base")
	}
	pre := shadow.preexecuteVMSenderChains(cfg, false)
	if pre == nil || len(pre.results) != 1 || !preexecutedResultReady(&pre.results[0]) {
		t.Fatalf("VM preexecution unavailable: %+v", pre)
	}
	result := &pre.results[0]
	override, admitted := overridePublicNetReservation(result, base.DynamicProperties())
	if !admitted {
		t.Fatal("block-start public-net reservation was not admitted at the same boundary")
	}
	defer override.restore()

	commitment := func() tcommon.Hash {
		copyState, copyErr := base.Copy()
		if copyErr != nil {
			t.Fatal(copyErr)
		}
		root, commitErr := copyState.Commit()
		if commitErr != nil {
			t.Fatal(commitErr)
		}
		return root
	}
	rootBefore := commitment()
	intsBefore := base.DynamicProperties().All()
	stringsBefore := make(map[string]string, len(base.DynamicProperties().StringKeys()))
	for _, key := range base.DynamicProperties().StringKeys() {
		stringsBefore[key], _ = base.DynamicProperties().GetString(key)
	}
	journalBefore := base.DomainChangeJournalMark()
	var outerRecorder state.TransactionAccessRecorder
	outerRecorder.Reset(32)
	base.SetTransactionAccessRecorder(&outerRecorder)
	base.DynamicProperties().SetTransactionAccessRecorder(&outerRecorder)
	verification := verifyVMResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg)
	if !verification.matched() {
		t.Fatalf("matching VM boundary result was rejected: %+v", verification)
	}
	if verification.crossCheck == nil || !verification.crossCheck.matched() {
		t.Fatalf("VM isolated/direct cross-check missing: %+v", verification.crossCheck)
	}
	_ = base.GetBalance(owner)
	if reads := outerRecorder.CaptureReadSet(); len(reads.Reads) == 0 {
		t.Fatal("VM direct oracle did not restore the outer recorder")
	}
	base.SetTransactionAccessRecorder(nil)
	base.DynamicProperties().SetTransactionAccessRecorder(nil)
	if rootAfter := commitment(); rootAfter != rootBefore {
		t.Fatalf("VM direct oracle changed commitment: before=%x after=%x", rootBefore, rootAfter)
	}
	if journalAfter := base.DomainChangeJournalMark(); journalAfter != journalBefore {
		t.Fatalf("VM direct oracle changed domain journal: before=%d after=%d", journalBefore, journalAfter)
	}
	if !maps.Equal(intsBefore, base.DynamicProperties().All()) {
		t.Fatal("VM direct oracle changed integer dynamic properties")
	}
	stringsAfter := make(map[string]string, len(base.DynamicProperties().StringKeys()))
	for _, key := range base.DynamicProperties().StringKeys() {
		stringsAfter[key], _ = base.DynamicProperties().GetString(key)
	}
	if !maps.Equal(stringsBefore, stringsAfter) {
		t.Fatal("VM direct oracle changed string dynamic properties")
	}
	if got := base.GetState(contractAddr, tcommon.Hash{}); got != (tcommon.Hash{}) {
		t.Fatalf("VM direct oracle left storage behind: %x", got)
	}
	result.balanceTrace = &contractpb.TransactionBalanceTrace{TransactionIdentifier: []byte("unexpected")}
	verification = verifyVMResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg)
	if verification.matched() || verification.balanceMatch || !verification.infoMatch || !verification.writeMatch || verification.err != nil {
		t.Fatalf("trace-disabled oracle admitted an unauthenticated trace: %+v", verification)
	}
	result.balanceTrace = nil
	storageKey := state.TransactionAccessKey{
		Kind: state.TransactionAccessStorage, Address: contractAddr, StorageKey: tcommon.Hash{},
	}
	storageWrite, ok := result.writes[storageKey]
	if !ok {
		t.Fatalf("VM result has no storage write: %+v", result.writes)
	}
	storageWrite.Value = make([]byte, tcommon.HashLength)
	storageWrite.Value[len(storageWrite.Value)-1] = 0xff
	result.writes[storageKey] = storageWrite

	verification = verifyVMResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg)
	if verification.matched() || verification.writeMatch || !verification.infoMatch || verification.err != nil {
		t.Fatalf("mutated storage write was not isolated as a boundary mismatch: %+v", verification)
	}
	if canonical, err := validateVMResultAtCanonicalBoundary(base, base.DynamicProperties(), 0, result, cfg); canonical != nil || !errors.Is(err, errSpeculativePublicationAudit) {
		t.Fatalf("VM serial-oracle mismatch did not open safety path: canonical=%+v err=%v", canonical, err)
	}
}

func TestVMCanonicalBoundarySerialVerificationUsesAuthoritativeCachedAccount(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	base, err := state.New(tcommon.Hash{}, state.NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	dynProps := base.DynamicProperties()
	dynProps.SetAllowCreationOfContracts(true)
	dynProps.SetAllowAdaptiveEnergy(true)
	dynProps.SetAllowBlackHoleOptimization(true)
	dynProps.SetLatestBlockHeaderTimestamp(30_000)
	passVersion3_6_5(base, 27)

	owner := testProcessorAddr(1)
	contractAddr := testProcessorAddr(0x85)
	base.CreateAccount(owner, corepb.AccountType_Normal)
	base.AddBalance(owner, 100_000_000)
	base.CreateAccount(params.BlackholeAddress, corepb.AccountType_Normal)
	base.CreateAccount(contractAddr, corepb.AccountType_Contract)
	base.SetContract(contractAddr, &contractpb.SmartContract{
		OriginAddress: owner.Bytes(), ContractAddress: contractAddr.Bytes(),
	})
	base.SetCode(contractAddr, []byte{0x60, 0x01, 0x60, 0x02, 0x01, 0x50, 0x00})
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	if got := base.GetBalance(owner); got != 100_000_000 {
		t.Fatalf("cached owner balance = %d, want 100000000", got)
	}

	tx := makeTestTriggerTx(1, contractAddr, nil)
	tx.Proto().RawData.FeeLimit = 10_000_000
	tx.Proto().Ret = []*corepb.Transaction_Result{{ContractRet: corepb.Transaction_Result_SUCCESS}}
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
			Number: int64(vmSenderChainPublishInterval), Timestamp: 33_000,
		}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	cfg := discardShadowRunConfig{
		block:                   block,
		db:                      ethrawdb.NewMemoryDatabase(),
		transactions:            []*types.Transaction{tx},
		energyLimitForkBlockNum: params.DefaultBlockNumForEnergyLimit,
		retainInfos:             true,
	}
	shadow := prepareTransferExecutionBlock(base, dynProps, block.Number(), false)
	pre := shadow.preexecuteVMSenderChains(cfg, false)
	if pre == nil || len(pre.results) != 1 || !preexecutedResultReady(&pre.results[0]) {
		t.Fatalf("VM preexecution unavailable: %+v", pre)
	}
	result := &pre.results[0]
	override, admitted := overridePublicNetReservation(result, dynProps)
	if !admitted {
		t.Fatal("public-net reservation was not admitted")
	}
	defer override.restore()

	// Model a cache/backing-store visibility gap. The canonical StateDB still
	// owns the exact account used by normal serial execution. An oracle built
	// with CopyBlockExecutionBase would omit that clean account and share the
	// speculative copy's failure mode; the full-copy oracle must remain exact.
	if err := rawdb.DeleteStateAccountLatest(disk, owner); err != nil {
		t.Fatal(err)
	}
	if got := base.GetBalance(owner); got != 100_000_000 {
		t.Fatalf("authoritative cached owner balance after hot-row removal = %d", got)
	}
	verification := verifyVMResultAtCanonicalBoundary(base, dynProps, 0, result, cfg)
	if !verification.matched() {
		t.Fatalf("full-copy boundary oracle lost authoritative cached state: %+v", verification)
	}
}

func TestVMCanonicalBoundarySerialOracleRejectionIsObservable(t *testing.T) {
	var buf bytes.Buffer
	previousLogger := gtronlog.Root()
	t.Cleanup(func() { gtronlog.SetDefault(previousLogger) })
	gtronlog.SetDefault(gtronlog.NewLogger(gtronlog.LogfmtHandlerWithLevel(&buf, gtronlog.LevelWarn)))

	contractAddr := testProcessorAddr(0x86)
	tx := makeTestTriggerTx(1, contractAddr, nil)
	block := types.NewBlockFromPB(&corepb.Block{
		BlockHeader:  &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{Number: 1234}},
		Transactions: []*corepb.Transaction{tx.Proto()},
	})
	logVMSerialOracleRejection(discardShadowRunConfig{
		block:        block,
		transactions: []*types.Transaction{tx},
	}, 0, boundarySerialVerification{
		infoMatch:    true,
		writeMatch:   false,
		readMatch:    true,
		balanceMatch: true,
		err:          errors.New("injected oracle failure"),
	})

	output := buf.String()
	for _, want := range []string{
		"Speculative VM result rejected by canonical serial oracle",
		"module=core/chain",
		"block=1234",
		"txIndex=0",
		"infoMatch=true",
		"writeSetMatch=false",
		"readSetMatch=true",
		"balanceTraceMatch=true",
		"action=block-rollback-persistent-serial-circuit",
		"injected oracle failure",
	} {
		if !strings.Contains(output, want) {
			t.Fatalf("oracle rejection log missing %q:\n%s", want, output)
		}
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
	result.writes[usageKey] = encodeInt(300)
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

func TestPublicNetWriteOverrideRejectsMalformedRetainedWriteSet(t *testing.T) {
	usageKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_usage"}
	timeKey := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "public_net_time"}
	encodeInt := func(value int64) state.TransactionWriteValue {
		encoded := make([]byte, 8)
		binary.BigEndian.PutUint64(encoded, uint64(value))
		return state.TransactionWriteValue{Exists: true, Value: encoded}
	}
	dynProps := state.NewDynamicProperties()
	dynProps.SetPublicNetLimit(1_000)
	dynProps.SetPublicNetUsage(50)
	dynProps.SetPublicNetTime(5)
	base := discardShadowTaskResult{
		publicNetValid: true,
		publicNet: state.PublicNetReservation{
			StartUsage: 50, StartTime: 5, RecoveredUsage: 50, ResourceTime: 10, Delta: 100, Limit: 1_000,
		},
	}
	tests := []struct {
		name   string
		dp     *state.DynamicProperties
		writes state.TransactionWriteSet
	}{
		{name: "nil dynamic properties", writes: state.TransactionWriteSet{usageKey: encodeInt(150), timeKey: encodeInt(10)}},
		{name: "missing usage", dp: dynProps, writes: state.TransactionWriteSet{timeKey: encodeInt(10)}},
		{name: "short usage", dp: dynProps, writes: state.TransactionWriteSet{usageKey: {Exists: true, Value: []byte{1}}, timeKey: encodeInt(10)}},
		{name: "commutative usage", dp: dynProps, writes: state.TransactionWriteSet{usageKey: {Exists: true, Commutative: true, Value: make([]byte, 8)}, timeKey: encodeInt(10)}},
		{name: "short time", dp: dynProps, writes: state.TransactionWriteSet{usageKey: encodeInt(150), timeKey: {Exists: true, Value: []byte{1}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := base
			result.writes = tt.writes
			before := cloneTransactionWriteSet(result.writes)
			override, admitted := overridePublicNetReservation(&result, tt.dp)
			if admitted {
				t.Fatal("malformed retained WriteSet was admitted")
			}
			if !override.reservation {
				t.Fatal("rejected result did not retain reservation classification")
			}
			if !state.EqualTransactionWriteSets(result.writes, before) {
				t.Fatal("rejected override mutated the retained WriteSet")
			}
		})
	}
}

func TestCanonicalPublicationSealRejectsConsumedPayloadMutation(t *testing.T) {
	key := state.TransactionAccessKey{
		Kind:         state.TransactionAccessAccountField,
		Address:      testProcessorAddr(1),
		AccountField: state.TransactionAccountFieldBalance,
	}
	base := discardShadowTaskResult{
		writes: state.TransactionWriteSet{key: {Exists: true, Value: make([]byte, 8)}},
		reads: state.TransactionReadSet{Reads: []state.TransactionRead{{
			Key: key, Mode: state.TransactionAccessRead,
		}}},
		info: &corepb.TransactionInfo{Fee: 7},
		balanceTrace: &contractpb.TransactionBalanceTrace{
			TransactionIdentifier: []byte("tx"),
		},
	}
	tests := []struct {
		name   string
		mutate func(*discardShadowTaskResult)
	}{
		{name: "info", mutate: func(result *discardShadowTaskResult) {
			result.info.Fee++
		}},
		{name: "balance trace", mutate: func(result *discardShadowTaskResult) {
			result.balanceTrace.TransactionIdentifier[0] ^= 1
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := base
			result.writes = cloneTransactionWriteSet(base.writes)
			result.reads = cloneTransactionReadSet(base.reads)
			result.info = proto.Clone(base.info).(*corepb.TransactionInfo)
			result.balanceTrace = proto.Clone(base.balanceTrace).(*contractpb.TransactionBalanceTrace)
			canonical := result
			canonical.writes = cloneTransactionWriteSet(result.writes)
			canonical.reads = cloneTransactionReadSet(result.reads)
			canonical.info = proto.Clone(result.info).(*corepb.TransactionInfo)
			canonical.balanceTrace = proto.Clone(result.balanceTrace).(*contractpb.TransactionBalanceTrace)
			seal, err := newCanonicalPublicationWriteSeal("Transfer", &result, &canonical)
			if err != nil {
				t.Fatal(err)
			}
			tt.mutate(&result)
			if err := seal.validateSource("test", &result); !errors.Is(err, errSpeculativePublicationAudit) {
				t.Fatalf("mutation error = %v, want speculative safety sentinel", err)
			}
			if seal.info.GetFee() != 7 || string(seal.balanceTrace.GetTransactionIdentifier()) != "tx" || seal.reads.Unsupported {
				t.Fatal("source mutation escaped into the private publication seal")
			}
		})
	}

	result := base
	result.writes = cloneTransactionWriteSet(base.writes)
	result.reads = cloneTransactionReadSet(base.reads)
	result.info = proto.Clone(base.info).(*corepb.TransactionInfo)
	result.balanceTrace = proto.Clone(base.balanceTrace).(*contractpb.TransactionBalanceTrace)
	canonical := result
	canonical.writes = cloneTransactionWriteSet(result.writes)
	canonical.reads = cloneTransactionReadSet(result.reads)
	canonical.info = proto.Clone(result.info).(*corepb.TransactionInfo)
	canonical.balanceTrace = proto.Clone(result.balanceTrace).(*contractpb.TransactionBalanceTrace)
	seal, err := newCanonicalPublicationWriteSeal("Transfer", &result, &canonical)
	if err != nil {
		t.Fatal(err)
	}
	result.reads.Unsupported = true
	result.reads.Reads = nil
	if err := seal.validateSource("test", &result); err != nil {
		t.Fatalf("unused speculative read mutation rejected canonical payload: %v", err)
	}
	if seal.reads.Unsupported || len(seal.reads.Reads) != 1 {
		t.Fatal("speculative read mutation escaped into the canonical serial read carrier")
	}
}

func TestCanonicalReadSetComparisonIgnoresOnlySchedulerMetadata(t *testing.T) {
	first := state.TransactionAccessKey{Kind: state.TransactionAccessAccountField, Address: testProcessorAddr(1), AccountField: state.TransactionAccountFieldBalance}
	second := state.TransactionAccessKey{Kind: state.TransactionAccessDynamicInt, LogicalKey: "transaction_fee"}
	canonical := state.TransactionReadSet{Reads: []state.TransactionRead{
		{Key: first, Mode: state.TransactionAccessRead},
		{Key: second, Mode: state.TransactionAccessCommutativeRead},
	}}
	scheduled := state.TransactionReadSet{Reads: []state.TransactionRead{
		{Key: second, Mode: state.TransactionAccessCommutativeRead, ExpectedWriter: 3, HasExpectedWriter: true},
		{Key: first, Mode: state.TransactionAccessRead, ExpectedWriter: 1, HasExpectedWriter: true},
	}}
	if !equalTransactionReadSetAccesses(canonical, scheduled) {
		t.Fatal("scheduler metadata or order changed canonical read-set semantics")
	}
	scheduled.Reads = scheduled.Reads[:1]
	if equalTransactionReadSetAccesses(canonical, scheduled) {
		t.Fatal("missing read was accepted as canonical-equivalent")
	}
	scheduled = cloneTransactionReadSet(canonical)
	scheduled.Unsupported = true
	if equalTransactionReadSetAccesses(canonical, scheduled) {
		t.Fatal("unsupported read-set marker was ignored")
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

	// The publication-side value must be recomputed from the sealed canonical
	// receipt. A corrupted speculative expectation must fail the audit instead
	// of being assigned into DynamicProperties and then compared to itself.
	dynProps.SetBlockEnergyUsage(40)
	pre.blockEnergy[0] = discardShadowBlockEnergyProjection{
		observed: true,
		baseline: 40,
		expected: 1_041,
	}
	accumulateBlockEnergyUsageFromReceipt(dynProps, stats, 0, pre.results[0].info.GetReceipt(), nil)
	pre.validateBlockEnergyBoundary(0, dynProps)
	if pre.blockEnergy[0].match || dynProps.BlockEnergyUsage() != 1_040 {
		t.Fatalf("corrupt projection audit = %+v, canonical=%d; want mismatch at independently accumulated 1040",
			pre.blockEnergy[0], dynProps.BlockEnergyUsage())
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

func TestSenderChainWorkerVersionMapDoesNotLeakAcrossIndependentChains(t *testing.T) {
	base := newTestState(t)
	const chainCount = 5 // Four workers guarantee that one worker accepts a second job.
	recipient := testProcessorAddr(20)
	base.CreateAccount(recipient, corepb.AccountType_Normal)
	transactions := make([]*types.Transaction, 0, chainCount)
	blockPB := &corepb.Block{BlockHeader: &corepb.BlockHeader{RawData: &corepb.BlockHeaderRaw{
		Number: 1, Timestamp: 3_000,
	}}}
	for ownerID := byte(1); ownerID <= chainCount; ownerID++ {
		owner := testProcessorAddr(ownerID)
		base.CreateAccount(owner, corepb.AccountType_Normal)
		base.AddBalance(owner, 10_000_000)
		tx := makeTestTransferTx(ownerID, 20, 1_000)
		transactions = append(transactions, tx)
		blockPB.Transactions = append(blockPB.Transactions, tx.Proto())
	}
	if _, err := base.Commit(); err != nil {
		t.Fatal(err)
	}
	base.SetDynamicProperties(base.DynamicProperties().Copy())
	chains := transferSenderChains(transactions)
	shadow := &discardShadowBlock{base: base}
	pre := shadow.preexecuteSenderChainsWithRetryState(discardShadowRunConfig{
		block: types.NewBlockFromPB(blockPB), transactions: transactions,
	}, chains, preexecutedTransferReady, false, false)
	if pre == nil || len(pre.results) != chainCount {
		t.Fatalf("sender-chain results = %+v", pre)
	}
	for _, result := range pre.results {
		for _, read := range result.reads.Reads {
			if read.HasExpectedWriter {
				t.Fatalf("independent chain tx %d inherited writer %d for %+v", result.txIndex, read.ExpectedWriter, read.Key)
			}
		}
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
