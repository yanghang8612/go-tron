package core

import (
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/tronprotocol/go-tron/core/forks"
	"github.com/tronprotocol/go-tron/core/state"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
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
	got := worker.execute(0, discardShadowRunConfig{
		block:              block,
		transactions:       []*types.Transaction{tx},
		canonicalInfos:     []*corepb.TransactionInfo{canonicalInfo},
		canonicalWriteSets: []state.TransactionWriteSet{canonicalWriteSet},
		genesisTimestamp:   0,
	})
	if got.err != nil || !got.matched || got.writeSetErr != nil || !got.writeSetMatch || !got.applyEligible || got.applyErr != nil || !got.applyMatch {
		t.Fatalf("discard worker = info-matched:%v writes-matched:%v apply-eligible:%v apply-matched:%v err:%v write-err:%v apply-err:%v", got.matched, got.writeSetMatch, got.applyEligible, got.applyMatch, got.err, got.writeSetErr, got.applyErr)
	}
	if balance := workerState.GetBalance(owner); balance != 10_000_000 {
		t.Fatalf("discard worker owner balance = %d, want 10000000", balance)
	}
	if balance := workerState.GetBalance(recipient); balance != 0 {
		t.Fatalf("discard worker recipient balance = %d, want 0", balance)
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
