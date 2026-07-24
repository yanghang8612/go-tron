package state

import (
	"bytes"
	"errors"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

var errDomainChangeSetWriterUsed = errors.New("domain change set writer used")

var benchmarkPreparedDomainChange *rawdb.StateDomainChange

type failingDomainChangeSetWriter struct{}

func (failingDomainChangeSetWriter) Put([]byte, []byte) error { return errDomainChangeSetWriterUsed }
func (failingDomainChangeSetWriter) Delete([]byte) error      { return errDomainChangeSetWriterUsed }

type capturingDomainChangePublisher struct {
	changes []*rawdb.StateDomainChange
}

func (p *capturingDomainChangePublisher) PublishStateDomainChanges(changes []*rawdb.StateDomainChange) error {
	p.changes = append(p.changes, changes...)
	return nil
}

func TestDefaultStateDomainChangePublicationConfigUsesRegisteredHistoryDomain(t *testing.T) {
	cfg := DefaultStateDomainChangePublicationConfig()
	if cfg.Name != "HistoryDomain" || cfg.WriteTxRange == nil || cfg.WriteRow == nil || cfg.WriteInverseIndex == nil {
		t.Fatalf("default publication config = %+v", cfg)
	}
}

func TestPrepareDomainChangeStampsFreshChangeInPlace(t *testing.T) {
	capture := domainChangeSetCapture{
		enabled:   true,
		blockNum:  10,
		blockHash: tcommon.Hash{0x10},
		txNum:     42,
	}
	change := &rawdb.StateDomainChange{
		FlatDomain: rawdb.StateFlatDomainAccountLatest,
		Owner:      tcommon.Address{0x41, 0x22},
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("new"),
	}
	prepared := capture.prepareDomainChange(change)
	if prepared != change {
		t.Fatal("prepareDomainChange copied a freshly owned change")
	}
	if prepared.BlockNum != 10 || prepared.BlockHash != (tcommon.Hash{0x10}) || prepared.TxNum != 42 || prepared.Seq != 1 {
		t.Fatalf("prepared metadata = %+v", prepared)
	}
}

func TestStateDomainChangeRunnerUsesConfiguredPublicationSteps(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	var calls []string
	cfg := StateDomainChangePublicationConfig{
		Name: "custom-history",
		WriteTxRange: func(writer ethdb.KeyValueWriter, blockNum uint64, blockHash tcommon.Hash, beginTxNum, endTxNum uint64) error {
			if writer != db {
				t.Fatalf("tx-range writer mismatch")
			}
			if blockNum != 9 || blockHash != (tcommon.Hash{0x09}) || beginTxNum != 90 || endTxNum != 99 {
				t.Fatalf("tx-range = block %d hash %x [%d,%d]", blockNum, blockHash, beginTxNum, endTxNum)
			}
			calls = append(calls, "tx-range")
			return nil
		},
		WriteRow: func(writer ethdb.KeyValueWriter, change *rawdb.StateDomainChange) error {
			if writer != db {
				t.Fatalf("row writer mismatch")
			}
			if change == nil || change.Seq != 1 {
				t.Fatalf("row change = %+v", change)
			}
			calls = append(calls, "row")
			return nil
		},
		WriteInverseIndex: func(writer ethdb.KeyValueWriter, change *rawdb.StateDomainChange) error {
			if writer != db {
				t.Fatalf("index writer mismatch")
			}
			if change == nil || change.Seq != 1 {
				t.Fatalf("index change = %+v", change)
			}
			calls = append(calls, "index")
			return nil
		},
	}
	change := &rawdb.StateDomainChange{
		BlockNum:   1,
		TxNum:      1,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      tcommon.SystemAccountAddress,
		Domain:     kvdomains.SystemReward,
		Key:        []byte("reward/runner"),
		NextExists: true,
		Next:       []byte("value"),
	}
	if err := NewStateDomainChangeRunner(db, cfg).PublishStateTxRange(9, tcommon.Hash{0x09}, 90, 99); err != nil {
		t.Fatalf("publish tx-range with configured runner: %v", err)
	}
	if err := NewStateDomainChangeRunner(db, cfg).PublishStateDomainChanges([]*rawdb.StateDomainChange{change}); err != nil {
		t.Fatalf("publish with configured runner: %v", err)
	}
	if len(calls) != 3 || calls[0] != "tx-range" || calls[1] != "row" || calls[2] != "index" {
		t.Fatalf("publication calls = %v, want [tx-range row index]", calls)
	}
}

func TestStateDBDomainChangeSetCapturesAccountKVAndStorage(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	systemKey := []byte("reward/key")
	systemValue := []byte("reward-v1")
	contract := testAddr(0x77)
	slot := tcommon.Hash{0x01}
	slotValue := tcommon.Hash{0x99}

	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, systemKey, systemValue); err != nil {
		t.Fatal(err)
	}
	sdb.CreateAccount(contract, corepb.AccountType_Contract)
	sdb.SetState(contract, slot, slotValue)

	changeDB := ethrawdb.NewMemoryDatabase()
	blockHash := tcommon.Hash{0x05}
	beginTxNum, endTxNum, err := rawdb.NextStateTxRange(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	sdb.SetDomainChangeSetWriterRange(changeDB, 5, blockHash, beginTxNum, endTxNum)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	txRange, ok, err := rawdb.ReadStateTxRange(changeDB, 5)
	if err != nil || !ok {
		t.Fatalf("tx range = ok:%v err:%v", ok, err)
	}
	if txRange.BeginTxNum != beginTxNum || txRange.EndTxNum != endTxNum || txRange.BlockHash != blockHash {
		t.Fatalf("tx range = %+v", txRange)
	}

	changes := collectStateDomainChanges(t, changeDB, 5)
	for _, change := range changes {
		if change.TxNum != endTxNum {
			t.Fatalf("change txNum = %d, want block-final %d", change.TxNum, endTxNum)
		}
	}
	if !hasDomainChange(changes, tcommon.SystemAccountAddress, kvdomains.SystemReward, systemKey, false, nil, true, systemValue) {
		t.Fatalf("system reward change not captured: %+v", changes)
	}
	rowKey := sdb.storageRowKey(contract, slot).Bytes()
	if !hasDomainChange(changes, contract, kvdomains.ContractStorage, rowKey, false, nil, true, slotValue.Bytes()) {
		t.Fatalf("contract storage change not captured: %+v", changes)
	}

	reopened, err := New(root, NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	nextValue := []byte("reward-v2")
	if err := reopened.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, systemKey, nextValue); err != nil {
		t.Fatal(err)
	}
	nextChangeDB := ethrawdb.NewMemoryDatabase()
	reopened.SetDomainChangeSetWriter(nextChangeDB, 6, tcommon.Hash{0x06})
	if _, err := reopened.Commit(); err != nil {
		t.Fatalf("second commit: %v", err)
	}
	changes = collectStateDomainChanges(t, nextChangeDB, 6)
	if !hasDomainChange(changes, tcommon.SystemAccountAddress, kvdomains.SystemReward, systemKey, true, systemValue, true, nextValue) {
		t.Fatalf("system reward update pre-value not captured: %+v", changes)
	}
}

func TestStateDBDomainChangeSetTxNumOverride(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("reward/txnum")
	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	changeDB := ethrawdb.NewMemoryDatabase()
	beginTxNum, endTxNum, err := rawdb.NextStateTxRange(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	sdb.SetDomainChangeSetWriterRange(changeDB, 9, tcommon.Hash{0x09}, beginTxNum, endTxNum)
	sdb.SetDomainChangeTxNum(beginTxNum + 1)
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}
	txRange, ok, err := rawdb.ReadStateTxRange(changeDB, 9)
	if err != nil || !ok {
		t.Fatalf("tx range = ok:%v err:%v", ok, err)
	}
	if txRange.BeginTxNum != beginTxNum || txRange.EndTxNum != endTxNum {
		t.Fatalf("tx range = %+v, want [%d,%d]", txRange, beginTxNum, endTxNum)
	}
	changes := collectStateDomainChanges(t, changeDB, 9)
	if len(changes) == 0 {
		t.Fatal("no domain changes captured")
	}
	for _, change := range changes {
		if change.TxNum != beginTxNum+1 {
			t.Fatalf("change txNum = %d, want %d", change.TxNum, beginTxNum+1)
		}
	}
}

func TestDomainChangeStagePublishesTxAndBlockFinalRows(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	changeDB := ethrawdb.NewMemoryDatabase()
	beginTxNum, endTxNum, err := rawdb.NextStateTxRange(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	blockHash := tcommon.Hash{0x07}
	stage, err := sdb.BeginDomainChangeStage(changeDB, &rawdb.StateTxRange{
		BlockNum:   7,
		BlockHash:  blockHash,
		BeginTxNum: beginTxNum,
		EndTxNum:   endTxNum,
	})
	if err != nil {
		t.Fatalf("begin stage: %v", err)
	}

	txKey := []byte("reward/tx")
	txValue := []byte("tx-value")
	txMark := stage.JournalMark()
	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, txKey, txValue); err != nil {
		t.Fatal(err)
	}
	if err := stage.FlushOrdinal(txMark, 0); err != nil {
		t.Fatalf("flush tx ordinal: %v", err)
	}

	finalKey := []byte("reward/final")
	finalValue := []byte("final-value")
	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, finalKey, finalValue); err != nil {
		t.Fatal(err)
	}
	if err := stage.FlushFinal(); err != nil {
		t.Fatalf("flush final: %v", err)
	}
	if _, err := sdb.Commit(); err != nil {
		t.Fatalf("commit: %v", err)
	}

	txRange, ok, err := rawdb.ReadStateTxRange(changeDB, 7)
	if err != nil || !ok {
		t.Fatalf("tx range = ok:%v err:%v", ok, err)
	}
	if txRange.BeginTxNum != beginTxNum || txRange.EndTxNum != endTxNum || txRange.BlockHash != blockHash {
		t.Fatalf("tx range = %+v, want hash %x [%d,%d]", txRange, blockHash, beginTxNum, endTxNum)
	}
	changes := collectStateDomainChanges(t, changeDB, 7)
	seenKV := 0
	for _, change := range changes {
		if change.FlatDomain != rawdb.StateFlatDomainKVLatest || change.Domain != kvdomains.SystemReward {
			continue
		}
		seenKV++
		switch string(change.Key) {
		case string(txKey):
			if change.TxNum != beginTxNum {
				t.Fatalf("tx change txNum = %d, want %d", change.TxNum, beginTxNum)
			}
		case string(finalKey):
			if change.TxNum != endTxNum {
				t.Fatalf("final change txNum = %d, want %d", change.TxNum, endTxNum)
			}
		default:
			t.Fatalf("unexpected change key %q", change.Key)
		}
	}
	if seenKV != 2 {
		t.Fatalf("system reward KV changes = %d, want 2: %+v", seenKV, changes)
	}
}

func TestDomainChangeStagePublishesThroughStageWriter(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	stageWriter := ethrawdb.NewMemoryDatabase()
	beginTxNum, endTxNum, err := rawdb.NextStateTxRange(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	stage, err := sdb.BeginDomainChangeStage(stageWriter, &rawdb.StateTxRange{
		BlockNum:   11,
		BlockHash:  tcommon.Hash{0x0b},
		BeginTxNum: beginTxNum,
		EndTxNum:   endTxNum,
	})
	if err != nil {
		t.Fatalf("begin stage: %v", err)
	}
	sdb.changeSet.writer = failingDomainChangeSetWriter{}

	key := []byte("reward/stage-writer")
	value := []byte("value")
	mark := stage.JournalMark()
	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, value); err != nil {
		t.Fatal(err)
	}
	if err := stage.FlushOrdinal(mark, 0); err != nil {
		t.Fatalf("flush through stage writer: %v", err)
	}

	changes := collectStateDomainChanges(t, stageWriter, 11)
	if !hasDomainChange(changes, tcommon.SystemAccountAddress, kvdomains.SystemReward, key, false, nil, true, value) {
		t.Fatalf("stage writer did not receive system reward change: %+v", changes)
	}
	var blocks []uint64
	if err := rawdb.IterateStateDomainChangeBlocks(stageWriter, tcommon.SystemAccountAddress, 0, kvdomains.SystemReward, key, func(blockNum uint64) (bool, error) {
		blocks = append(blocks, blockNum)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate stage inverse index: %v", err)
	}
	if len(blocks) != 1 || blocks[0] != 11 {
		t.Fatalf("stage inverse blocks = %v, want [11]", blocks)
	}
}

func TestStateDBPublishesCollectedDomainChangesThroughPublisher(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), NewDatabase(disk))
	if err != nil {
		t.Fatal(err)
	}
	beginTxNum, endTxNum, err := rawdb.NextStateTxRange(0, 1)
	if err != nil {
		t.Fatal(err)
	}
	sdb.BeginDomainChangeJournalCapture(disk, 12, tcommon.Hash{0x0c}, beginTxNum, endTxNum)

	key := []byte("reward/publisher")
	value := []byte("value")
	mark := sdb.DomainChangeJournalMark()
	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, value); err != nil {
		t.Fatal(err)
	}
	publisher := new(capturingDomainChangePublisher)
	if err := sdb.publishDomainChangesSince(publisher, mark, beginTxNum); err != nil {
		t.Fatalf("publish through custom publisher: %v", err)
	}
	if !hasDomainChange(publisher.changes, tcommon.SystemAccountAddress, kvdomains.SystemReward, key, false, nil, true, value) {
		t.Fatalf("publisher did not receive collected change: %+v", publisher.changes)
	}
	stored := collectStateDomainChanges(t, disk, 12)
	if len(stored) != 0 {
		t.Fatalf("custom publisher should not write rawdb changes, got %+v", stored)
	}
}

func TestStateDBGetAccountKVAsOfUsesDomainChanges(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	key := []byte("reward/as-of")
	v1 := []byte("v1")
	v2 := []byte("v2")

	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, v1); err != nil {
		t.Fatal(err)
	}
	sdb.SetDomainChangeSetWriter(disk, 1, tcommon.Hash{0x01})
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit block 1: %v", err)
	}

	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, v2); err != nil {
		t.Fatal(err)
	}
	sdb.SetDomainChangeSetWriter(disk, 2, tcommon.Hash{0x02})
	root, err = sdb.Commit()
	if err != nil {
		t.Fatalf("commit block 2: %v", err)
	}

	head, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	got, ok, err := head.GetAccountKVAsOf(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, 1, 2)
	if err != nil || !ok || !bytes.Equal(got, v1) {
		t.Fatalf("as-of block 1 = %q ok:%v err:%v, want %q", got, ok, err, v1)
	}
	got, ok, err = head.GetAccountKVAsOf(tcommon.SystemAccountAddress, kvdomains.SystemReward, key, 2, 2)
	if err != nil || !ok || !bytes.Equal(got, v2) {
		t.Fatalf("as-of block 2 = %q ok:%v err:%v, want %q", got, ok, err, v2)
	}
}

func TestStateDBJournalDomainChangeCaptureUsesTransactionTxNums(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	owner := testAddr(0x7a)
	domain := kvdomains.SystemReward
	key := []byte("reward/per-tx")
	beginTxNum, endTxNum, err := rawdb.NextStateTxRange(0, 2)
	if err != nil {
		t.Fatal(err)
	}
	sdb.BeginDomainChangeJournalCapture(disk, 9, tcommon.Hash{0x09}, beginTxNum, endTxNum)

	mark := sdb.DomainChangeJournalMark()
	if err := sdb.SetAccountKV(owner, domain, key, []byte("v1")); err != nil {
		t.Fatal(err)
	}
	if err := sdb.FlushDomainChangesSince(mark, beginTxNum); err != nil {
		t.Fatalf("flush tx0: %v", err)
	}
	mark = sdb.DomainChangeJournalMark()
	if err := sdb.SetAccountKV(owner, domain, key, []byte("v2")); err != nil {
		t.Fatal(err)
	}
	if err := sdb.FlushDomainChangesSince(mark, beginTxNum+1); err != nil {
		t.Fatalf("flush tx1: %v", err)
	}
	if err := sdb.FlushPendingDomainChanges(endTxNum); err != nil {
		t.Fatalf("flush final: %v", err)
	}
	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	_ = root

	changes := collectStateDomainChanges(t, disk, 9)
	var kvChanges []*rawdb.StateDomainChange
	for _, change := range changes {
		if change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Owner == owner && change.Domain == domain && bytes.Equal(change.Key, key) {
			kvChanges = append(kvChanges, change)
		}
	}
	if len(kvChanges) != 2 {
		t.Fatalf("kv changes = %+v, want 2 transaction changes", kvChanges)
	}
	if kvChanges[0].TxNum != beginTxNum || kvChanges[0].PrevExists || !kvChanges[0].NextExists || !bytes.Equal(kvChanges[0].Next, []byte("v1")) {
		t.Fatalf("tx0 change = %+v", kvChanges[0])
	}
	if kvChanges[1].TxNum != beginTxNum+1 || !kvChanges[1].PrevExists || !bytes.Equal(kvChanges[1].Prev, []byte("v1")) || !bytes.Equal(kvChanges[1].Next, []byte("v2")) {
		t.Fatalf("tx1 change = %+v", kvChanges[1])
	}
	got, ok, err := rawdb.ReadStateAccountKVAsOfTxNum(disk, owner, domain, key, beginTxNum, endTxNum)
	if err != nil || !ok || !bytes.Equal(got, []byte("v1")) {
		t.Fatalf("as-of tx0 = %q ok=%v err=%v", got, ok, err)
	}
}

func TestStateDBJournalDomainChangeCaptureReadsPendingLatestPreviousValue(t *testing.T) {
	sdb := newTestStateDB(t)
	disk := sdb.db.DiskDB()
	owner := testAddr(0x7b)
	domain := kvdomains.SystemReward
	key := []byte("reward/pending-prev")
	v1 := []byte("v1")
	v2 := []byte("v2")
	sdb.CreateAccount(owner, corepb.AccountType_Normal)
	scope := sdb.NewCommitScope()
	defer scope.Close()

	if err := sdb.SetAccountKV(owner, domain, key, v1); err != nil {
		t.Fatalf("set v1: %v", err)
	}
	if _, _, err := sdb.CommitWithStatsOptionsInScope(scope, CommitOptions{
		FlushLatestDomain: func() error { return nil },
	}); err != nil {
		t.Fatalf("commit v1 with deferred latest flush: %v", err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(disk, owner, 0, domain, key); err != nil || ok {
		t.Fatalf("disk latest before scoped flush ok=%v err=%v, want not visible", ok, err)
	}

	beginTxNum, endTxNum, err := rawdb.NextStateTxRange(0, 0)
	if err != nil {
		t.Fatal(err)
	}
	sdb.BeginDomainChangeJournalCapture(disk, 10, tcommon.Hash{0x0a}, beginTxNum, endTxNum)
	if err := sdb.setAccountKVFinalNoRead(owner, domain, key, v2); err != nil {
		t.Fatalf("set v2 without pre-read: %v", err)
	}
	if err := sdb.FlushPendingDomainChanges(endTxNum); err != nil {
		t.Fatalf("flush pending domain changes: %v", err)
	}

	changes := collectStateDomainChanges(t, disk, 10)
	var kvChange *rawdb.StateDomainChange
	for _, change := range changes {
		if change.FlatDomain == rawdb.StateFlatDomainKVLatest && change.Owner == owner && change.Domain == domain && bytes.Equal(change.Key, key) {
			kvChange = change
			break
		}
	}
	if kvChange == nil {
		t.Fatalf("missing pending-latest kv domain change in %+v", changes)
	}
	if !kvChange.PrevExists || !bytes.Equal(kvChange.Prev, v1) || !kvChange.NextExists || !bytes.Equal(kvChange.Next, v2) {
		t.Fatalf("pending-latest kv change = %+v, want prev v1 next v2", kvChange)
	}
}

func TestStateDBIterateAccountKVAsOfUsesDomainChanges(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	owner := tcommon.SystemAccountAddress
	domain := kvdomains.SystemReward
	if err := sdb.SetAccountKV(owner, domain, []byte("reward/a"), []byte("a1")); err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(owner, domain, []byte("reward/b"), []byte("b1")); err != nil {
		t.Fatal(err)
	}
	sdb.SetDomainChangeSetWriter(disk, 1, tcommon.Hash{0x01})
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(owner, domain, []byte("reward/a"), []byte("a2")); err != nil {
		t.Fatal(err)
	}
	if err := sdb.DeleteAccountKV(owner, domain, []byte("reward/b")); err != nil {
		t.Fatal(err)
	}
	if err := sdb.SetAccountKV(owner, domain, []byte("other/c"), []byte("c2")); err != nil {
		t.Fatal(err)
	}
	sdb.SetDomainChangeSetWriter(disk, 2, tcommon.Hash{0x02})
	root, err = sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	head, err := New(root, db)
	if err != nil {
		t.Fatal(err)
	}
	at1 := make(map[string]string)
	if err := head.IterateAccountKVAsOf(owner, domain, []byte("reward/"), 1, 2, func(key, value []byte) (bool, error) {
		at1[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate as-of block 1: %v", err)
	}
	if len(at1) != 2 || at1["reward/a"] != "a1" || at1["reward/b"] != "b1" {
		t.Fatalf("block 1 prefix = %v", at1)
	}
	at2 := make(map[string]string)
	if err := head.IterateAccountKVAsOf(owner, domain, []byte("reward/"), 2, 2, func(key, value []byte) (bool, error) {
		at2[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate as-of block 2: %v", err)
	}
	if len(at2) != 1 || at2["reward/a"] != "a2" {
		t.Fatalf("block 2 prefix = %v", at2)
	}
}

func collectStateDomainChanges(t *testing.T, db ethdb.Iteratee, blockNum uint64) []*rawdb.StateDomainChange {
	t.Helper()
	var changes []*rawdb.StateDomainChange
	if err := rawdb.IterateStateDomainChanges(db, blockNum, func(change *rawdb.StateDomainChange) (bool, error) {
		changes = append(changes, change)
		return true, nil
	}); err != nil {
		t.Fatalf("iterate changes: %v", err)
	}
	return changes
}

func hasDomainChange(changes []*rawdb.StateDomainChange, owner tcommon.Address, domain kvdomains.KVDomain, key []byte, prevExists bool, prev []byte, nextExists bool, next []byte) bool {
	for _, change := range changes {
		if change.Owner != owner || change.Domain != domain || !bytes.Equal(change.Key, key) {
			continue
		}
		return change.PrevExists == prevExists &&
			bytes.Equal(change.Prev, prev) &&
			change.NextExists == nextExists &&
			bytes.Equal(change.Next, next)
	}
	return false
}

func BenchmarkPrepareAccountDomainChange(b *testing.B) {
	payload := bytes.Repeat([]byte{0xab}, 16*1024)
	capture := domainChangeSetCapture{
		enabled:   true,
		blockNum:  10,
		blockHash: tcommon.Hash{0x10},
		txNum:     42,
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkPreparedDomainChange = capture.prepareDomainChange(&rawdb.StateDomainChange{
			FlatDomain: rawdb.StateFlatDomainAccountLatest,
			Owner:      tcommon.Address{0x41, 0x22},
			PrevExists: true,
			Prev:       payload,
			NextExists: true,
			Next:       payload,
		})
	}
}
