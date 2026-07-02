package state

import (
	"bytes"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// historyFixture spins up an in-memory disk store and a StateDB that persists
// through it. Each call to applyBlock mutates the state under flat temporal
// domain capture, flushes journal changes, then Commit()s.
type historyFixture struct {
	t        *testing.T
	disk     ethdb.Database
	state    *StateDB
	head     uint64
	endTxNum uint64
}

func newHistoryFixture(t *testing.T) *historyFixture {
	t.Helper()
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatalf("state.New: %v", err)
	}
	return &historyFixture{t: t, disk: disk, state: sdb}
}

// applyBlock mutates state via fn, records history, and commits. The
// next block in the chain is `head` after this call returns.
func (f *historyFixture) applyBlock(blockHash tcommon.Hash, fn func(*StateDB)) {
	f.t.Helper()
	f.head++
	begin, end, err := rawdb.NextStateTxRange(f.endTxNum, 0)
	if err != nil {
		f.t.Fatalf("NextStateTxRange block=%d: %v", f.head, err)
	}
	f.endTxNum = end
	f.state.BeginDomainChangeJournalCapture(f.disk, f.head, blockHash, begin, end)
	mark := f.state.DomainChangeJournalMark()
	fn(f.state)
	if err := f.state.FlushDomainChangesSince(mark, end); err != nil {
		f.t.Fatalf("FlushDomainChangesSince block=%d: %v", f.head, err)
	}
	if _, err := f.state.Commit(); err != nil {
		f.t.Fatalf("Commit block=%d: %v", f.head, err)
	}
}

// reader builds a fresh per-request reader pinned to the current head.
func (f *historyFixture) reader() *PersistentHistoryReader {
	return NewPersistentHistoryReader(f.disk, f.state, f.head)
}

func (f *historyFixture) pruneHotStateDomainHistory() {
	f.t.Helper()
	for blockNum := uint64(1); blockNum <= f.head; blockNum++ {
		if err := rawdb.DeleteStateDomainChanges(f.disk, blockNum); err != nil {
			f.t.Fatalf("DeleteStateDomainChanges block=%d: %v", blockNum, err)
		}
		if err := rawdb.DeleteStateTxRange(f.disk, blockNum); err != nil {
			f.t.Fatalf("DeleteStateTxRange block=%d: %v", blockNum, err)
		}
	}
}

func TestPersistentHistoryReaderLiveAccountSurfacesCorruptEnvelope(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := testAddr(0x91)
	if err := rawdb.WriteStateAccountLatest(db, addr, []byte{0x80}); err != nil {
		t.Fatalf("write corrupt account envelope: %v", err)
	}

	got, err := NewPersistentHistoryReader(db, nil, 1).AccountAt(addr, 1)
	if err == nil {
		t.Fatal("AccountAt corrupt live envelope error = nil")
	}
	if got != nil {
		t.Fatalf("AccountAt corrupt live envelope account = %+v, want nil", got)
	}
	if !strings.Contains(err.Error(), "read live account latest") {
		t.Fatalf("AccountAt corrupt live envelope error = %v, want live account latest context", err)
	}
}

func TestPersistentHistoryReaderLiveAccountSurfacesCorruptAccountProto(t *testing.T) {
	db := ethrawdb.NewMemoryDatabase()
	addr := testAddr(0x92)
	envelope := &StateAccountV2{
		Version:       StateAccountVersion,
		AccountProto:  []byte{0x80},
		AccountKVRoot: EmptyKVRoot,
	}
	encoded, err := envelope.Encode()
	if err != nil {
		t.Fatalf("encode account envelope: %v", err)
	}
	if err := rawdb.WriteStateAccountLatest(db, addr, encoded); err != nil {
		t.Fatalf("write corrupt account proto envelope: %v", err)
	}

	got, err := NewPersistentHistoryReader(db, nil, 1).AccountAt(addr, 1)
	if err == nil {
		t.Fatal("AccountAt corrupt live account proto error = nil")
	}
	if got != nil {
		t.Fatalf("AccountAt corrupt live account proto account = %+v, want nil", got)
	}
	if !strings.Contains(err.Error(), "decode live account proto") {
		t.Fatalf("AccountAt corrupt live account proto error = %v, want decode account proto context", err)
	}
}

func TestPersistentHistoryReaderUsesStateDomainAccountLatest(t *testing.T) {
	disk := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(disk)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	addr := testAddr(0x22)
	slot := tcommon.Hash{0x44}
	code1 := []byte{0x60, 0x01}
	code2 := []byte{0x60, 0x02}
	var endTxNum uint64

	applyDomainBlock := func(blockNum uint64, mutate func(*StateDB)) {
		t.Helper()
		begin, end, err := rawdb.NextStateTxRange(endTxNum, 0)
		if err != nil {
			t.Fatal(err)
		}
		endTxNum = end
		sdb.BeginDomainChangeJournalCapture(disk, blockNum, tcommon.Hash{byte(blockNum)}, begin, end)
		mark := sdb.DomainChangeJournalMark()
		mutate(sdb)
		if err := sdb.FlushDomainChangesSince(mark, end); err != nil {
			t.Fatalf("flush domain changes block %d: %v", blockNum, err)
		}
		root, err := sdb.Commit()
		if err != nil {
			t.Fatalf("commit block %d: %v", blockNum, err)
		}
		sdb, err = New(root, db)
		if err != nil {
			t.Fatalf("reopen block %d: %v", blockNum, err)
		}
	}

	applyDomainBlock(1, func(s *StateDB) {
		s.AddBalance(addr, 1_000_000)
		s.SetCode(addr, code1)
		s.SetState(addr, slot, tcommon.Hash{0x01})
	})
	applyDomainBlock(2, func(s *StateDB) {
		s.AddBalance(addr, 1_000_000)
		s.SetCode(addr, code2)
		s.SetState(addr, slot, tcommon.Hash{0x02})
	})

	r := NewPersistentHistoryReader(disk, nil, 2)
	acc, err := r.AccountAt(addr, 1)
	if err != nil {
		t.Fatalf("AccountAt block 1: %v", err)
	}
	if acc == nil || acc.Balance() != 1_000_000 {
		t.Fatalf("domain AccountAt block 1 = %+v", acc)
	}
	code, err := r.CodeAt(addr, 1)
	if err != nil {
		t.Fatalf("CodeAt block 1: %v", err)
	}
	if !bytes.Equal(code, code1) {
		t.Fatalf("domain CodeAt block 1 = %x, want %x", code, code1)
	}
	storage, err := r.StorageAt(addr, slot, 1)
	if err != nil {
		t.Fatalf("StorageAt block 1: %v", err)
	}
	if storage != (tcommon.Hash{0x01}) {
		t.Fatalf("domain StorageAt block 1 = %x, want 01", storage)
	}
	acc, err = r.AccountAt(addr, 0)
	if err != nil {
		t.Fatalf("AccountAt block 0: %v", err)
	}
	if acc != nil {
		t.Fatalf("domain AccountAt block 0 = %+v, want nil", acc)
	}
	storage, err = r.StorageAt(addr, slot, 0)
	if err != nil {
		t.Fatalf("StorageAt block 0: %v", err)
	}
	if storage != (tcommon.Hash{}) {
		t.Fatalf("domain StorageAt block 0 = %x, want zero", storage)
	}
	code, err = r.CodeAt(addr, 2)
	if err != nil {
		t.Fatalf("CodeAt head: %v", err)
	}
	if !bytes.Equal(code, code2) {
		t.Fatalf("domain CodeAt head = %x, want %x", code, code2)
	}
	storage, err = r.StorageAt(addr, slot, 2)
	if err != nil {
		t.Fatalf("StorageAt head: %v", err)
	}
	if storage != (tcommon.Hash{0x02}) {
		t.Fatalf("domain StorageAt head = %x, want 02", storage)
	}
}

func TestPersistentHistoryReaderReadsAccountStorageAndCodeFromColdStateDomainHistory(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x75)
	other := testAddr(0x76)
	var slot tcommon.Hash
	slot[31] = 0x75
	value1 := tcommon.HexToHash("01")
	value2 := tcommon.HexToHash("02")
	code1 := []byte{0x60, 0x75, 0x60, 0x01}
	code2 := []byte{0x60, 0x75, 0x60, 0x02}
	codeHash1 := tcommon.Keccak256(code1)
	codeHash2 := tcommon.Keccak256(code2)

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.CreateAccount(addr, corepb.AccountType_Contract)
		s.AddBalance(addr, 100)
		s.SetCode(addr, code1)
		s.SetState(addr, slot, value1)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.AddBalance(addr, 50)
		s.SetCode(addr, code2)
		s.SetState(addr, slot, value2)
	})
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})

	fromRange, ok, err := rawdb.ReadStateTxRange(f.disk, 1)
	if err != nil || !ok {
		t.Fatalf("read block 1 tx range: ok=%v err=%v", ok, err)
	}
	toRange, ok, err := rawdb.ReadStateTxRange(f.disk, f.head)
	if err != nil || !ok {
		t.Fatalf("read head tx range: ok=%v err=%v", ok, err)
	}
	dir := t.TempDir()
	refs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(
		f.disk,
		dir,
		fromRange.BeginTxNum,
		toRange.EndTxNum,
		"history/state-domain-change-1-3.seg",
	)
	if err != nil {
		t.Fatalf("build cold state-domain history: %v", err)
	}
	accountRef, accountAccessorRef, accountBTreeRef, err := statesnapshots.BuildAccountLatestSegmentFilesFromDB(
		f.disk,
		dir,
		fromRange.BeginTxNum,
		toRange.EndTxNum,
		"latest/account-1-3.seg",
	)
	if err != nil {
		t.Fatalf("build cold account latest: %v", err)
	}
	storageRef, storageAccessorRef, storageBTreeRef, err := statesnapshots.BuildLatestDomainSegmentFilesFromDB(
		f.disk,
		dir,
		kvdomains.ContractStorage,
		fromRange.BeginTxNum,
		toRange.EndTxNum,
		"latest/contract-storage-1-3.seg",
	)
	if err != nil {
		t.Fatalf("build cold contract storage latest: %v", err)
	}
	codeRef, codeAccessorRef, codeBTreeRef, err := statesnapshots.BuildCodeSegmentFilesFromDB(
		f.disk,
		dir,
		fromRange.BeginTxNum,
		toRange.EndTxNum,
		"latest/code-1-3.seg",
	)
	if err != nil {
		t.Fatalf("build cold code latest: %v", err)
	}
	refs = append(
		refs,
		accountRef,
		accountAccessorRef,
		accountBTreeRef,
		storageRef,
		storageAccessorRef,
		storageBTreeRef,
		codeRef,
		codeAccessorRef,
		codeBTreeRef,
	)
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(fromRange.BeginTxNum, toRange.EndTxNum, refs)); err != nil {
		t.Fatalf("publish cold state-domain history: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}

	f.pruneHotStateDomainHistory()
	if _, ok, err := rawdb.ReadStateTxRange(f.disk, 2); err != nil || ok {
		t.Fatalf("hot tx range after prune: ok=%v err=%v, want missing", ok, err)
	}
	hotChanges := 0
	if err := rawdb.IterateStateDomainChanges(f.disk, 2, func(*rawdb.StateDomainChange) (bool, error) {
		hotChanges++
		return true, nil
	}); err != nil {
		t.Fatalf("iterate hot changes after prune: %v", err)
	}
	if hotChanges != 0 {
		t.Fatalf("hot changes after prune = %d, want 0", hotChanges)
	}
	generation, _, err := rawdb.ReadStateKVGeneration(f.disk, addr)
	if err != nil {
		t.Fatalf("read hot kv generation before latest prune: %v", err)
	}
	if err := rawdb.DeleteStateAccountLatest(f.disk, addr); err != nil {
		t.Fatalf("delete hot account latest: %v", err)
	}
	if err := rawdb.DeleteStateKVGeneration(f.disk, addr); err != nil {
		t.Fatalf("delete hot kv generation: %v", err)
	}
	if err := rawdb.DeleteStateKVLatestPrefix(f.disk, addr, generation, kvdomains.ContractStorage, nil); err != nil {
		t.Fatalf("delete hot contract storage latest: %v", err)
	}
	if err := rawdb.DeleteStateCode(f.disk, codeHash1); err != nil {
		t.Fatalf("delete hot code 1: %v", err)
	}
	if err := rawdb.DeleteStateCode(f.disk, codeHash2); err != nil {
		t.Fatalf("delete hot code 2: %v", err)
	}

	hotOnly := NewPersistentHistoryReader(f.disk, nil, f.head)
	if _, err := hotOnly.AccountAt(addr, 1); !errors.Is(err, ErrStateDomainHistoryUnavailable) {
		t.Fatalf("hot-only AccountAt after prune err = %v, want ErrStateDomainHistoryUnavailable", err)
	}

	cold := NewPersistentHistoryReaderWithColdHistory(f.disk, nil, f.head, mgr)
	acc, err := cold.AccountAt(addr, 1)
	if err != nil {
		t.Fatalf("cold AccountAt block 1: %v", err)
	}
	if acc == nil || acc.Balance() != 100 {
		t.Fatalf("cold AccountAt block 1 = %+v, want balance 100", acc)
	}
	headAcc, err := cold.AccountAt(addr, f.head)
	if err != nil {
		t.Fatalf("cold AccountAt head: %v", err)
	}
	if headAcc == nil || headAcc.Balance() != 150 {
		t.Fatalf("cold AccountAt head = %+v, want balance 150", headAcc)
	}
	gotStorage, err := cold.StorageAt(addr, slot, 1)
	if err != nil {
		t.Fatalf("cold StorageAt block 1: %v", err)
	}
	if gotStorage != value1 {
		t.Fatalf("cold StorageAt block 1 = %x, want %x", gotStorage, value1)
	}
	headStorage, err := cold.StorageAt(addr, slot, f.head)
	if err != nil {
		t.Fatalf("cold StorageAt head: %v", err)
	}
	if headStorage != value2 {
		t.Fatalf("cold StorageAt head = %x, want %x", headStorage, value2)
	}
	gotCode, err := cold.CodeAt(addr, 1)
	if err != nil {
		t.Fatalf("cold CodeAt block 1: %v", err)
	}
	if !bytes.Equal(gotCode, code1) {
		t.Fatalf("cold CodeAt block 1 = %x, want %x", gotCode, code1)
	}
	headCode, err := cold.CodeAt(addr, f.head)
	if err != nil {
		t.Fatalf("cold CodeAt head: %v", err)
	}
	if !bytes.Equal(headCode, code2) {
		t.Fatalf("cold CodeAt head = %x, want %x", headCode, code2)
	}
}

func TestPersistentHistoryReaderHeadLatestUsesColdSnapshotTxNumWithoutStateRange(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x83)
	var slot tcommon.Hash
	slot[31] = 0x83
	value := tcommon.HexToHash("83")

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.CreateAccount(addr, corepb.AccountType_Contract)
		s.AddBalance(addr, 83)
		s.SetState(addr, slot, value)
	})

	dir := t.TempDir()
	accountRef, accountAccessorRef, accountBTreeRef, err := statesnapshots.BuildAccountLatestSegmentFilesFromDB(
		f.disk,
		dir,
		100,
		110,
		"latest/account-100-110.seg",
	)
	if err != nil {
		t.Fatalf("build cold account latest: %v", err)
	}
	storageRef, storageAccessorRef, storageBTreeRef, err := statesnapshots.BuildLatestDomainSegmentFilesFromDB(
		f.disk,
		dir,
		kvdomains.ContractStorage,
		100,
		110,
		"latest/contract-storage-100-110.seg",
	)
	if err != nil {
		t.Fatalf("build cold contract storage latest: %v", err)
	}
	manifest := statesnapshots.NewManifest(100, 110, []statesnapshots.SegmentRef{
		accountRef, accountAccessorRef, accountBTreeRef,
		storageRef, storageAccessorRef, storageBTreeRef,
	})
	if err := statesnapshots.PublishManifest(dir, manifest); err != nil {
		t.Fatalf("publish cold latest manifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}

	if err := rawdb.DeleteStateTxRange(f.disk, f.head); err != nil {
		t.Fatalf("delete hot head tx range: %v", err)
	}
	if err := rawdb.DeleteStateAccountLatest(f.disk, addr); err != nil {
		t.Fatalf("delete hot account latest: %v", err)
	}
	if err := rawdb.DeleteStateKVLatestPrefix(f.disk, addr, 0, kvdomains.ContractStorage, nil); err != nil {
		t.Fatalf("delete hot contract storage latest: %v", err)
	}

	cold := NewPersistentHistoryReaderWithColdHistory(f.disk, nil, f.head, mgr)
	acc, err := cold.AccountAt(addr, f.head)
	if err != nil {
		t.Fatalf("cold AccountAt head without tx range: %v", err)
	}
	if acc == nil || acc.Balance() != 83 {
		t.Fatalf("cold AccountAt head without tx range = %+v, want balance 83", acc)
	}
	gotStorage, err := cold.StorageAt(addr, slot, f.head)
	if err != nil {
		t.Fatalf("cold StorageAt head without tx range: %v", err)
	}
	if gotStorage != value {
		t.Fatalf("cold StorageAt head without tx range = %x, want %x", gotStorage, value)
	}
}

func TestPersistentHistoryReaderAccountKVPrefixAtUsesStateDomainHistory(t *testing.T) {
	f := newHistoryFixture(t)
	owner := testAddr(0x77)
	domain := kvdomains.SystemReward

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		mustSetAccountKV(t, s, owner, domain, "reward/a", "a1")
		mustSetAccountKV(t, s, owner, domain, "reward/b", "b1")
		mustSetAccountKV(t, s, owner, domain, "other/c", "c1")
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		mustSetAccountKV(t, s, owner, domain, "reward/a", "a2")
		if err := s.DeleteAccountKV(owner, domain, []byte("reward/b")); err != nil {
			t.Fatalf("DeleteAccountKV: %v", err)
		}
		mustSetAccountKV(t, s, owner, domain, "reward/c", "c2")
	})

	r := f.reader()
	at1 := collectHistoryAccountKVPrefix(t, r, owner, domain, "reward/", 1)
	if len(at1) != 2 || at1["reward/a"] != "a1" || at1["reward/b"] != "b1" {
		t.Fatalf("block 1 prefix = %v, want reward/a=a1 reward/b=b1", at1)
	}
	at2 := collectHistoryAccountKVPrefix(t, r, owner, domain, "reward/", 2)
	if len(at2) != 2 || at2["reward/a"] != "a2" || at2["reward/c"] != "c2" {
		t.Fatalf("block 2 prefix = %v, want reward/a=a2 reward/c=c2", at2)
	}
}

func TestHistoricalLatestViewIteratesAccountKVPrefixAtBound(t *testing.T) {
	f := newHistoryFixture(t)
	owner := testAddr(0x78)
	domain := kvdomains.SystemReward

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		mustSetAccountKV(t, s, owner, domain, "reward/a", "a1")
		mustSetAccountKV(t, s, owner, domain, "reward/b", "b1")
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		mustSetAccountKV(t, s, owner, domain, "reward/a", "a2")
		if err := s.DeleteAccountKV(owner, domain, []byte("reward/b")); err != nil {
			t.Fatalf("DeleteAccountKV: %v", err)
		}
		mustSetAccountKV(t, s, owner, domain, "reward/c", "c2")
	})

	f.state.SetHistoricalLatestView(f.reader(), 1)
	at1 := collectStateDBAccountKVPrefix(t, f.state, owner, domain, "reward/")
	if len(at1) != 2 || at1["reward/a"] != "a1" || at1["reward/b"] != "b1" {
		t.Fatalf("historical latest view prefix = %v, want block 1 values", at1)
	}
}

func TestPersistentHistoryReaderAccountKVPrefixAtUsesColdStateDomainHistory(t *testing.T) {
	f := newHistoryFixture(t)
	owner := testAddr(0x79)
	other := testAddr(0x7A)
	domain := kvdomains.SystemReward

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		mustSetAccountKV(t, s, owner, domain, "reward/a", "a1")
		mustSetAccountKV(t, s, owner, domain, "reward/b", "b1")
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		mustSetAccountKV(t, s, owner, domain, "reward/a", "a2")
		if err := s.DeleteAccountKV(owner, domain, []byte("reward/b")); err != nil {
			t.Fatalf("DeleteAccountKV: %v", err)
		}
		mustSetAccountKV(t, s, owner, domain, "reward/c", "c2")
	})
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})

	fromRange, ok, err := rawdb.ReadStateTxRange(f.disk, 1)
	if err != nil || !ok {
		t.Fatalf("read block 1 tx range: ok=%v err=%v", ok, err)
	}
	toRange, ok, err := rawdb.ReadStateTxRange(f.disk, f.head)
	if err != nil || !ok {
		t.Fatalf("read head tx range: ok=%v err=%v", ok, err)
	}
	dir := t.TempDir()
	refs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(
		f.disk,
		dir,
		fromRange.BeginTxNum,
		toRange.EndTxNum,
		"history/state-domain-change-prefix-1-3.seg",
	)
	if err != nil {
		t.Fatalf("build cold state-domain history: %v", err)
	}
	latestRef, latestAccessorRef, latestBTreeRef, err := statesnapshots.BuildLatestDomainSegmentFilesFromDB(
		f.disk,
		dir,
		domain,
		fromRange.BeginTxNum,
		toRange.EndTxNum,
		"latest/system-reward-prefix-1-3.seg",
	)
	if err != nil {
		t.Fatalf("build cold latest prefix domain: %v", err)
	}
	refs = append(refs, latestRef, latestAccessorRef, latestBTreeRef)
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(fromRange.BeginTxNum, toRange.EndTxNum, refs)); err != nil {
		t.Fatalf("publish cold state-domain history: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}

	f.pruneHotStateDomainHistory()
	if err := rawdb.DeleteStateKVLatestPrefix(f.disk, owner, 0, domain, []byte("reward/")); err != nil {
		t.Fatalf("delete hot reward latest prefix: %v", err)
	}
	hotOnly := NewPersistentHistoryReader(f.disk, nil, f.head)
	if err := hotOnly.AccountKVPrefixAt(owner, domain, []byte("reward/"), 1, func(key, value []byte) (bool, error) {
		return true, nil
	}); !errors.Is(err, ErrStateDomainHistoryUnavailable) {
		t.Fatalf("hot-only AccountKVPrefixAt after prune err = %v, want ErrStateDomainHistoryUnavailable", err)
	}

	cold := NewPersistentHistoryReaderWithColdHistory(f.disk, nil, f.head, mgr)
	at1 := collectHistoryAccountKVPrefix(t, cold, owner, domain, "reward/", 1)
	if len(at1) != 2 || at1["reward/a"] != "a1" || at1["reward/b"] != "b1" {
		t.Fatalf("cold block 1 prefix = %v, want reward/a=a1 reward/b=b1", at1)
	}
	at2 := collectHistoryAccountKVPrefix(t, cold, owner, domain, "reward/", 2)
	if len(at2) != 2 || at2["reward/a"] != "a2" || at2["reward/c"] != "c2" {
		t.Fatalf("cold block 2 prefix = %v, want reward/a=a2 reward/c=c2", at2)
	}
	headValue, ok, err := cold.AccountKVAt(owner, domain, []byte("reward/a"), f.head)
	if err != nil {
		t.Fatalf("cold AccountKVAt head: %v", err)
	}
	if !ok || string(headValue) != "a2" {
		t.Fatalf("cold AccountKVAt head reward/a = %q ok=%v, want a2", headValue, ok)
	}
	headAccountPrefix := collectHistoryAccountKVPrefix(t, cold, owner, domain, "reward/", f.head)
	if len(headAccountPrefix) != 2 || headAccountPrefix["reward/a"] != "a2" || headAccountPrefix["reward/c"] != "c2" {
		t.Fatalf("cold head account prefix = %v, want reward/a=a2 reward/c=c2", headAccountPrefix)
	}
	headLatestPrefix := collectHistoryKVLatestPrefix(t, cold, owner, 0, domain, "reward/", f.head)
	if len(headLatestPrefix) != 2 || headLatestPrefix["reward/a"] != "a2" || headLatestPrefix["reward/c"] != "c2" {
		t.Fatalf("cold head latest prefix = %v, want reward/a=a2 reward/c=c2", headLatestPrefix)
	}
}

func TestPersistentHistoryReaderReadsCodeFromColdCodeDomain(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x73)
	code1 := []byte{0x60, 0x01, 0x60, 0x02}
	code2 := []byte{0x60, 0x03, 0x60, 0x04}
	codeHash1 := tcommon.Keccak256(code1)
	codeHash2 := tcommon.Keccak256(code2)

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.SetCode(addr, code1)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.SetCode(addr, code2)
	})
	// Block 3: unrelated mutation so blocks 1 and 2 sit below head and resolve
	// through historical reconstruction (and thus the cold CodeDomain) rather
	// than the live read at head.
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.AddBalance(testAddr(0x74), 1)
	})

	range1, ok, err := rawdb.ReadStateTxRange(f.disk, 1)
	if err != nil || !ok {
		t.Fatalf("read block 1 tx range: ok=%v err=%v", ok, err)
	}
	range2, ok, err := rawdb.ReadStateTxRange(f.disk, 2)
	if err != nil || !ok {
		t.Fatalf("read block 2 tx range: ok=%v err=%v", ok, err)
	}
	dir := t.TempDir()
	codeRef, codeAccessorRef, codeBTreeRef, err := statesnapshots.BuildCodeSegmentFilesFromDB(f.disk, dir, range1.BeginTxNum, range2.EndTxNum, "latest/code-1-2.seg")
	if err != nil {
		t.Fatalf("build code latest snapshot: %v", err)
	}
	refs := []statesnapshots.SegmentRef{codeRef, codeAccessorRef, codeBTreeRef}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(range1.BeginTxNum, range2.EndTxNum, refs)); err != nil {
		t.Fatalf("publish code manifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}

	if err := rawdb.DeleteStateCode(f.disk, codeHash1); err != nil {
		t.Fatalf("delete hot code 1: %v", err)
	}
	if err := rawdb.DeleteStateCode(f.disk, codeHash2); err != nil {
		t.Fatalf("delete hot code 2: %v", err)
	}
	r := NewPersistentHistoryReaderWithColdHistory(f.disk, nil, f.head, mgr)
	code, err := r.CodeAt(addr, 1)
	if err != nil {
		t.Fatalf("CodeAt block 1: %v", err)
	}
	if !bytes.Equal(code, code1) {
		t.Fatalf("CodeAt block 1 = %x, want %x", code, code1)
	}
	// The updated bytecode must also reconstruct from the cold CodeDomain: the
	// account envelope as-of block 2 references codeHash2, and the cold snapshot
	// (built before hot deletion) retains both content-addressed versions.
	code2Got, err := r.CodeAt(addr, 2)
	if err != nil {
		t.Fatalf("CodeAt block 2: %v", err)
	}
	if !bytes.Equal(code2Got, code2) {
		t.Fatalf("CodeAt block 2 = %x, want %x", code2Got, code2)
	}
}

func TestPersistentHistoryReaderCodeAtSurfacesMissingHistoricalCodeRow(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0x83)
	other := testAddr(0x84)
	code := []byte{0x60, 0x0c, 0x60, 0x00, 0xf3}
	codeHash := tcommon.Keccak256(code)

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.CreateAccount(contract, corepb.AccountType_Contract)
		s.SetCode(contract, code)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})
	if err := rawdb.DeleteStateCode(f.disk, codeHash); err != nil {
		t.Fatalf("delete state code: %v", err)
	}

	got, err := f.reader().CodeAt(contract, 1)
	if err == nil {
		t.Fatal("CodeAt missing historical code row error = nil")
	}
	if got != nil {
		t.Fatalf("CodeAt missing historical code row = %x, want nil", got)
	}
	if !strings.Contains(err.Error(), "state code") ||
		!strings.Contains(err.Error(), "block 1") ||
		!strings.Contains(err.Error(), "is missing") {
		t.Fatalf("CodeAt missing historical code row error = %v, want block-scoped missing state code context", err)
	}
}

func TestPersistentHistoryReaderHeadCodeSurfacesMissingCodeRowWithoutLiveState(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0x85)
	code := []byte{0x60, 0x0d, 0x60, 0x00, 0xf3}
	codeHash := tcommon.Keccak256(code)

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.CreateAccount(contract, corepb.AccountType_Contract)
		s.SetCode(contract, code)
	})
	if err := rawdb.DeleteStateCode(f.disk, codeHash); err != nil {
		t.Fatalf("delete state code: %v", err)
	}

	r := NewPersistentHistoryReader(f.disk, nil, f.head)
	got, err := r.CodeAt(contract, f.head)
	if err == nil {
		t.Fatal("CodeAt missing head code row error = nil")
	}
	if got != nil {
		t.Fatalf("CodeAt missing head code row = %x, want nil", got)
	}
	if !strings.Contains(err.Error(), "read live code") ||
		!strings.Contains(err.Error(), "state code") ||
		!strings.Contains(err.Error(), "is missing") {
		t.Fatalf("CodeAt missing head code row error = %v, want live missing state code context", err)
	}
}

// TestPersistentHistoryReader_TenBlockSweep is the spec's headline test:
// drive a known account's balance and a contract's slot through ten
// blocks of mutations, plus a code change at block 5, and assert
// byte-exact reconstruction at every blockNum 1..10.
//
// Coverage:
//
//   - balance changes at every block 1..10 — exercises the dense
//     inverse-index walk
//   - slot K modified only at blocks {3, 7} — exercises the SPARSE
//     inverse-index seek (between 7 and 10 we walk past nothing; from 1
//     to 6 we hit only block 7's entry)
//   - code unchanged at blocks 1..4, set at block 5, unchanged 6..10 —
//     exercises the CodeAt path's "share work with AccountAt" walk plus
//     the "CodePre nil means no codeChange" handling
//
// Each assertion is byte-exact; any deviation indicates either a slice-2
// capture bug or a slice-3 reconstruction bug.
func TestPersistentHistoryReader_TenBlockSweep(t *testing.T) {
	f := newHistoryFixture(t)
	acct := testAddr(0x10)
	contract := testAddr(0x20)
	slotK := tcommon.Hash{0xAA, 0xBB, 0xCC}

	// Block 1: create acct, create contract, set initial state.
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(acct, 1_000_000)
		s.GetOrCreateAccount(contract)
		// No SetState here — sparse slot starts post-block-1.
	})

	// Blocks 2..10: balance every block, slot at 3 and 7 only,
	// code only at block 5.
	for n := uint64(2); n <= 10; n++ {
		blockHash := tcommon.Hash{byte(n)}
		bn := n
		f.applyBlock(blockHash, func(s *StateDB) {
			// Drive balance from N*1M to (N+1)*1M to make balance==N*1M
			// at end-of-block-N: start with bal=1M from block 1; block 2
			// adds 1M → bal=2M=2*1M. block N adds 1M → bal=N*1M.
			s.AddBalance(acct, 1_000_000)

			if bn == 3 {
				s.SetState(contract, slotK, tcommon.Hash{0x03})
			}
			if bn == 7 {
				s.SetState(contract, slotK, tcommon.Hash{0x07})
			}
			if bn == 5 {
				s.SetCode(contract, []byte{0xDE, 0xAD, 0xBE, 0xEF})
			}
		})
	}

	// Now query at every block 1..10.
	r := f.reader()

	// Balance assertions: at end-of-N, balance = N * 1_000_000.
	for n := uint64(1); n <= 10; n++ {
		acc, err := r.AccountAt(acct, n)
		if err != nil {
			t.Fatalf("AccountAt(acct, %d): %v", n, err)
		}
		if acc == nil {
			t.Fatalf("AccountAt(acct, %d) = nil; want non-nil", n)
		}
		want := int64(n) * 1_000_000
		if got := acc.Balance(); got != want {
			t.Errorf("AccountAt(acct, %d).Balance() = %d, want %d", n, got, want)
		}
	}

	// Slot assertions: slot was set to 0x03 at block 3, 0x07 at block 7.
	//
	//   block 1, 2:  slot empty (zero hash)
	//   block 3, 4, 5, 6: slot = 0x03
	//   block 7, 8, 9, 10: slot = 0x07
	slotCases := []struct {
		n    uint64
		want tcommon.Hash
	}{
		{1, tcommon.Hash{}},
		{2, tcommon.Hash{}},
		{3, tcommon.Hash{0x03}},
		{4, tcommon.Hash{0x03}},
		{5, tcommon.Hash{0x03}},
		{6, tcommon.Hash{0x03}},
		{7, tcommon.Hash{0x07}},
		{8, tcommon.Hash{0x07}},
		{9, tcommon.Hash{0x07}},
		{10, tcommon.Hash{0x07}},
	}
	for _, tc := range slotCases {
		got, err := r.StorageAt(contract, slotK, tc.n)
		if err != nil {
			t.Fatalf("StorageAt(contract, slotK, %d): %v", tc.n, err)
		}
		if got != tc.want {
			t.Errorf("StorageAt(contract, slotK, %d) = %x, want %x", tc.n, got, tc.want)
		}
	}

	// Code assertions: contract was code-less until block 5, then has
	// {0xDE,0xAD,0xBE,0xEF} from block 5 onward.
	wantPostCode := []byte{0xDE, 0xAD, 0xBE, 0xEF}
	codeCases := []struct {
		n    uint64
		want []byte
	}{
		{1, nil},
		{2, nil},
		{3, nil},
		{4, nil},
		{5, wantPostCode},
		{6, wantPostCode},
		{10, wantPostCode},
	}
	for _, tc := range codeCases {
		got, err := r.CodeAt(contract, tc.n)
		if err != nil {
			t.Fatalf("CodeAt(contract, %d): %v", tc.n, err)
		}
		if !bytes.Equal(got, tc.want) {
			t.Errorf("CodeAt(contract, %d) = %x, want %x", tc.n, got, tc.want)
		}
	}
}

func TestPersistentHistoryReaderUsesColdStateDomainChangeSnapshot(t *testing.T) {
	f := newHistoryFixture(t)
	acct := testAddr(0x61)
	contract := testAddr(0x62)
	slot := tcommon.Hash{0x33}

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(acct, 1_000_000)
		s.GetOrCreateAccount(contract)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.AddBalance(acct, 1_000_000)
		s.SetState(contract, slot, tcommon.Hash{0x02})
	})
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.AddBalance(acct, 1_000_000)
		s.SetState(contract, slot, tcommon.Hash{0x03})
	})
	f.applyBlock(tcommon.Hash{0x04}, func(s *StateDB) {
		s.AddBalance(acct, 1_000_000)
		s.SetState(contract, slot, tcommon.Hash{0x04})
	})

	range2, ok, err := rawdb.ReadStateTxRange(f.disk, 2)
	if err != nil || !ok {
		t.Fatalf("read block 2 tx range: ok=%v err=%v", ok, err)
	}
	range3, ok, err := rawdb.ReadStateTxRange(f.disk, 3)
	if err != nil || !ok {
		t.Fatalf("read block 3 tx range: ok=%v err=%v", ok, err)
	}
	dir := t.TempDir()
	refs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(f.disk, dir, range2.BeginTxNum, range3.EndTxNum, "history/state-domain-change-2-3.seg")
	if err != nil {
		t.Fatalf("build cold state-domain-change segment: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(range2.BeginTxNum, range3.EndTxNum, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	if err := rawdb.DeleteStateDomainChanges(f.disk, 2); err != nil {
		t.Fatalf("delete hot block 2 changes: %v", err)
	}
	if err := rawdb.DeleteStateDomainChanges(f.disk, 3); err != nil {
		t.Fatalf("delete hot block 3 changes: %v", err)
	}
	if err := rawdb.DeleteStateTxRange(f.disk, 2); err != nil {
		t.Fatalf("delete hot block 2 tx range: %v", err)
	}
	if err := rawdb.DeleteStateTxRange(f.disk, 3); err != nil {
		t.Fatalf("delete hot block 3 tx range: %v", err)
	}

	r := NewPersistentHistoryReaderWithColdHistory(f.disk, f.state, f.head, mgr)
	acc, err := r.AccountAt(acct, 1)
	if err != nil {
		t.Fatalf("cold AccountAt block 1: %v", err)
	}
	if acc == nil || acc.Balance() != 1_000_000 {
		t.Fatalf("cold AccountAt block 1 = %+v, want balance 1000000", acc)
	}
	acc, err = r.AccountAt(acct, 2)
	if err != nil {
		t.Fatalf("cold AccountAt block 2: %v", err)
	}
	if acc == nil || acc.Balance() != 2_000_000 {
		t.Fatalf("cold AccountAt block 2 = %+v, want balance 2000000", acc)
	}
	acc, err = r.AccountAt(acct, 3)
	if err != nil {
		t.Fatalf("cold AccountAt block 3: %v", err)
	}
	if acc == nil || acc.Balance() != 3_000_000 {
		t.Fatalf("cold AccountAt block 3 = %+v, want balance 3000000", acc)
	}
	got, err := r.StorageAt(contract, slot, 1)
	if err != nil {
		t.Fatalf("cold StorageAt block 1: %v", err)
	}
	if got != (tcommon.Hash{}) {
		t.Fatalf("cold StorageAt block 1 = %x, want zero", got)
	}
	got, err = r.StorageAt(contract, slot, 2)
	if err != nil {
		t.Fatalf("cold StorageAt block 2: %v", err)
	}
	if got != (tcommon.Hash{0x02}) {
		t.Fatalf("cold StorageAt block 2 = %x, want 0x02", got)
	}
	got, err = r.StorageAt(contract, slot, 3)
	if err != nil {
		t.Fatalf("cold StorageAt block 3: %v", err)
	}
	if got != (tcommon.Hash{0x03}) {
		t.Fatalf("cold StorageAt block 3 = %x, want 0x03", got)
	}
}

func TestPersistentHistoryReaderUsesColdStateDomainChangeSnapshotAcrossRecreate(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0x63)
	other := testAddr(0x64)
	slotA := tcommon.Hash{31: 0xA0}
	slotB := tcommon.Hash{31: 0xB0}
	oldA := tcommon.Hash{31: 0x0A}
	oldB := tcommon.Hash{31: 0x0B}
	newA := tcommon.Hash{31: 0x1A}

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.CreateAccount(contract, corepb.AccountType_Contract)
		s.SetState(contract, slotA, oldA)
		s.SetState(contract, slotB, oldB)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.SelfDestruct(contract)
		s.FinalizeTransaction()
	})
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.CreateAccount(contract, corepb.AccountType_Contract)
		s.SetState(contract, slotA, newA)
	})
	f.applyBlock(tcommon.Hash{0x04}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})

	range2, ok, err := rawdb.ReadStateTxRange(f.disk, 2)
	if err != nil || !ok {
		t.Fatalf("read block 2 tx range: ok=%v err=%v", ok, err)
	}
	range3, ok, err := rawdb.ReadStateTxRange(f.disk, 3)
	if err != nil || !ok {
		t.Fatalf("read block 3 tx range: ok=%v err=%v", ok, err)
	}
	dir := t.TempDir()
	refs, err := statesnapshots.BuildStateDomainChangeHistorySegmentsFromDB(f.disk, dir, range2.BeginTxNum, range3.EndTxNum, "history/state-domain-change-2-3.seg")
	if err != nil {
		t.Fatalf("build cold state-domain-change segment: %v", err)
	}
	if err := statesnapshots.PublishManifest(dir, statesnapshots.NewManifest(range2.BeginTxNum, range3.EndTxNum, refs)); err != nil {
		t.Fatalf("publish manifest: %v", err)
	}
	mgr, err := statesnapshots.OpenManager(dir)
	if err != nil {
		t.Fatalf("open snapshot manager: %v", err)
	}
	for _, blockNum := range []uint64{2, 3} {
		if err := rawdb.DeleteStateDomainChanges(f.disk, blockNum); err != nil {
			t.Fatalf("delete hot block %d changes: %v", blockNum, err)
		}
		if err := rawdb.DeleteStateTxRange(f.disk, blockNum); err != nil {
			t.Fatalf("delete hot block %d tx range: %v", blockNum, err)
		}
	}

	r := NewPersistentHistoryReaderWithColdHistory(f.disk, f.state, f.head, mgr)
	for _, tc := range []struct {
		name  string
		block uint64
		slot  tcommon.Hash
		want  tcommon.Hash
	}{
		{"old slot A before delete", 1, slotA, oldA},
		{"old slot B before delete", 1, slotB, oldB},
		{"slot A while deleted", 2, slotA, tcommon.Hash{}},
		{"slot B while deleted", 2, slotB, tcommon.Hash{}},
		{"new slot A after recreate", 3, slotA, newA},
		{"old slot B after recreate", 3, slotB, tcommon.Hash{}},
		{"new slot A at head", 4, slotA, newA},
		{"old slot B at head", 4, slotB, tcommon.Hash{}},
	} {
		got, err := r.StorageAt(contract, tc.slot, tc.block)
		if err != nil {
			t.Fatalf("%s: StorageAt block %d: %v", tc.name, tc.block, err)
		}
		if got != tc.want {
			t.Fatalf("%s: StorageAt block %d = %x, want %x", tc.name, tc.block, got, tc.want)
		}
	}
}

// TestPersistentHistoryReader_NeverModified covers the inverse-index
// empty-scan short-circuit. An addr that was set at genesis (here block
// 1 in our fixture's terms) but never modified afterwards must read live
// for any blockNum >= the genesis block, regardless of headNum.
func TestPersistentHistoryReader_NeverModified(t *testing.T) {
	f := newHistoryFixture(t)
	never := testAddr(0x30)
	// Touch a different account at every block so there's chain
	// history, but never touch `never`.
	driver := testAddr(0x31)
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		// Seed `never` BEFORE history-capture flips on: write to disk
		// via Commit, with no history rows produced. Then never touch.
		s.GetOrCreateAccount(never)
		s.AddBalance(never, 99)
		s.AddBalance(driver, 1)
	})
	for n := uint64(2); n <= 5; n++ {
		f.applyBlock(tcommon.Hash{byte(n)}, func(s *StateDB) {
			s.AddBalance(driver, 1)
		})
	}

	r := f.reader()
	for n := uint64(1); n <= 5; n++ {
		acc, err := r.AccountAt(never, n)
		if err != nil {
			t.Fatalf("AccountAt(never, %d): %v", n, err)
		}
		if acc == nil {
			t.Fatalf("AccountAt(never, %d) = nil; want non-nil (account exists from block 1 onward)", n)
		}
		// `never` was credited 99 in block 1 and untouched thereafter.
		if got := acc.Balance(); got != 99 {
			t.Errorf("AccountAt(never, %d).Balance() = %d, want 99", n, got)
		}
	}
}

// TestPersistentHistoryReader_PastHead asserts the at-or-past-head
// short-circuit. blockNum >= headNum returns live (no inverse-index
// walk) and never errors. blockNum > headNum is clamped to live; the
// JSON-RPC layer is responsible for rejecting future blocks before they
// reach the reader.
func TestPersistentHistoryReader_PastHead(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x40)
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(addr, 1_000)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.AddBalance(addr, 2_000)
	})
	// head = 2; live balance = 3_000.

	r := f.reader()
	for _, n := range []uint64{2, 3, 99, 1 << 50} {
		acc, err := r.AccountAt(addr, n)
		if err != nil {
			t.Fatalf("AccountAt(addr, %d): %v", n, err)
		}
		if acc == nil {
			t.Fatalf("AccountAt(addr, %d) = nil", n)
		}
		if got := acc.Balance(); got != 3_000 {
			t.Errorf("AccountAt(addr, %d).Balance() = %d, want 3000 (live)", n, got)
		}
	}
}

// TestPersistentHistoryReader_BlockNumZero asserts a query at blockNum=0
// returns genesis state. In our fixture genesis is "before block 1"; we
// seed nothing pre-block-1, so blockNum=0 must report no account.
func TestPersistentHistoryReader_BlockNumZero(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x50)
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(addr, 12345)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.AddBalance(addr, 678)
	})

	r := f.reader()
	acc, err := r.AccountAt(addr, 0)
	if err != nil {
		t.Fatalf("AccountAt(addr, 0): %v", err)
	}
	if acc != nil {
		t.Fatalf("AccountAt(addr, 0) = %v, want nil (address didn't exist pre-block-1)", acc)
	}
}

// TestPersistentHistoryReader_CacheHit wraps the disk store in a
// counting adapter and asserts a second AccountAt at the same (addr,
// blockNum) issues NO additional iterator calls.
func TestPersistentHistoryReader_CacheHit(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x60)
	for n := uint64(1); n <= 5; n++ {
		f.applyBlock(tcommon.Hash{byte(n)}, func(s *StateDB) {
			s.AddBalance(addr, int64(n)*100)
		})
	}

	counting := &countingDB{readerDB: f.disk}
	r := NewPersistentHistoryReader(counting, f.state, f.head)

	if _, err := r.AccountAt(addr, 3); err != nil {
		t.Fatalf("AccountAt(addr, 3) #1: %v", err)
	}
	firstIters := atomic.LoadInt64(&counting.iterCalls)
	if firstIters == 0 {
		t.Fatal("expected at least one iterator call on first read")
	}

	if _, err := r.AccountAt(addr, 3); err != nil {
		t.Fatalf("AccountAt(addr, 3) #2: %v", err)
	}
	secondIters := atomic.LoadInt64(&counting.iterCalls)
	if secondIters != firstIters {
		t.Errorf("second AccountAt issued %d new iterator calls; cache should have absorbed it", secondIters-firstIters)
	}

	// CodeAt at the same (addr, blockNum) shares the AccountAt walk —
	// expect zero new iterator scans.
	if _, err := r.CodeAt(addr, 3); err != nil {
		t.Fatalf("CodeAt(addr, 3): %v", err)
	}
	thirdIters := atomic.LoadInt64(&counting.iterCalls)
	if thirdIters != firstIters {
		t.Errorf("CodeAt(addr, 3) issued %d new iterator calls; should reuse AccountAt cache", thirdIters-firstIters)
	}

	// Storage cache is per-(addr, slot, blockNum). Two reads of the
	// same triple are one iterator scan total.
	slotK := tcommon.Hash{0xDE}
	if _, err := r.StorageAt(addr, slotK, 3); err != nil {
		t.Fatalf("StorageAt #1: %v", err)
	}
	afterStorage1 := atomic.LoadInt64(&counting.iterCalls)
	if _, err := r.StorageAt(addr, slotK, 3); err != nil {
		t.Fatalf("StorageAt #2: %v", err)
	}
	afterStorage2 := atomic.LoadInt64(&counting.iterCalls)
	if afterStorage2 != afterStorage1 {
		t.Errorf("second StorageAt issued %d new iterator calls; cache should have absorbed it", afterStorage2-afterStorage1)
	}
}

// TestPersistentHistoryReader_AccountDeletedThenRecreated drives a
// SELFDESTRUCT-then-CREATE2 shape across blocks: account exists from
// block 3, is destroyed at block 7, recreated at block 9. AccountAt
// must correctly report each lifecycle phase.
//
// gtron's stateObject API doesn't expose a raw SELFDESTRUCT hook here;
// we simulate the same journal shape (accountChange with prev=<orig>,
// then prev=nil for the recreation) by emptying the account at block
// 7 and adding it back at block 9. The captured slice-2 deltas have
// the same ExistedPre flag transitions.
func TestPersistentHistoryReader_AccountDeletedThenRecreated(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x70)
	slot := tcommon.Hash{0x99}
	oldSlotValue := tcommon.Hash{0x01}
	newSlotValue := tcommon.Hash{0x09}

	// Blocks 1, 2: nothing relevant.
	other := testAddr(0x71)
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})

	// Block 3: create addr with balance 100.
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.AddBalance(addr, 100)
		s.SetState(addr, slot, oldSlotValue)
	})

	// Blocks 4-6: addr is untouched.
	for n := uint64(4); n <= 6; n++ {
		f.applyBlock(tcommon.Hash{byte(n)}, func(s *StateDB) {
			s.AddBalance(other, 1)
		})
	}

	// Block 7: destroy addr. Mirror the gtron VM flow (opSelfDestruct in
	// vm/instructions.go), which marks the account self-destructed and
	// defers the real account/code deletion until Commit.
	f.applyBlock(tcommon.Hash{0x07}, func(s *StateDB) {
		s.SelfDestruct(addr)
	})

	// Block 8: addr is untouched.
	f.applyBlock(tcommon.Hash{0x08}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})

	// Block 9: recreate addr.
	f.applyBlock(tcommon.Hash{0x09}, func(s *StateDB) {
		s.AddBalance(addr, 999)
		s.SetState(addr, slot, newSlotValue)
	})

	// Block 10: addr untouched.
	f.applyBlock(tcommon.Hash{0x0A}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})

	r := f.reader()

	// At block 5 (created at 3, alive), balance == 100.
	if acc, _ := r.AccountAt(addr, 5); acc == nil {
		t.Error("AccountAt(addr, 5) = nil; want non-nil (alive)")
	} else if acc.Balance() != 100 {
		t.Errorf("AccountAt(addr, 5).Balance() = %d, want 100", acc.Balance())
	}

	// At block 6, alive: balance == 100.
	if acc, _ := r.AccountAt(addr, 6); acc == nil {
		t.Error("AccountAt(addr, 6) = nil; want non-nil (alive)")
	} else if acc.Balance() != 100 {
		t.Errorf("AccountAt(addr, 6).Balance() = %d, want 100", acc.Balance())
	}

	// At block 7 (destroyed end-of-7), account is nil.
	if acc, _ := r.AccountAt(addr, 7); acc != nil {
		t.Errorf("AccountAt(addr, 7) = %v; want nil (destroyed)", acc)
	}
	// At block 8 (still destroyed), nil.
	if acc, _ := r.AccountAt(addr, 8); acc != nil {
		t.Errorf("AccountAt(addr, 8) = %v; want nil", acc)
	}

	// At block 9 (recreated end-of-9), balance == 999.
	if acc, _ := r.AccountAt(addr, 9); acc == nil {
		t.Error("AccountAt(addr, 9) = nil; want non-nil (recreated)")
	} else if acc.Balance() != 999 {
		t.Errorf("AccountAt(addr, 9).Balance() = %d, want 999", acc.Balance())
	}

	// At block 10 (untouched after recreation), balance == 999.
	if acc, _ := r.AccountAt(addr, 10); acc == nil {
		t.Error("AccountAt(addr, 10) = nil; want non-nil")
	} else if acc.Balance() != 999 {
		t.Errorf("AccountAt(addr, 10).Balance() = %d, want 999", acc.Balance())
	}

	if got, err := r.StorageAt(addr, slot, 5); err != nil {
		t.Fatalf("StorageAt(addr, slot, 5): %v", err)
	} else if got != oldSlotValue {
		t.Errorf("StorageAt(addr, slot, 5) = %x, want old generation value %x", got, oldSlotValue)
	}
	if got, err := r.StorageAt(addr, slot, 7); err != nil {
		t.Fatalf("StorageAt(addr, slot, 7): %v", err)
	} else if got != (tcommon.Hash{}) {
		t.Errorf("StorageAt(addr, slot, 7) = %x, want zero after delete", got)
	}
	if got, err := r.StorageAt(addr, slot, 9); err != nil {
		t.Fatalf("StorageAt(addr, slot, 9): %v", err)
	} else if got != newSlotValue {
		t.Errorf("StorageAt(addr, slot, 9) = %x, want new generation value %x", got, newSlotValue)
	}
}

// TestPersistentHistoryReader_CodeUpdateHistory pins historical code
// reconstruction across an in-place bytecode overwrite (gap doc item #10):
// a contract whose code is replaced (codeA -> codeB, both non-empty) must
// reconstruct the correct bytes at each historical block. TenBlockSweep only
// covers empty->code creation; this covers a true update where both the
// before- and after-bytes are non-empty and must be told apart.
//
// All queried blocks are strictly below head so they exercise the historical
// reconstruction path (accountAndCodeFromStateDomain), not the live read.
func TestPersistentHistoryReader_CodeUpdateHistory(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0xC1)
	other := testAddr(0xC2)
	codeA := []byte{0x60, 0x01}
	codeB := []byte{0x60, 0x02, 0x60, 0x03}

	// Block 1: deploy with codeA. Block 2: overwrite in place with codeB.
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.SetCode(contract, codeA)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.SetCode(contract, codeB)
	})
	// Block 3: unrelated mutation so blocks 1 and 2 are below head and resolve
	// through the historical reconstruction path rather than the live read.
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})

	r := f.reader()
	if got, err := r.CodeAt(contract, 1); err != nil {
		t.Fatalf("CodeAt(contract, 1): %v", err)
	} else if !bytes.Equal(got, codeA) {
		t.Errorf("CodeAt(contract, 1) = %x, want codeA %x", got, codeA)
	}
	if got, err := r.CodeAt(contract, 2); err != nil {
		t.Fatalf("CodeAt(contract, 2): %v", err)
	} else if !bytes.Equal(got, codeB) {
		t.Errorf("CodeAt(contract, 2) = %x, want codeB %x", got, codeB)
	}
}

// TestPersistentHistoryReader_StorageSlotZeroPreValue exercises a flat-domain
// storage rollback where the later write deletes a previously non-zero slot.
//
// Setup:
//  1. Block 1: write slot = 0xDEAD on contract.
//  2. Block 2: write slot = 0x0000 (zero-out — pre-block was 0xDEAD).
//  3. Query StorageAt(slot, 1) → must return 0xDEAD (pre-block-2 value).
//
// The capture path at block 2 stores 0xDEAD as the StateDomainChange previous
// value. Because we also test the dense case (write zero to a then-non-zero
// slot), this confirms the flat rollback walk handles deletion pre-images.
func TestPersistentHistoryReader_StorageSlotZeroPreValue(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0x80)
	slot := tcommon.Hash{0xCD}

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.GetOrCreateAccount(contract)
		s.SetState(contract, slot, tcommon.HexToHash("dead"))
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		// Zero-out the slot; pre-block value was 0xDEAD.
		s.SetState(contract, slot, tcommon.Hash{})
	})

	r := f.reader()
	got, err := r.StorageAt(contract, slot, 1)
	if err != nil {
		t.Fatalf("StorageAt(contract, slot, 1): %v", err)
	}
	if want := tcommon.HexToHash("dead"); got != want {
		t.Errorf("StorageAt(contract, slot, 1) = %x, want %x (pre-block-2 value)", got, want)
	}
	// And at block 2 (end-of-2) the slot is zero.
	got2, err := r.StorageAt(contract, slot, 2)
	if err != nil {
		t.Fatalf("StorageAt(contract, slot, 2): %v", err)
	}
	if got2 != (tcommon.Hash{}) {
		t.Errorf("StorageAt(contract, slot, 2) = %x, want zero", got2)
	}
}

func TestPersistentHistoryReaderStorageAtSurfacesOversizedValue(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0x81)
	slot := tcommon.Hash{0xCE}
	rowKey := javaStorageRowKey(contract, slot, nil)
	oversized := bytes.Repeat([]byte{0xaa}, tcommon.HashLength+1)

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.GetOrCreateAccount(contract)
		if err := s.SetAccountKV(contract, kvdomains.ContractStorage, rowKey.Bytes(), oversized); err != nil {
			t.Fatalf("write oversized storage value: %v", err)
		}
	})
	f.applyBlock(tcommon.Hash{0x02}, func(*StateDB) {})

	got, err := f.reader().StorageAt(contract, slot, 1)
	if err == nil {
		t.Fatal("StorageAt oversized value error = nil")
	}
	if got != (tcommon.Hash{}) {
		t.Fatalf("StorageAt oversized value = %x, want zero", got)
	}
	if !strings.Contains(err.Error(), "storage value at block 1") || !strings.Contains(err.Error(), "length 33, want <= 32") {
		t.Fatalf("StorageAt oversized value error = %v, want storage length context", err)
	}
}

func TestPersistentHistoryReaderLiveStorageSurfacesCorruptMetadata(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0x82)
	slot := tcommon.Hash{0xCF}

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(contract, 1)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		if err := s.SetAccountKV(contract, kvdomains.ContractMetadata, contractMetaKVKey, []byte{0x80}); err != nil {
			t.Fatalf("write corrupt contract metadata: %v", err)
		}
	})

	got, err := f.reader().StorageAt(contract, slot, f.head)
	if err == nil {
		t.Fatal("live StorageAt corrupt metadata error = nil")
	}
	if got != (tcommon.Hash{}) {
		t.Fatalf("live StorageAt corrupt metadata = %x, want zero", got)
	}
	if !strings.Contains(err.Error(), "decode contract metadata for storage key") {
		t.Fatalf("live StorageAt corrupt metadata error = %v, want metadata decode context", err)
	}
}

func TestPersistentHistoryReaderHistoricalStorageSurfacesCorruptMetadataContext(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0x83)
	slot := tcommon.Hash{0xD0}

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(contract, 1)
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		if err := s.SetAccountKV(contract, kvdomains.ContractMetadata, contractMetaKVKey, []byte{0x80}); err != nil {
			t.Fatalf("write corrupt contract metadata: %v", err)
		}
	})
	f.applyBlock(tcommon.Hash{0x03}, func(*StateDB) {})

	got, err := f.reader().StorageAt(contract, slot, 2)
	if err == nil {
		t.Fatal("historical StorageAt corrupt metadata error = nil")
	}
	if got != (tcommon.Hash{}) {
		t.Fatalf("historical StorageAt corrupt metadata = %x, want zero", got)
	}
	if !strings.Contains(err.Error(), "decode contract metadata for storage key") ||
		!strings.Contains(err.Error(), contract.Hex()) ||
		!strings.Contains(err.Error(), "block 2") {
		t.Fatalf("historical StorageAt corrupt metadata error = %v, want metadata decode context", err)
	}
}

// TestPersistentHistoryReader_SparseInverseIndexSeek pins down the
// advisor's concern: if every block touches every slot, the inverse
// index has dense entries and the reader's walk is trivial. The
// non-trivial case is a slot that's touched at only a few sparse
// blocks; the reader must seek correctly through the gaps.
func TestPersistentHistoryReader_SparseInverseIndexSeek(t *testing.T) {
	f := newHistoryFixture(t)
	contract := testAddr(0x90)
	slot := tcommon.Hash{31: 0x42}
	other := tcommon.Hash{31: 0x43}

	// Block 1: write `other` slot (so contract exists post-block-1).
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.SetState(contract, other, tcommon.Hash{0x01})
	})

	// Block 3: write `slot` = 0x33.
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) {
		s.SetState(contract, other, tcommon.Hash{0x02})
	})
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) {
		s.SetState(contract, slot, tcommon.Hash{0x33})
	})

	// Blocks 4..6: only `other` touched.
	for n := uint64(4); n <= 6; n++ {
		f.applyBlock(tcommon.Hash{byte(n)}, func(s *StateDB) {
			s.SetState(contract, other, tcommon.Hash{byte(n)})
		})
	}

	// Block 7: write `slot` = 0x77.
	f.applyBlock(tcommon.Hash{0x07}, func(s *StateDB) {
		s.SetState(contract, slot, tcommon.Hash{0x77})
	})

	// Blocks 8..10: only `other` touched.
	for n := uint64(8); n <= 10; n++ {
		f.applyBlock(tcommon.Hash{byte(n)}, func(s *StateDB) {
			s.SetState(contract, other, tcommon.Hash{byte(n)})
		})
	}

	r := f.reader()

	// `slot` history: nothing pre-block-3, 0x33 from end-of-3 to
	// end-of-6, 0x77 from end-of-7 to end-of-10.
	cases := []struct {
		n    uint64
		want tcommon.Hash
	}{
		{1, tcommon.Hash{}},
		{2, tcommon.Hash{}},
		{3, tcommon.Hash{0x33}},
		{4, tcommon.Hash{0x33}},
		{5, tcommon.Hash{0x33}},
		{6, tcommon.Hash{0x33}},
		{7, tcommon.Hash{0x77}},
		{10, tcommon.Hash{0x77}},
	}
	for _, tc := range cases {
		got, err := r.StorageAt(contract, slot, tc.n)
		if err != nil {
			t.Fatalf("StorageAt(slot, %d): %v", tc.n, err)
		}
		if got != tc.want {
			t.Errorf("StorageAt(slot, %d) = %x, want %x", tc.n, got, tc.want)
		}
	}
}

func TestPersistentHistoryReaderUsesKeyedColdHistory(t *testing.T) {
	owner := tcommon.BytesToAddress(append([]byte{tcommon.AddressPrefixMainnet}, bytes.Repeat([]byte{0x91}, tcommon.AccountIDLength)...))
	change := &rawdb.StateDomainChange{
		BlockNum:   2,
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 7,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot/a"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("new"),
	}
	cold := &keyedColdHistoryStub{changes: []*rawdb.StateDomainChange{change}}
	reader := NewPersistentHistoryReaderWithColdHistory(rawdb.NewMemoryDatabase(), nil, 2, cold)

	changes, err := reader.collectStateDomainChangesByKey(1, 2, rawdb.StateFlatDomainKVLatest, owner, 7, kvdomains.ContractStorage, []byte("slot/a"))
	if err != nil {
		t.Fatalf("collect keyed changes: %v", err)
	}
	if !cold.keyedCalled {
		t.Fatal("keyed cold history iterator was not used")
	}
	if cold.genericCalled {
		t.Fatal("generic cold history iterator was used despite keyed support")
	}
	if len(changes) != 1 || string(changes[0].Prev) != "old" {
		t.Fatalf("changes = %+v", changes)
	}
}

func TestPersistentHistoryReaderKeyedHotHistoryUsesInverseIndex(t *testing.T) {
	owner := tcommon.BytesToAddress(append([]byte{tcommon.AddressPrefixMainnet}, bytes.Repeat([]byte{0x90}, tcommon.AccountIDLength)...))
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateTxRange(db, 1, tcommon.Hash{}, 1, 1); err != nil {
		t.Fatalf("write tx range 1: %v", err)
	}
	if err := rawdb.WriteStateTxRange(db, 2, tcommon.Hash{}, 2, 2); err != nil {
		t.Fatalf("write tx range 2: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.ContractStorage, []byte("slot/a"), []byte("live")); err != nil {
		t.Fatalf("write latest kv: %v", err)
	}
	if err := rawdb.WriteStateDomainChange(db, &rawdb.StateDomainChange{
		BlockNum:   2,
		TxNum:      2,
		Seq:        1,
		FlatDomain: rawdb.StateFlatDomainKVLatest,
		Owner:      owner,
		Generation: 7,
		Domain:     kvdomains.ContractStorage,
		Key:        []byte("slot/a"),
		PrevExists: true,
		Prev:       []byte("old"),
		NextExists: true,
		Next:       []byte("live"),
	}); err != nil {
		t.Fatalf("write domain change: %v", err)
	}
	recording := &prefixRecordingDB{readerDB: db}
	reader := NewPersistentHistoryReaderWithColdHistory(recording, nil, 2, nil)

	changes, err := reader.collectStateDomainChangesByKey(1, 2, rawdb.StateFlatDomainKVLatest, owner, 7, kvdomains.ContractStorage, []byte("slot/a"))
	if err != nil {
		t.Fatalf("collect keyed hot history: %v", err)
	}
	if len(changes) != 1 || string(changes[0].Prev) != "old" {
		t.Fatalf("changes = %+v", changes)
	}
	for _, prefix := range recording.prefixes {
		if bytes.Equal(prefix, []byte("state-tx-range-v1-")) {
			t.Fatalf("keyed hot history scanned StateTxRange prefix: %q", prefix)
		}
	}
}

func TestPersistentHistoryReaderReadsAccountKVWithKeyedColdHistory(t *testing.T) {
	owner := tcommon.BytesToAddress(append([]byte{tcommon.AddressPrefixMainnet}, bytes.Repeat([]byte{0x92}, tcommon.AccountIDLength)...))
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateKVGeneration(db, owner, 7); err != nil {
		t.Fatalf("write generation: %v", err)
	}
	if err := rawdb.WriteStateKVLatest(db, owner, 7, kvdomains.ContractMetadata, []byte("meta"), []byte("live")); err != nil {
		t.Fatalf("write latest kv: %v", err)
	}
	cold := &keyedColdHistoryStub{changes: []*rawdb.StateDomainChange{
		{
			BlockNum:   2,
			TxNum:      2,
			Seq:        1,
			FlatDomain: rawdb.StateFlatDomainKVLatest,
			Owner:      owner,
			Generation: 7,
			Domain:     kvdomains.ContractMetadata,
			Key:        []byte("meta"),
			PrevExists: true,
			Prev:       []byte("old"),
			NextExists: true,
			Next:       []byte("live"),
		},
	}}
	reader := NewPersistentHistoryReaderWithColdHistory(db, nil, 2, cold)

	value, ok, err := reader.readStateAccountKVAsOf(owner, kvdomains.ContractMetadata, []byte("meta"), 1, 2)
	if err != nil {
		t.Fatalf("read account kv: %v", err)
	}
	if !ok || string(value) != "old" {
		t.Fatalf("value = %q, ok = %v", value, ok)
	}
	if cold.genericCalled {
		t.Fatal("generic cold history iterator was used despite keyed support")
	}
	if len(cold.keyedCalls) != 2 {
		t.Fatalf("keyed calls = %d, want 2", len(cold.keyedCalls))
	}
	if cold.keyedCalls[0].flatDomain != rawdb.StateFlatDomainKVLatest || cold.keyedCalls[1].flatDomain != rawdb.StateFlatDomainKVGeneration {
		t.Fatalf("keyed calls = %+v", cold.keyedCalls)
	}
}

func TestPersistentHistoryReaderColdMergeUsesHotLatestReader(t *testing.T) {
	owner := tcommon.BytesToAddress(append([]byte{tcommon.AddressPrefixMainnet}, bytes.Repeat([]byte{0x93}, tcommon.AccountIDLength)...))
	db := rawdb.NewMemoryDatabase()
	if err := rawdb.WriteStateTxRange(db, 2, tcommon.Hash{0x02}, 2, 2); err != nil {
		t.Fatalf("write tx range: %v", err)
	}
	accountLatest := []byte("typed-account-latest")
	kvKey := []byte("reward/typed-latest")
	kvValue := []byte("typed-kv")
	code := []byte{0x60, 0x02, 0x00}
	codeHash := tcommon.Keccak256(code)
	latest := &recordingHotStateLatestReader{
		account: map[tcommon.Address][]byte{owner: accountLatest},
		generation: map[tcommon.Address]uint64{
			owner: 7,
		},
		kv: map[string][]byte{
			recordingHotLatestKVKey(owner, 7, kvdomains.SystemReward, kvKey): kvValue,
		},
		code: map[tcommon.Hash][]byte{codeHash: code},
	}
	reader := NewPersistentHistoryReaderWithColdHistory(db, nil, 2, &keyedColdHistoryStub{})
	reader.latest = latest

	gotAccount, ok, err := reader.readStateAccountLatestAsOf(owner, 2, 2)
	if err != nil || !ok || !bytes.Equal(gotAccount, accountLatest) {
		t.Fatalf("account latest = %q ok=%v err=%v", gotAccount, ok, err)
	}
	gotGeneration, ok, err := reader.readStateKVGenerationAsOfTxNum(owner, 2, 2)
	if err != nil || !ok || gotGeneration != 7 {
		t.Fatalf("generation = %d ok=%v err=%v", gotGeneration, ok, err)
	}
	gotKV, ok, err := reader.readStateAccountKVAsOf(owner, kvdomains.SystemReward, kvKey, 2, 2)
	if err != nil || !ok || !bytes.Equal(gotKV, kvValue) {
		t.Fatalf("account kv = %q ok=%v err=%v", gotKV, ok, err)
	}
	gotCode, err := reader.readCodeByHashAtBlock(codeHash, 2)
	if err != nil || !bytes.Equal(gotCode, code) {
		t.Fatalf("code = %x err=%v", gotCode, err)
	}
	if _, ok, err := rawdb.ReadStateAccountLatest(db, owner); err != nil || ok {
		t.Fatalf("rawdb account latest unexpectedly available ok=%v err=%v", ok, err)
	}
	if _, ok, err := rawdb.ReadStateKVLatest(db, owner, 7, kvdomains.SystemReward, kvKey); err != nil || ok {
		t.Fatalf("rawdb kv latest unexpectedly available ok=%v err=%v", ok, err)
	}
	if len(rawdb.ReadStateCode(db, codeHash)) != 0 {
		t.Fatal("rawdb code latest unexpectedly available")
	}
	if !latest.saw("account") || !latest.saw("generation") || !latest.saw("kv") || !latest.saw("code") {
		t.Fatalf("hot latest calls = %v, want account/generation/kv/code", latest.calls)
	}
}

type keyedColdHistoryCall struct {
	flatDomain rawdb.StateFlatDomain
	owner      tcommon.Address
	generation uint64
	domain     kvdomains.KVDomain
	key        string
}

func mustSetAccountKV(t *testing.T, s *StateDB, owner tcommon.Address, domain kvdomains.KVDomain, key, value string) {
	t.Helper()
	if err := s.SetAccountKV(owner, domain, []byte(key), []byte(value)); err != nil {
		t.Fatalf("SetAccountKV(%s): %v", key, err)
	}
}

func collectHistoryAccountKVPrefix(t *testing.T, r *PersistentHistoryReader, owner tcommon.Address, domain kvdomains.KVDomain, prefix string, blockNum uint64) map[string]string {
	t.Helper()
	out := make(map[string]string)
	if err := r.AccountKVPrefixAt(owner, domain, []byte(prefix), blockNum, func(key, value []byte) (bool, error) {
		out[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("AccountKVPrefixAt block %d prefix %q: %v", blockNum, prefix, err)
	}
	return out
}

func collectHistoryKVLatestPrefix(t *testing.T, r *PersistentHistoryReader, owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, prefix string, blockNum uint64) map[string]string {
	t.Helper()
	out := make(map[string]string)
	if err := r.KVLatestPrefixAt(owner, generation, domain, []byte(prefix), blockNum, func(key, value []byte) (bool, error) {
		out[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("KVLatestPrefixAt block %d prefix %q: %v", blockNum, prefix, err)
	}
	return out
}

func collectStateDBAccountKVPrefix(t *testing.T, s *StateDB, owner tcommon.Address, domain kvdomains.KVDomain, prefix string) map[string]string {
	t.Helper()
	out := make(map[string]string)
	if err := s.IterateAccountKV(owner, domain, []byte(prefix), func(key, value []byte) (bool, error) {
		out[string(key)] = string(value)
		return true, nil
	}); err != nil {
		t.Fatalf("IterateAccountKV prefix %q: %v", prefix, err)
	}
	return out
}

type keyedColdHistoryStub struct {
	changes       []*rawdb.StateDomainChange
	keyedCalled   bool
	genericCalled bool
	keyedCalls    []keyedColdHistoryCall
}

type recordingHotStateLatestReader struct {
	account    map[tcommon.Address][]byte
	generation map[tcommon.Address]uint64
	kv         map[string][]byte
	code       map[tcommon.Hash][]byte
	calls      []string
}

func (r *recordingHotStateLatestReader) AccountLatest(owner tcommon.Address) ([]byte, bool, error) {
	r.calls = append(r.calls, "account")
	value, ok := r.account[owner]
	return append([]byte(nil), value...), ok, nil
}

func (r *recordingHotStateLatestReader) KVLatest(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) ([]byte, bool, error) {
	r.calls = append(r.calls, "kv")
	value, ok := r.kv[recordingHotLatestKVKey(owner, generation, domain, key)]
	return append([]byte(nil), value...), ok, nil
}

func (r *recordingHotStateLatestReader) KVGeneration(owner tcommon.Address) (uint64, bool, error) {
	r.calls = append(r.calls, "generation")
	value, ok := r.generation[owner]
	return value, ok, nil
}

func (r *recordingHotStateLatestReader) Code(hash tcommon.Hash) ([]byte, bool, error) {
	r.calls = append(r.calls, "code")
	value, ok := r.code[hash]
	return append([]byte(nil), value...), ok, nil
}

func (r *recordingHotStateLatestReader) saw(call string) bool {
	for _, got := range r.calls {
		if got == call {
			return true
		}
	}
	return false
}

func recordingHotLatestKVKey(owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte) string {
	return string(owner.Bytes()) + "/" + string(rawdb.EncodeStateKVGenerationValue(generation)) + "/" + string([]byte{byte(domain >> 8), byte(domain)}) + "/" + string(key)
}

func (s *keyedColdHistoryStub) IterateStateDomainChanges(fromTxNum, toTxNum uint64, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	s.genericCalled = true
	for _, change := range s.changes {
		if change.TxNum < fromTxNum || change.TxNum > toTxNum {
			continue
		}
		cont, err := fn(change)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

func (s *keyedColdHistoryStub) IterateStateDomainChangesByKey(fromTxNum, toTxNum uint64, flatDomain rawdb.StateFlatDomain, owner tcommon.Address, generation uint64, domain kvdomains.KVDomain, key []byte, fn func(*rawdb.StateDomainChange) (bool, error)) error {
	s.keyedCalled = true
	s.keyedCalls = append(s.keyedCalls, keyedColdHistoryCall{
		flatDomain: flatDomain,
		owner:      owner,
		generation: generation,
		domain:     domain,
		key:        string(key),
	})
	for _, change := range s.changes {
		if change.TxNum < fromTxNum || change.TxNum > toTxNum || change.FlatDomain != flatDomain || change.Owner != owner {
			continue
		}
		if flatDomain == rawdb.StateFlatDomainKVLatest && (change.Generation != generation || change.Domain != domain || !bytes.Equal(change.Key, key)) {
			continue
		}
		cont, err := fn(change)
		if err != nil || !cont {
			return err
		}
	}
	return nil
}

type prefixRecordingDB struct {
	readerDB
	prefixes [][]byte
}

func (p *prefixRecordingDB) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	p.prefixes = append(p.prefixes, append([]byte(nil), prefix...))
	return p.readerDB.NewIterator(prefix, start)
}

type stateCodeHidingDB struct {
	ethdb.Database
	hidden map[tcommon.Hash]struct{}
}

func (db *stateCodeHidingDB) Get(key []byte) ([]byte, error) {
	if hash, ok := rawdb.DecodeStateCodeKey(key); ok {
		if _, hide := db.hidden[hash]; hide {
			return nil, errors.New("hidden state code")
		}
	}
	return db.Database.Get(key)
}

// ---- counting adapter -----------------------------------------------------

// countingDB wraps a readerDB and counts NewIterator calls. Used by
// TestPersistentHistoryReader_CacheHit to verify the per-request cache
// absorbs repeated reads.
type countingDB struct {
	readerDB  readerDB
	iterCalls int64
}

func (c *countingDB) Has(key []byte) (bool, error) { return c.readerDB.Has(key) }
func (c *countingDB) Get(key []byte) ([]byte, error) {
	return c.readerDB.Get(key)
}
func (c *countingDB) NewIterator(prefix []byte, start []byte) ethdb.Iterator {
	atomic.AddInt64(&c.iterCalls, 1)
	return c.readerDB.NewIterator(prefix, start)
}

// Make sure the test wrapper satisfies the interfaces the reader needs.
var (
	_ ethdb.KeyValueReader = (*countingDB)(nil)
	_ ethdb.Iteratee       = (*countingDB)(nil)
)

// TestPersistentHistoryReader_RecreatedMultiSlotStorageNoLeak is the CONTROL for
// the account-KV "generation" investigation. PersistentHistoryReader.StorageAt
// threads the account envelope's generation (storageFromStateDomain in
// history.go reads envelope.AccountKVGeneration), which IS bumped on recreate.
// So contract storage must NOT leak across a destroy+recreate, even when the
// recreated account rewrites only one of the original slots. This passing test
// proves the envelope-threaded archive path is already correct.
func TestPersistentHistoryReader_RecreatedMultiSlotStorageNoLeak(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x72)
	other := testAddr(0x73)
	// java storage row keys are addrHash[:16] || slotKey[16:], so distinct slots
	// must differ in the low 16 bytes (byte 31 here), not the high half.
	slotA := tcommon.Hash{31: 0xA1}
	slotB := tcommon.Hash{31: 0xB1}
	oldA := tcommon.Hash{0x0A}
	oldB := tcommon.Hash{0x0B}
	newA := tcommon.Hash{0x1A}

	// Block 1: create addr at generation 0 with two storage slots.
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(addr, 100)
		s.SetState(addr, slotA, oldA)
		s.SetState(addr, slotB, oldB)
	})
	// Block 2: filler.
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) { s.AddBalance(other, 1) })
	// Block 3: destroy addr.
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) { s.SelfDestruct(addr) })
	// Block 4: recreate addr, rewriting ONLY slotA. slotB is left untouched at
	// the new generation, so it must read as empty.
	f.applyBlock(tcommon.Hash{0x04}, func(s *StateDB) {
		s.AddBalance(addr, 999)
		s.SetState(addr, slotA, newA)
	})
	// Block 5: filler.
	f.applyBlock(tcommon.Hash{0x05}, func(s *StateDB) { s.AddBalance(other, 1) })

	r := f.reader()

	// Before deletion (block 1): both old slots visible.
	if got, err := r.StorageAt(addr, slotA, 1); err != nil || got != oldA {
		t.Errorf("StorageAt(slotA, 1) = %x err=%v, want %x", got, err, oldA)
	}
	if got, err := r.StorageAt(addr, slotB, 1); err != nil || got != oldB {
		t.Errorf("StorageAt(slotB, 1) = %x err=%v, want %x", got, err, oldB)
	}
	// While destroyed (block 3): both zero.
	if got, err := r.StorageAt(addr, slotA, 3); err != nil || got != (tcommon.Hash{}) {
		t.Errorf("StorageAt(slotA, 3) = %x err=%v, want zero", got, err)
	}
	if got, err := r.StorageAt(addr, slotB, 3); err != nil || got != (tcommon.Hash{}) {
		t.Errorf("StorageAt(slotB, 3) = %x err=%v, want zero", got, err)
	}
	// After recreation (block 4): slotA is the NEW value; slotB must be empty
	// (the old-generation value must NOT leak into the recreated account).
	if got, err := r.StorageAt(addr, slotA, 4); err != nil || got != newA {
		t.Errorf("StorageAt(slotA, 4) = %x err=%v, want new %x", got, err, newA)
	}
	if got, err := r.StorageAt(addr, slotB, 4); err != nil || got != (tcommon.Hash{}) {
		t.Errorf("StorageAt(slotB, 4) = %x err=%v, want zero (old-generation slotB must not leak)", got, err)
	}
}

// TestStateDB_RecreatedAccountKVAsOfDoesNotLeakOldGeneration exposes the
// account-KV generation divergence on the GENERIC archive read path
// (StateDB.GetAccountKVAsOf -> rawdb.ReadStateAccountKVAsOfTxNum), which seeds
// the generation from the separate KVGeneration row instead of the account
// envelope. GetOrCreateAccount bumps the generation on recreate but never marks
// it dirty, so the recreate bump is never persisted to the KVGeneration row or
// its change-set. Live reads use the bumped envelope generation; the archive
// read used the stale row and leaked the destroyed account's storage.
//
// ContractRuntimeState models a CREATE2-resurrected contract's per-account KV.
func TestStateDB_RecreatedAccountKVAsOfDoesNotLeakOldGeneration(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x74)
	other := testAddr(0x75)
	domain := kvdomains.ContractRuntimeState
	keyA := []byte("slotA")
	keyB := []byte("slotB")

	mustSet := func(s *StateDB, key, val []byte) {
		t.Helper()
		if err := s.SetAccountKV(addr, domain, key, val); err != nil {
			t.Fatalf("SetAccountKV(%s): %v", key, err)
		}
	}

	// Block 1: create addr at generation 0 with two KV entries.
	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(addr, 100)
		mustSet(s, keyA, []byte("a0"))
		mustSet(s, keyB, []byte("b0"))
	})
	// Block 2: filler.
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) { s.AddBalance(other, 1) })
	// Block 3: destroy addr.
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) { s.SelfDestruct(addr) })
	// Block 4: recreate addr at the next generation, rewriting ONLY keyA.
	f.applyBlock(tcommon.Hash{0x04}, func(s *StateDB) {
		s.AddBalance(addr, 999)
		mustSet(s, keyA, []byte("a1"))
	})
	// Block 5: filler.
	f.applyBlock(tcommon.Hash{0x05}, func(s *StateDB) { s.AddBalance(other, 1) })

	// Root-cause visibility: the persisted KVGeneration row. Before the fix it
	// is the stale pre-delete value (0); after the fix it tracks the envelope (1).
	if gen, ok, err := rawdb.ReadStateKVGeneration(f.disk, addr); err != nil {
		t.Fatalf("ReadStateKVGeneration: %v", err)
	} else if !ok || gen != 1 {
		t.Fatalf("persisted KVGeneration row = %d exists=%v, want 1 (must track the envelope/live generation)", gen, ok)
	}

	// Live reads (head): recreated account at generation 1.
	if got, ok, err := f.state.GetAccountKV(addr, domain, keyA); err != nil || !ok || string(got) != "a1" {
		t.Fatalf("live GetAccountKV(keyA) = %q ok=%v err=%v, want \"a1\"", got, ok, err)
	}
	if _, ok, err := f.state.GetAccountKV(addr, domain, keyB); err != nil || ok {
		t.Fatalf("live GetAccountKV(keyB) ok=%v err=%v, want absent (not rewritten at the new generation)", ok, err)
	}

	// Archive read at the post-recreate block (4) must AGREE with the live read:
	// keyA is the new value, keyB is absent. Before the fix the archive read
	// seeds the stale generation (0) and leaks the destroyed account's storage.
	if got, ok, err := f.state.GetAccountKVAsOf(addr, domain, keyA, 4, f.head); err != nil {
		t.Fatalf("GetAccountKVAsOf(keyA, 4): %v", err)
	} else if !ok || string(got) != "a1" {
		t.Errorf("archive GetAccountKVAsOf(keyA, 4) = %q ok=%v, want \"a1\" (stale old-generation value leaked)", got, ok)
	}
	if got, ok, err := f.state.GetAccountKVAsOf(addr, domain, keyB, 4, f.head); err != nil {
		t.Fatalf("GetAccountKVAsOf(keyB, 4): %v", err)
	} else if ok {
		t.Errorf("archive GetAccountKVAsOf(keyB, 4) = %q ok=true, want absent (destroyed account's slot leaked into recreated account)", got)
	}

	// Guard: the pre-destruction history (block 1) must STILL reconstruct the
	// original generation-0 values after the fix bumps the row, i.e. the
	// backward generation-boundary replay stays correct.
	if got, ok, err := f.state.GetAccountKVAsOf(addr, domain, keyA, 1, f.head); err != nil || !ok || string(got) != "a0" {
		t.Errorf("archive GetAccountKVAsOf(keyA, 1) = %q ok=%v err=%v, want \"a0\"", got, ok, err)
	}
	if got, ok, err := f.state.GetAccountKVAsOf(addr, domain, keyB, 1, f.head); err != nil || !ok || string(got) != "b0" {
		t.Errorf("archive GetAccountKVAsOf(keyB, 1) = %q ok=%v err=%v, want \"b0\"", got, ok, err)
	}
}

func TestPersistentHistoryReaderAccountKVPrefixAtDoesNotLeakOldGeneration(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x7B)
	other := testAddr(0x7C)
	domain := kvdomains.ContractRuntimeState

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(addr, 100)
		mustSetAccountKV(t, s, addr, domain, "slot/a", "a0")
		mustSetAccountKV(t, s, addr, domain, "slot/b", "b0")
	})
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) { s.AddBalance(other, 1) })
	f.applyBlock(tcommon.Hash{0x03}, func(s *StateDB) { s.SelfDestruct(addr) })
	f.applyBlock(tcommon.Hash{0x04}, func(s *StateDB) {
		s.AddBalance(addr, 999)
		mustSetAccountKV(t, s, addr, domain, "slot/a", "a1")
	})
	f.applyBlock(tcommon.Hash{0x05}, func(s *StateDB) { s.AddBalance(other, 1) })

	r := f.reader()
	beforeDestroy := collectHistoryAccountKVPrefix(t, r, addr, domain, "slot/", 1)
	if len(beforeDestroy) != 2 || beforeDestroy["slot/a"] != "a0" || beforeDestroy["slot/b"] != "b0" {
		t.Fatalf("block 1 prefix = %v, want old generation values", beforeDestroy)
	}
	afterRecreate := collectHistoryAccountKVPrefix(t, r, addr, domain, "slot/", 4)
	if len(afterRecreate) != 1 || afterRecreate["slot/a"] != "a1" {
		t.Fatalf("block 4 prefix = %v, want only recreated generation slot/a=a1", afterRecreate)
	}
}

// TestGetOrCreateAccount_RecreateGenerationRevert verifies snapshot revert of
// the generation bump GetOrCreateAccount records on recreate (the kvResetChange
// journal entry added for archive parity). Reverting must restore the destroyed
// account, must not corrupt generation accounting, and must not panic on the
// reset overlay (kvResetChange.revert assigns prevDirty directly to kvDirty, so
// it must be a non-nil map).
func TestGetOrCreateAccount_RecreateGenerationRevert(t *testing.T) {
	f := newHistoryFixture(t)
	addr := testAddr(0x76)
	domain := kvdomains.ContractRuntimeState

	f.applyBlock(tcommon.Hash{0x01}, func(s *StateDB) {
		s.AddBalance(addr, 100)
		if err := s.SetAccountKV(addr, domain, []byte("k"), []byte("v0")); err != nil {
			t.Fatalf("SetAccountKV: %v", err)
		}
	})
	// Destroy in its own block so the committed object is marked deleted; only
	// then does GetOrCreateAccount take the recreate path (a same-block
	// SELFDESTRUCT leaves the object self-destructed-but-not-deleted).
	f.applyBlock(tcommon.Hash{0x02}, func(s *StateDB) { s.SelfDestruct(addr) })

	s := f.state
	if s.Exist(addr) {
		t.Fatalf("addr should be destroyed before recreate")
	}
	snap := s.Snapshot()
	if err := s.SetAccountKV(addr, domain, []byte("k"), []byte("v1")); err != nil {
		t.Fatalf("recreate SetAccountKV: %v", err)
	}
	s.AddBalance(addr, 5)
	if !s.Exist(addr) {
		t.Fatalf("addr should exist after recreate")
	}
	if got, ok, err := s.GetAccountKV(addr, domain, []byte("k")); err != nil || !ok || string(got) != "v1" {
		t.Fatalf("recreated GetAccountKV(k) = %q ok=%v err=%v, want \"v1\"", got, ok, err)
	}

	s.RevertToSnapshot(snap)

	if s.Exist(addr) {
		t.Fatalf("addr should be destroyed again after revert")
	}
	if _, ok, err := s.GetAccountKV(addr, domain, []byte("k")); err != nil || ok {
		t.Fatalf("after revert GetAccountKV(k) ok=%v err=%v, want absent", ok, err)
	}
	// A fresh recreate after revert must still work (reset overlay usable, no
	// nil-map panic) and recompute the generation.
	if err := s.SetAccountKV(addr, domain, []byte("k"), []byte("v2")); err != nil {
		t.Fatalf("recreate-after-revert SetAccountKV: %v", err)
	}
	if got, ok, err := s.GetAccountKV(addr, domain, []byte("k")); err != nil || !ok || string(got) != "v2" {
		t.Fatalf("recreate-after-revert GetAccountKV(k) = %q ok=%v err=%v, want \"v2\"", got, ok, err)
	}
}
