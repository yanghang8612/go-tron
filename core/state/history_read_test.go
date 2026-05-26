package state

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
	statesnapshots "github.com/tronprotocol/go-tron/core/state/snapshots"
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
