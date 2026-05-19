package state

import (
	"bytes"
	"sync/atomic"
	"testing"

	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
)

// historyFixture spins up an in-memory disk store and a StateDB that
// persists through it. Each call to applyBlock mutates the state under
// the caller-supplied function, calls AccumulateHistory in capture mode,
// then Commit()s — exactly mirroring the applyBlock contract in
// core/blockchain.go (history flush BEFORE Commit because Commit clears
// the journal).
type historyFixture struct {
	t     *testing.T
	disk  ethdb.Database
	state *StateDB
	head  uint64
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
	fn(f.state)
	f.state.SetHistoryEnabled(true)
	if err := f.state.AccumulateHistory(f.disk, f.head, blockHash); err != nil {
		f.t.Fatalf("AccumulateHistory block=%d: %v", f.head, err)
	}
	if _, err := f.state.Commit(); err != nil {
		f.t.Fatalf("Commit block=%d: %v", f.head, err)
	}
	// SetHistoryEnabled persists across the AccumulateHistory→Commit
	// boundary on the same StateDB; reset so the next block's
	// applyBlock's GetState pre-warm path runs in the same shape as
	// production (history flag flipped on per-block).
	f.state.SetHistoryEnabled(false)
}

// reader builds a fresh per-request reader pinned to the current head.
func (f *historyFixture) reader() *PersistentHistoryReader {
	return NewPersistentHistoryReader(f.disk, f.state, f.head)
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
		bn := n
		_ = bn
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
		bn := n
		_ = bn
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
	})

	// Blocks 4-6: addr is untouched.
	for n := uint64(4); n <= 6; n++ {
		f.applyBlock(tcommon.Hash{byte(n)}, func(s *StateDB) {
			s.AddBalance(other, 1)
		})
	}

	// Block 7: destroy addr. Mirror the gtron VM flow (opSelfDestruct in
	// vm/instructions.go) which calls both SelfDestruct (records the
	// selfDestructChange entry) and DeleteAccount (records the
	// accountChange + codeChange entries the SHI capture path actually
	// consumes; also flips obj.deleted=true so Commit drops the trie
	// row).
	f.applyBlock(tcommon.Hash{0x07}, func(s *StateDB) {
		s.SelfDestruct(addr)
		s.DeleteAccount(addr)
	})

	// Block 8: addr is untouched.
	f.applyBlock(tcommon.Hash{0x08}, func(s *StateDB) {
		s.AddBalance(other, 1)
	})

	// Block 9: recreate addr.
	f.applyBlock(tcommon.Hash{0x09}, func(s *StateDB) {
		s.AddBalance(addr, 999)
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
}

// TestPersistentHistoryReader_StorageSlotZeroPreValue exercises the
// slotSentinelZero round-trip from slice 1's accessor.
//
// Setup:
//  1. Block 1: write slot = 0xDEAD on contract.
//  2. Block 2: write slot = 0x0000 (zero-out — pre-block was 0xDEAD).
//  3. Query StorageAt(slot, 1) → must return 0xDEAD (pre-block-2 value).
//
// The capture path at block 2 stores 0xDEAD as preValue under the
// slotSentinelZero discriminator path (because preValue != 0); the
// reader path through ReadSlotDelta is the inverse half of the same
// round-trip. Because we also test the dense case (write zero to a
// then-non-zero slot), this also confirms the slot rollback walk does
// the right thing when the pre-block value happens to be the
// zero-sentinel itself.
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
	slot := tcommon.Hash{0x42}
	other := tcommon.Hash{0x43}

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

// ---- counting adapter -----------------------------------------------------

// countingDB wraps a readerDB and counts NewIterator calls. Used by
// TestPersistentHistoryReader_CacheHit to verify the per-request cache
// absorbs repeated reads.
type countingDB struct {
	readerDB readerDB
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

