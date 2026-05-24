package state

import (
	"testing"

	ethcommon "github.com/ethereum/go-ethereum/common"
	ethrawdb "github.com/ethereum/go-ethereum/core/rawdb"
	ethtypes "github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/ethdb"
	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

func newTestStateDB(t *testing.T) *StateDB {
	t.Helper()
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	return sdb
}

func testAddr(b byte) tcommon.Address {
	var addr tcommon.Address
	addr[0] = 0x41
	addr[20] = b
	return addr
}

func TestStateDBGetSetBalance(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100_000_000)
	if got := sdb.GetBalance(addr); got != 100_000_000 {
		t.Fatalf("balance: want 100000000, got %d", got)
	}
	if err := sdb.SubBalance(addr, 30_000_000); err != nil {
		t.Fatal(err)
	}
	if got := sdb.GetBalance(addr); got != 70_000_000 {
		t.Fatalf("balance after sub: want 70000000, got %d", got)
	}
}

func TestStateDBSubBalanceInsufficient(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 50)
	err := sdb.SubBalance(addr, 100)
	if err == nil {
		t.Fatal("expected error for insufficient balance")
	}
}

func TestStateDBSnapshotRevert(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100)
	snap := sdb.Snapshot()
	sdb.AddBalance(addr, 50)
	if got := sdb.GetBalance(addr); got != 150 {
		t.Fatalf("before revert: want 150, got %d", got)
	}
	sdb.RevertToSnapshot(snap)
	if got := sdb.GetBalance(addr); got != 100 {
		t.Fatalf("after revert: want 100, got %d", got)
	}
}

func TestStateDBSnapshotRevertNewAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	snap := sdb.Snapshot()
	addr := testAddr(2)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 200)
	sdb.RevertToSnapshot(snap)
	acc := sdb.GetAccount(addr)
	if acc != nil {
		t.Fatal("account should not exist after revert")
	}
}

func TestStateDBCommitChangesRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	emptyRoot := ethcommon.Hash(ethtypes.EmptyRootHash)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	if ethcommon.Hash(root) == emptyRoot {
		t.Fatal("root should differ from empty root after commit")
	}
}

func TestStateDBCommitDeterministic(t *testing.T) {
	makeState := func() tcommon.Hash {
		diskdb := ethrawdb.NewMemoryDatabase()
		db := NewDatabase(diskdb)
		sdb, _ := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
		addr1 := testAddr(1)
		addr2 := testAddr(2)
		sdb.GetOrCreateAccount(addr1)
		sdb.AddBalance(addr1, 500)
		sdb.GetOrCreateAccount(addr2)
		sdb.AddBalance(addr2, 300)
		root, _ := sdb.Commit()
		return root
	}
	root1 := makeState()
	root2 := makeState()
	if root1 != root2 {
		t.Fatalf("roots differ: %x vs %x", root1, root2)
	}
}

func TestStateDBFreezeV2(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 100_000_000)
	sdb.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 50_000_000)
	acc := sdb.GetAccount(addr)
	if acc == nil {
		t.Fatal("account not found")
	}
	if got := acc.GetFrozenV2Amount(corepb.ResourceCode_BANDWIDTH); got != 50_000_000 {
		t.Fatalf("frozen bandwidth: want 50000000, got %d", got)
	}
}

func TestStateDBCommitThenContinue(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)

	root1, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	// StateDB should still be usable after commit
	sdb.AddBalance(addr, 500)
	if got := sdb.GetBalance(addr); got != 1500 {
		t.Fatalf("balance after second add: want 1500, got %d", got)
	}

	root2, err := sdb.Commit()
	if err != nil {
		t.Fatal("second commit failed:", err)
	}

	if root1 == root2 {
		t.Fatal("roots should differ after second commit")
	}
}

func TestStateDBCommitPersistsHistoricalAccountTrieRoots(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	sdb.GetOrCreateAccount(addr)
	sdb.AddBalance(addr, 1000)
	root1, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit root1: %v", err)
	}

	atRoot1, err := New(root1, sdb.db)
	if err != nil {
		t.Fatalf("open root1: %v", err)
	}
	if got := atRoot1.GetBalance(addr); got != 1000 {
		t.Fatalf("balance at root1 = %d, want 1000", got)
	}

	sdb.AddBalance(addr, 500)
	root2, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit root2: %v", err)
	}
	atRoot2, err := New(root2, sdb.db)
	if err != nil {
		t.Fatalf("open root2: %v", err)
	}
	if got := atRoot2.GetBalance(addr); got != 1500 {
		t.Fatalf("balance at root2 = %d, want 1500", got)
	}
	atRoot1Again, err := New(root1, sdb.db)
	if err != nil {
		t.Fatalf("reopen root1: %v", err)
	}
	if got := atRoot1Again.GetBalance(addr); got != 1000 {
		t.Fatalf("balance at root1 after root2 commit = %d, want 1000", got)
	}
}

func TestStateDBWitness(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(1)
	w := sdb.GetWitness(addr)
	if w != nil {
		t.Fatal("witness should not exist")
	}
	sdb.GetOrCreateAccount(addr)
	sdb.PutWitness(addr, "http://example.com")
	w = sdb.GetWitness(addr)
	if w == nil {
		t.Fatal("witness should exist")
	}
	if w.URL() != "http://example.com" {
		t.Fatalf("witness url: want http://example.com, got %s", w.URL())
	}
}

// TestStateDBFlushWitnesses_VoteCountDelta covers the D-2.c scenario: a
// VoteWitness-style in-memory delta on a pre-existing witness must persist
// to rawdb without clobbering production counters. Prior to the fix the
// delta lived only in s.witnesses and was discarded after the block,
// causing accumulateWitnessVi to use a stale VoteCount on every subsequent
// maintenance.
func TestStateDBFlushWitnesses_VoteCountDelta(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	addr := testAddr(1)

	// Seed disk with a witness that already has counters from prior blocks.
	pre := types.NewWitness(addr, "http://sr-a")
	pre.SetVoteCount(100)
	pre.SetTotalProduced(42)
	pre.SetTotalMissed(7)
	pre.SetLatestBlockNum(99)
	pre.SetLatestSlotNum(101)
	rawdb.WriteWitness(diskdb, addr, pre)

	// Fresh statedb opens with disk-backed Database; pre-load picks up the
	// existing record (VoteCount=100, URL set).
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), NewDatabase(diskdb))
	if err != nil {
		t.Fatal(err)
	}
	sdb.PutWitness(addr, pre.URL())
	sdb.AddWitnessVoteCount(addr, pre.VoteCount())

	// Simulate VoteWitnessActuator applying a +1 delta in-memory.
	sdb.AddWitnessVoteCount(addr, 1)
	if got := sdb.GetWitness(addr).VoteCount(); got != 101 {
		t.Fatalf("in-memory VoteCount = %d, want 101", got)
	}

	// Flush: VoteCount must reach disk; counters must be preserved.
	sdb.FlushWitnesses(diskdb)

	post := rawdb.ReadWitness(diskdb, addr)
	if post == nil {
		t.Fatal("witness disappeared from disk after flush")
	}
	if post.VoteCount() != 101 {
		t.Errorf("VoteCount = %d, want 101", post.VoteCount())
	}
	if post.URL() != "http://sr-a" {
		t.Errorf("URL = %q, want http://sr-a", post.URL())
	}
	if post.TotalProduced() != 42 {
		t.Errorf("TotalProduced = %d, want 42 (must not be clobbered)", post.TotalProduced())
	}
	if post.TotalMissed() != 7 {
		t.Errorf("TotalMissed = %d, want 7 (must not be clobbered)", post.TotalMissed())
	}
	if post.LatestBlockNum() != 99 || post.LatestSlotNum() != 101 {
		t.Errorf("Latest block/slot clobbered: got (%d,%d), want (99,101)",
			post.LatestBlockNum(), post.LatestSlotNum())
	}
}

// TestStateDBFlushWitnesses_FreshWitness covers the WitnessCreate path
// where the in-memory record exists but rawdb has no entry yet. Flush
// should write the in-memory record verbatim so counters default to zero.
func TestStateDBFlushWitnesses_FreshWitness(t *testing.T) {
	sdb := newTestStateDB(t)
	addr := testAddr(2)
	sdb.PutWitness(addr, "http://new-sr")
	sdb.AddWitnessVoteCount(addr, 5)

	diskdb := ethrawdb.NewMemoryDatabase()
	sdb.FlushWitnesses(diskdb)

	post := rawdb.ReadWitness(diskdb, addr)
	if post == nil {
		t.Fatal("fresh witness must be written to disk on flush")
	}
	if post.URL() != "http://new-sr" || post.VoteCount() != 5 {
		t.Fatalf("fresh witness fields wrong: url=%q votes=%d", post.URL(), post.VoteCount())
	}
}

// countingKV wraps an ethdb.KeyValueStore and tallies Has/Get/Put/Delete
// calls. Used by the FlushWitnesses dirty-set tests to assert that no-op
// blocks issue zero rawdb operations while mutated witnesses issue exactly
// one Read + one Write each.
type countingKV struct {
	inner         ethdb.KeyValueStore
	hasN, getN    int
	putN, deleteN int
}

func newCountingKV() *countingKV {
	return &countingKV{inner: ethrawdb.NewMemoryDatabase()}
}

func (c *countingKV) Has(key []byte) (bool, error) {
	c.hasN++
	return c.inner.Has(key)
}

func (c *countingKV) Get(key []byte) ([]byte, error) {
	c.getN++
	return c.inner.Get(key)
}

func (c *countingKV) Put(key []byte, value []byte) error {
	c.putN++
	return c.inner.Put(key, value)
}

func (c *countingKV) Delete(key []byte) error {
	c.deleteN++
	return c.inner.Delete(key)
}

// seedWitness writes a witness to the underlying store directly (bypassing
// the counters) and returns the seeded record. Used to prepare the rawdb
// state that LoadWitness will hydrate from.
func (c *countingKV) seedWitness(t *testing.T, addr tcommon.Address, url string, votes int64) *types.Witness {
	t.Helper()
	w := types.NewWitness(addr, url)
	w.SetVoteCount(votes)
	rawdb.WriteWitness(c.inner, addr, w)
	return w
}

// resetCounts zeroes the call counters after seeding so subsequent
// assertions only see calls from the operation under test.
func (c *countingKV) resetCounts() {
	c.hasN, c.getN, c.putN, c.deleteN = 0, 0, 0, 0
}

// TestFlushWitnesses_NoMutation covers the common case: preload N
// witnesses from rawdb and flush without touching any of them. The dirty
// set must be empty, so FlushWitnesses issues zero Reads and zero Writes.
// This is the hot path the perf change targets — most blocks have no
// VoteWitness, WitnessUpdate, or Unfreeze tx.
func TestFlushWitnesses_NoMutation(t *testing.T) {
	const n = 27
	kv := newCountingKV()
	addrs := make([]tcommon.Address, n)
	for i := 0; i < n; i++ {
		addrs[i] = testAddr(byte(0x80 + i))
		kv.seedWitness(t, addrs[i], "http://sr", int64(100+i))
	}

	sdb := newTestStateDB(t)
	for i := 0; i < n; i++ {
		sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addrs[i]))
	}

	kv.resetCounts()
	sdb.FlushWitnesses(kv)
	if kv.getN != 0 || kv.hasN != 0 || kv.putN != 0 || kv.deleteN != 0 {
		t.Fatalf("expected zero db ops on no-mutation flush, got get=%d has=%d put=%d delete=%d",
			kv.getN, kv.hasN, kv.putN, kv.deleteN)
	}
}

// TestFlushWitnesses_SingleVoteMutation: one VoteWitness-style delta
// against a preloaded witness. Flush must issue exactly one rawdb Read
// (Has+Get) and one Put for the touched address; the other preloaded
// witnesses stay untouched.
func TestFlushWitnesses_SingleVoteMutation(t *testing.T) {
	kv := newCountingKV()
	addr1 := testAddr(0xA1)
	addr2 := testAddr(0xA2)
	kv.seedWitness(t, addr1, "http://sr1", 50)
	kv.seedWitness(t, addr2, "http://sr2", 60)

	sdb := newTestStateDB(t)
	sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addr1))
	sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addr2))

	sdb.AddWitnessVoteCount(addr1, 10)

	kv.resetCounts()
	sdb.FlushWitnesses(kv)

	// Has+Get for the read, Put for the merged write.
	if kv.getN != 1 || kv.putN != 1 || kv.deleteN != 0 {
		t.Fatalf("single-mutation flush: got get=%d has=%d put=%d delete=%d, want get=1 put=1 delete=0",
			kv.getN, kv.hasN, kv.putN, kv.deleteN)
	}

	post := rawdb.ReadWitness(kv.inner, addr1)
	if post.VoteCount() != 60 {
		t.Fatalf("VoteCount = %d, want 60", post.VoteCount())
	}
	post2 := rawdb.ReadWitness(kv.inner, addr2)
	if post2.VoteCount() != 60 {
		t.Fatalf("addr2 VoteCount = %d, want 60 (untouched)", post2.VoteCount())
	}
}

// TestFlushWitnesses_MultiMutation: VoteCount on addr1, URL on addr2.
// Both witnesses must flush; the third preloaded witness stays untouched.
func TestFlushWitnesses_MultiMutation(t *testing.T) {
	kv := newCountingKV()
	addr1 := testAddr(0xB1)
	addr2 := testAddr(0xB2)
	addr3 := testAddr(0xB3)
	kv.seedWitness(t, addr1, "http://sr1", 10)
	kv.seedWitness(t, addr2, "http://sr2-old", 20)
	kv.seedWitness(t, addr3, "http://sr3", 30)

	sdb := newTestStateDB(t)
	sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addr1))
	sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addr2))
	sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addr3))

	sdb.AddWitnessVoteCount(addr1, 5)
	sdb.SetWitnessURL(addr2, "http://sr2-new")

	kv.resetCounts()
	sdb.FlushWitnesses(kv)

	if kv.getN != 2 || kv.putN != 2 {
		t.Fatalf("multi-mutation flush: got get=%d put=%d, want get=2 put=2", kv.getN, kv.putN)
	}

	post1 := rawdb.ReadWitness(kv.inner, addr1)
	if post1.VoteCount() != 15 {
		t.Errorf("addr1 VoteCount = %d, want 15", post1.VoteCount())
	}
	post2 := rawdb.ReadWitness(kv.inner, addr2)
	if post2.URL() != "http://sr2-new" {
		t.Errorf("addr2 URL = %q, want http://sr2-new", post2.URL())
	}
	post3 := rawdb.ReadWitness(kv.inner, addr3)
	if post3.URL() != "http://sr3" || post3.VoteCount() != 30 {
		t.Errorf("addr3 changed unexpectedly: url=%q votes=%d", post3.URL(), post3.VoteCount())
	}
}

// TestFlushWitnesses_RemutationClearsBetweenFlushes: mutate addr1, flush
// (clears dirty set), then mutate addr2 only. The second flush must only
// touch addr2 — the dirty set must NOT carry addr1 forward.
func TestFlushWitnesses_RemutationClearsBetweenFlushes(t *testing.T) {
	kv := newCountingKV()
	addr1 := testAddr(0xC1)
	addr2 := testAddr(0xC2)
	kv.seedWitness(t, addr1, "http://sr1", 5)
	kv.seedWitness(t, addr2, "http://sr2", 7)

	sdb := newTestStateDB(t)
	sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addr1))
	sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addr2))

	// Round 1: mutate addr1 only.
	sdb.AddWitnessVoteCount(addr1, 1)
	kv.resetCounts()
	sdb.FlushWitnesses(kv)
	if kv.getN != 1 || kv.putN != 1 {
		t.Fatalf("round 1: got get=%d put=%d, want 1/1", kv.getN, kv.putN)
	}

	// Round 2: mutate addr2 only. The dirty set must have been cleared,
	// so addr1 must not flush again.
	sdb.AddWitnessVoteCount(addr2, 1)
	kv.resetCounts()
	sdb.FlushWitnesses(kv)
	if kv.getN != 1 || kv.putN != 1 {
		t.Fatalf("round 2: got get=%d put=%d, want 1/1 (addr1 must NOT re-flush)", kv.getN, kv.putN)
	}

	post1 := rawdb.ReadWitness(kv.inner, addr1)
	if post1.VoteCount() != 6 {
		t.Errorf("addr1 final VoteCount = %d, want 6", post1.VoteCount())
	}
	post2 := rawdb.ReadWitness(kv.inner, addr2)
	if post2.VoteCount() != 8 {
		t.Errorf("addr2 final VoteCount = %d, want 8", post2.VoteCount())
	}
}

// TestFlushWitnesses_RevertedWitnessSurvives: a witness created and then
// reverted within the block leaves a stale dirty mark but no in-memory
// record. The flush must tolerate this (the nil-guard inside the loop)
// and do zero work — there is no witness to write.
func TestFlushWitnesses_RevertedWitnessSurvives(t *testing.T) {
	kv := newCountingKV()
	sdb := newTestStateDB(t)
	addr := testAddr(0xD1)

	snap := sdb.Snapshot()
	sdb.PutWitness(addr, "http://transient")
	sdb.AddWitnessVoteCount(addr, 42)
	sdb.RevertToSnapshot(snap)

	if sdb.GetWitness(addr) != nil {
		t.Fatal("witness should be reverted out of the in-memory map")
	}

	kv.resetCounts()
	sdb.FlushWitnesses(kv)
	if kv.getN != 0 || kv.putN != 0 {
		t.Fatalf("reverted witness: got get=%d put=%d, want 0/0", kv.getN, kv.putN)
	}
}

// TestFlushWitnesses_LoadWitnessDoesNotMarkDirty is the explicit guard
// against the regression where preload paths mark every preloaded witness
// dirty (the bug this perf change exists to fix).
func TestFlushWitnesses_LoadWitnessDoesNotMarkDirty(t *testing.T) {
	kv := newCountingKV()
	addr := testAddr(0xE1)
	kv.seedWitness(t, addr, "http://sr", 100)

	sdb := newTestStateDB(t)
	sdb.LoadWitness(rawdb.ReadWitness(kv.inner, addr))

	// Sanity: witness is visible in-memory.
	if sdb.GetWitness(addr).VoteCount() != 100 {
		t.Fatal("LoadWitness should hydrate VoteCount")
	}

	kv.resetCounts()
	sdb.FlushWitnesses(kv)
	if kv.getN != 0 || kv.putN != 0 {
		t.Fatalf("LoadWitness must not mark dirty: got get=%d put=%d", kv.getN, kv.putN)
	}
}

func TestStateDBAccountExists(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)

	if db.AccountExists(addr) {
		t.Fatal("should not exist before creation")
	}

	db.CreateAccount(addr, corepb.AccountType_Normal)
	if !db.AccountExists(addr) {
		t.Fatal("should exist after creation")
	}
}

func TestStateDBFreezeAndUnfreeze(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)
	db.AddBalance(addr, 10_000_000)

	// Freeze
	db.AddFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 5_000_000)
	if got := db.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH); got != 5_000_000 {
		t.Fatalf("frozen: got %d, want 5000000", got)
	}
	if got := db.TotalFrozenV2(addr); got != 5_000_000 {
		t.Fatalf("total frozen: got %d, want 5000000", got)
	}

	// Reduce frozen
	db.ReduceFreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000)
	if got := db.GetFrozenV2Amount(addr, corepb.ResourceCode_BANDWIDTH); got != 3_000_000 {
		t.Fatalf("after reduce: got %d, want 3000000", got)
	}

	// Add unfreeze entry
	db.AddUnfreezeV2(addr, corepb.ResourceCode_BANDWIDTH, 2_000_000, 100_000)
	if got := db.UnfreezeV2Count(addr); got != 1 {
		t.Fatalf("unfreeze count: got %d, want 1", got)
	}

	// Remove expired
	withdrawn := db.RemoveExpiredUnfreezeV2(addr, 200_000)
	if withdrawn != 2_000_000 {
		t.Fatalf("withdrawn: got %d, want 2000000", withdrawn)
	}
	if got := db.UnfreezeV2Count(addr); got != 0 {
		t.Fatalf("unfreeze count after remove: got %d, want 0", got)
	}
}

func TestStateDBVotes(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)

	votes := []*corepb.Vote{
		{VoteAddress: testAddr(0x10).Bytes(), VoteCount: 100},
		{VoteAddress: testAddr(0x11).Bytes(), VoteCount: 200},
	}
	db.SetVotes(addr, votes)
	got := db.GetVotes(addr)
	if len(got) != 2 {
		t.Fatalf("votes: got %d, want 2", len(got))
	}

	db.ClearVotes(addr)
	got = db.GetVotes(addr)
	if len(got) != 0 {
		t.Fatalf("after clear: got %d votes, want 0", len(got))
	}
}

func TestStateDBAllowanceAndWithdraw(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x01)
	db.CreateAccount(addr, corepb.AccountType_Normal)

	db.SetAllowance(addr, 1000)
	if got := db.GetAllowance(addr); got != 1000 {
		t.Fatalf("allowance: got %d, want 1000", got)
	}

	db.AddAllowance(addr, 500)
	if got := db.GetAllowance(addr); got != 1500 {
		t.Fatalf("after add: got %d, want 1500", got)
	}

	db.SetLatestWithdrawTime(addr, 999)
	if got := db.GetLatestWithdrawTime(addr); got != 999 {
		t.Fatalf("withdraw time: got %d, want 999", got)
	}
}

func TestStateDBLoadAccountCopiesAndJournalsMutations(t *testing.T) {
	db := newTestStateDB(t)
	addr := testAddr(0x02)
	acc := types.NewAccount(addr, corepb.AccountType_Normal)
	acc.SetAllowance(100)

	db.LoadAccount(acc)
	acc.SetAllowance(999)
	if got := db.GetAllowance(addr); got != 100 {
		t.Fatalf("loaded account should be detached from source: got %d, want 100", got)
	}

	snap := db.Snapshot()
	db.AddAllowance(addr, 50)
	if got := db.GetAllowance(addr); got != 150 {
		t.Fatalf("after add: got %d, want 150", got)
	}
	db.RevertToSnapshot(snap)
	if got := db.GetAllowance(addr); got != 100 {
		t.Fatalf("after revert: got %d, want 100", got)
	}

	copied := db.CopyAccount(addr)
	copied.SetAllowance(777)
	if got := db.GetAllowance(addr); got != 100 {
		t.Fatalf("copied account should be detached from state: got %d, want 100", got)
	}
}

func TestStateDBWitnessVoteCount(t *testing.T) {
	db := newTestStateDB(t)
	wAddr := testAddr(0x10)
	db.PutWitness(wAddr, "http://w1.example.com")

	db.AddWitnessVoteCount(wAddr, 100)
	w := db.GetWitness(wAddr)
	if w.VoteCount() != 100 {
		t.Fatalf("vote count: got %d, want 100", w.VoteCount())
	}
	db.AddWitnessVoteCount(wAddr, -30)
	if w.VoteCount() != 70 {
		t.Fatalf("after sub: got %d, want 70", w.VoteCount())
	}
}

func TestStateDB_BandwidthMethods(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x01)
	statedb.GetOrCreateAccount(addr)

	// NetUsage
	if statedb.GetNetUsage(addr) != 0 {
		t.Fatal("initial NetUsage should be 0")
	}
	statedb.SetNetUsage(addr, 500)
	if statedb.GetNetUsage(addr) != 500 {
		t.Fatalf("NetUsage: want 500, got %d", statedb.GetNetUsage(addr))
	}

	// LatestConsumeTime
	statedb.SetLatestConsumeTime(addr, 3000)
	if statedb.GetLatestConsumeTime(addr) != 3000 {
		t.Fatalf("LatestConsumeTime: want 3000, got %d", statedb.GetLatestConsumeTime(addr))
	}

	// FreeNetUsage
	statedb.SetFreeNetUsage(addr, 200)
	if statedb.GetFreeNetUsage(addr) != 200 {
		t.Fatalf("FreeNetUsage: want 200, got %d", statedb.GetFreeNetUsage(addr))
	}

	// LatestConsumeFreeTime
	statedb.SetLatestConsumeFreeTime(addr, 6000)
	if statedb.GetLatestConsumeFreeTime(addr) != 6000 {
		t.Fatalf("LatestConsumeFreeTime: want 6000, got %d", statedb.GetLatestConsumeFreeTime(addr))
	}
}

func TestStateDB_BandwidthRevert(t *testing.T) {
	statedb := newTestStateDB(t)
	addr := testAddr(0x01)
	statedb.GetOrCreateAccount(addr)
	statedb.SetNetUsage(addr, 100)

	snap := statedb.Snapshot()
	statedb.SetNetUsage(addr, 999)
	if statedb.GetNetUsage(addr) != 999 {
		t.Fatalf("want 999 after set, got %d", statedb.GetNetUsage(addr))
	}

	statedb.RevertToSnapshot(snap)
	if statedb.GetNetUsage(addr) != 100 {
		t.Fatalf("want 100 after revert, got %d", statedb.GetNetUsage(addr))
	}
}

func TestContractStoragePersistsAcrossCommit(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}

	contract := testAddr(0x42)
	sdb.CreateAccount(contract, corepb.AccountType_Normal)

	var slot, value tcommon.Hash
	slot[31] = 0x00
	value[31] = 42

	sdb.SetState(contract, slot, value)

	root, err := sdb.Commit()
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Re-open state from committed root (simulates TriggerConstantContract)
	sdb2, err := New(root, db)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}

	got := sdb2.GetState(contract, slot)
	if got != value {
		t.Fatalf("storage after reopen: got %x, want %x", got, value)
	}
}

func TestStateDBIgnoresStaleFlatStorageMirror(t *testing.T) {
	diskdb := ethrawdb.NewMemoryDatabase()
	db := NewDatabase(diskdb)
	addr := testAddr(0x44)
	key := tcommon.BytesToHash([]byte{0x01})
	stale := tcommon.BytesToHash([]byte{0x99})

	sdb, err := New(tcommon.Hash(ethtypes.EmptyRootHash), db)
	if err != nil {
		t.Fatal(err)
	}
	sdb.GetOrCreateAccount(addr)
	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}

	rawdb.WriteStorage(diskdb, addr, javaStorageRowKey(addr, key, nil), stale.Bytes())
	sdb, err = New(root, db)
	if err != nil {
		t.Fatal(err)
	}

	if got := sdb.GetState(addr, key); got != (tcommon.Hash{}) {
		t.Fatalf("rooted storage must ignore stale flat mirror: got %x", got)
	}
}

// ---- M11.5: ApplyDefaultAccountPermissions --------------------------------

func TestApplyDefaultAccountPermissions_PopulatesBoth(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(7)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.ApplyDefaultAccountPermissions(addr, dp)

	acct := sdb.GetAccount(addr)
	if acct == nil {
		t.Fatal("account missing after create")
	}
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission is nil")
	}
	if owner.Type != corepb.Permission_Owner || owner.Id != 0 || owner.PermissionName != "owner" {
		t.Errorf("owner shape mismatch: type=%v id=%d name=%q", owner.Type, owner.Id, owner.PermissionName)
	}
	if len(owner.Keys) != 1 || string(owner.Keys[0].Address) != string(addr.Bytes()) {
		t.Errorf("owner key mismatch")
	}
	if len(owner.Operations) != 0 {
		t.Errorf("owner.Operations: want empty, got %d bytes", len(owner.Operations))
	}

	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry, got %d", len(actives))
	}
	active := actives[0]
	if active.Type != corepb.Permission_Active || active.Id != 2 || active.PermissionName != "active" {
		t.Errorf("active shape mismatch: type=%v id=%d name=%q", active.Type, active.Id, active.PermissionName)
	}
	want := dp.ActiveDefaultOperations()
	if len(active.Operations) != len(want) {
		t.Fatalf("active.Operations length: want %d, got %d", len(want), len(active.Operations))
	}
	for i := range want {
		if active.Operations[i] != want[i] {
			t.Errorf("active.Operations[%d]: want %#x, got %#x", i, want[i], active.Operations[i])
		}
	}
}

func TestApplyDefaultAccountPermissions_NoOpIfMissing(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(8)

	// Account does not exist; helper must not panic and must not create the account.
	sdb.ApplyDefaultAccountPermissions(addr, dp)

	if sdb.AccountExists(addr) {
		t.Fatal("ApplyDefaultAccountPermissions created an account; expected no-op")
	}
}

// ---- M11.5 slice 2a: ApplyWitnessPermissions ------------------------------

func TestApplyWitnessPermissions_NoOpIfMissing(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(9)

	// Account does not exist; helper must not panic and must not create the account.
	sdb.ApplyWitnessPermissions(addr, dp)

	if sdb.AccountExists(addr) {
		t.Fatal("ApplyWitnessPermissions created an account; expected no-op")
	}
}

func TestApplyWitnessPermissions_PopulatesAllOnEmptyAccount(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(10)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)
	sdb.ApplyWitnessPermissions(addr, dp)

	acct := sdb.GetAccount(addr)
	if acct == nil {
		t.Fatal("account missing after create")
	}
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission is nil; want default owner shape")
	}
	if owner.Type != corepb.Permission_Owner || owner.Id != 0 || owner.PermissionName != "owner" || owner.Threshold != 1 {
		t.Errorf("owner shape mismatch: type=%v id=%d name=%q threshold=%d", owner.Type, owner.Id, owner.PermissionName, owner.Threshold)
	}
	if len(owner.Keys) != 1 || string(owner.Keys[0].Address) != string(addr.Bytes()) || owner.Keys[0].Weight != 1 {
		t.Errorf("owner key mismatch")
	}

	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry, got %d", len(actives))
	}
	active := actives[0]
	if active.Type != corepb.Permission_Active || active.Id != 2 || active.PermissionName != "active" || active.Threshold != 1 {
		t.Errorf("active shape mismatch: type=%v id=%d name=%q threshold=%d", active.Type, active.Id, active.PermissionName, active.Threshold)
	}
	want := dp.ActiveDefaultOperations()
	if len(active.Operations) != len(want) {
		t.Fatalf("active.Operations length: want %d, got %d", len(want), len(active.Operations))
	}
	for i := range want {
		if active.Operations[i] != want[i] {
			t.Errorf("active.Operations[%d]: want %#x, got %#x", i, want[i], active.Operations[i])
		}
	}

	witness := acct.WitnessPermission()
	if witness == nil {
		t.Fatal("WitnessPermission is nil; want default witness shape")
	}
	if witness.Type != corepb.Permission_Witness || witness.Id != 1 || witness.PermissionName != "witness" || witness.Threshold != 1 {
		t.Errorf("witness shape mismatch: type=%v id=%d name=%q threshold=%d", witness.Type, witness.Id, witness.PermissionName, witness.Threshold)
	}
	if len(witness.Keys) != 1 || string(witness.Keys[0].Address) != string(addr.Bytes()) || witness.Keys[0].Weight != 1 {
		t.Errorf("witness key mismatch")
	}
	if len(witness.Operations) != 0 {
		t.Errorf("witness.Operations: want empty, got %d bytes", len(witness.Operations))
	}
}

func TestApplyWitnessPermissions_PreservesCustomOwner(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(11)
	co1 := testAddr(101)
	co2 := testAddr(102)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)

	// Install a custom 2-of-2-ish Owner via SetPermissions (Active untouched).
	customOwner := &corepb.Permission{
		Type:           corepb.Permission_Owner,
		Id:             0,
		PermissionName: "owner-custom",
		Threshold:      2,
		ParentId:       0,
		Keys: []*corepb.Key{
			{Address: co1.Bytes(), Weight: 1},
			{Address: co2.Bytes(), Weight: 1},
		},
	}
	sdb.SetPermissions(addr, customOwner, nil, nil)

	sdb.ApplyWitnessPermissions(addr, dp)

	acct := sdb.GetAccount(addr)
	if acct == nil {
		t.Fatal("account missing")
	}
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission cleared; want preserved custom")
	}
	if owner.PermissionName != "owner-custom" || owner.Threshold != 2 || len(owner.Keys) != 2 {
		t.Errorf("custom owner not preserved: name=%q threshold=%d keys=%d", owner.PermissionName, owner.Threshold, len(owner.Keys))
	}
	if string(owner.Keys[0].Address) != string(co1.Bytes()) || string(owner.Keys[1].Address) != string(co2.Bytes()) {
		t.Errorf("custom owner key addresses changed")
	}

	// Active was empty -> default Active[0] should now be installed.
	actives := acct.ActivePermission()
	if len(actives) != 1 {
		t.Fatalf("ActivePermission: want 1 entry installed (was empty), got %d", len(actives))
	}
	if actives[0].PermissionName != "active" || actives[0].Threshold != 1 {
		t.Errorf("active not the default shape after install")
	}

	// Witness is always set.
	witness := acct.WitnessPermission()
	if witness == nil || witness.PermissionName != "witness" {
		t.Fatal("WitnessPermission missing or wrong shape")
	}
}

func TestApplyWitnessPermissions_PreservesCustomActives(t *testing.T) {
	sdb := newTestStateDB(t)
	dp := NewDynamicProperties()
	addr := testAddr(12)
	signer1 := testAddr(201)
	signer2 := testAddr(202)

	sdb.CreateAccount(addr, corepb.AccountType_Normal)

	// Install two custom Active permissions; leave Owner nil.
	customA0 := &corepb.Permission{
		Type:           corepb.Permission_Active,
		Id:             2,
		PermissionName: "active-a",
		Threshold:      1,
		ParentId:       0,
		Operations:     []byte{0x01, 0x02},
		Keys:           []*corepb.Key{{Address: signer1.Bytes(), Weight: 1}},
	}
	customA1 := &corepb.Permission{
		Type:           corepb.Permission_Active,
		Id:             3,
		PermissionName: "active-b",
		Threshold:      1,
		ParentId:       0,
		Operations:     []byte{0x03, 0x04},
		Keys:           []*corepb.Key{{Address: signer2.Bytes(), Weight: 1}},
	}
	sdb.SetPermissions(addr, nil, nil, []*corepb.Permission{customA0, customA1})

	sdb.ApplyWitnessPermissions(addr, dp)

	acct := sdb.GetAccount(addr)
	if acct == nil {
		t.Fatal("account missing")
	}

	// Owner was nil -> default Owner should now be installed.
	owner := acct.OwnerPermission()
	if owner == nil {
		t.Fatal("OwnerPermission missing; expected default install")
	}
	if owner.PermissionName != "owner" {
		t.Errorf("Owner not default shape: name=%q", owner.PermissionName)
	}

	// Active list (non-empty) preserved verbatim.
	actives := acct.ActivePermission()
	if len(actives) != 2 {
		t.Fatalf("ActivePermission: want 2 entries (preserved), got %d", len(actives))
	}
	if actives[0].PermissionName != "active-a" || actives[1].PermissionName != "active-b" {
		t.Errorf("custom active list mutated: %q, %q", actives[0].PermissionName, actives[1].PermissionName)
	}

	// Witness is always set.
	witness := acct.WitnessPermission()
	if witness == nil || witness.PermissionName != "witness" {
		t.Fatal("WitnessPermission missing or wrong shape")
	}
}

// M11.5 slice 2b: CreateAccountWithTime mirrors java-tron's AccountCapsule
// 5-arg constructor (AccountCapsule.java:158-180), stamping create_time on
// the new account. Bare CreateAccount leaves it at 0 (used by VM-internal
// paths and genesis).
func TestCreateAccount_CreateTimeFromDynProps(t *testing.T) {
	sdb := newTestStateDB(t)

	// 2-arg form: create_time stays 0.
	addrZero := testAddr(0xa1)
	sdb.CreateAccount(addrZero, corepb.AccountType_Normal)
	if got := sdb.GetAccount(addrZero).CreateTime(); got != 0 {
		t.Errorf("CreateAccount: want create_time=0, got %d", got)
	}

	// 3-arg form: stamps the supplied timestamp (mirrors actuator's
	// dp.LatestBlockHeaderTimestamp() argument).
	const ts = int64(1_700_000_000_321)
	addrStamped := testAddr(0xa2)
	sdb.CreateAccountWithTime(addrStamped, corepb.AccountType_Normal, ts)
	if got := sdb.GetAccount(addrStamped).CreateTime(); got != ts {
		t.Errorf("CreateAccountWithTime: want create_time=%d, got %d", ts, got)
	}

	// Distinct timestamps must be wired through verbatim (no truncation /
	// override / fallback to zero). Use the `dp` accessor explicitly to mirror
	// the actuator call shape.
	dp := NewDynamicProperties()
	dp.SetLatestBlockHeaderTimestamp(ts + 999)
	addrFromDP := testAddr(0xa3)
	sdb.CreateAccountWithTime(addrFromDP, corepb.AccountType_Normal, dp.LatestBlockHeaderTimestamp())
	if got := sdb.GetAccount(addrFromDP).CreateTime(); got != ts+999 {
		t.Errorf("CreateAccountWithTime via DP: want %d, got %d", ts+999, got)
	}
}
