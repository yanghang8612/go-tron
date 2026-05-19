package historyprune

import (
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/ethdb/memorydb"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/rawdb"
	historypb "github.com/tronprotocol/go-tron/proto/core/historystate"
)

// fakeChain is the test stub for ChainSource — a memorydb-backed KV
// store plus an injectable solidified-block number. The pruner only
// reads `solidified` (a uint64-like in-test counter); production code
// uses *core.BlockChain whose LatestSolidifiedBlockNum derives from
// dynamic properties.
type fakeChain struct {
	db         *memorydb.Database
	solidified int64
}

func newFakeChain() *fakeChain {
	return &fakeChain{db: memorydb.New()}
}

func (f *fakeChain) DB() ethdb.KeyValueStore        { return f.db }
func (f *fakeChain) LatestSolidifiedBlockNum() int64 { return f.solidified }

// plantBlocks populates per-block sh-* rows for every block in [lo, hi]
// plus inverse-index rows for the given addresses/slots. Mirrors what
// AccumulateHistory would write at apply-block time, just enough to give
// the pruner something to delete.
func plantBlocks(t *testing.T, fc *fakeChain, lo, hi uint64, addrs []tcommon.Address, slots []tcommon.Hash) {
	t.Helper()
	for n := lo; n <= hi; n++ {
		if err := rawdb.WriteHistoryMeta(fc.db, n, &historypb.StateHistoryMeta{
			BlockNum:  n,
			SchemaVer: rawdb.HistorySchemaVersion,
			NumAddrs:  uint32(len(addrs)),
			NumSlots:  uint32(len(addrs) * len(slots)),
		}); err != nil {
			t.Fatalf("WriteHistoryMeta(%d): %v", n, err)
		}
		for _, a := range addrs {
			if err := rawdb.WriteAccountDelta(fc.db, n, a, &historypb.AccountDelta{ExistedPre: true}); err != nil {
				t.Fatalf("WriteAccountDelta(%d, %x): %v", n, a[:4], err)
			}
			if err := rawdb.WriteAddrInverse(fc.db, a, n); err != nil {
				t.Fatalf("WriteAddrInverse(%d, %x): %v", n, a[:4], err)
			}
			for _, s := range slots {
				if err := rawdb.WriteSlotDelta(fc.db, n, a, s, tcommon.Hash{}); err != nil {
					t.Fatalf("WriteSlotDelta(%d, %x, %x): %v", n, a[:4], s[:4], err)
				}
				if err := rawdb.WriteSlotInverse(fc.db, a, s, n); err != nil {
					t.Fatalf("WriteSlotInverse(%d, %x, %x): %v", n, a[:4], s[:4], err)
				}
			}
		}
	}
}

func mkAddr(b byte) tcommon.Address {
	var a tcommon.Address
	a[0] = 0x41
	a[20] = b
	return a
}

func mkSlot(b byte) tcommon.Hash {
	var h tcommon.Hash
	h[31] = b
	return h
}

// TestPrunePass_DeletesBelowCutoff plants 1000 blocks' worth of rows,
// sets window=100, drives one pass, and asserts everything below
// (solidified - window) is gone while rows within the retention window
// survive intact. Locks in the slice-5 acceptance: "prune_window blocks
// old, range-delete sh-m- / sh-a- / sh-s- blocks below cutoff".
func TestPrunePass_DeletesBelowCutoff(t *testing.T) {
	fc := newFakeChain()
	fc.solidified = 1000
	addrA, addrB := mkAddr(0xAA), mkAddr(0xBB)
	slot1, slot2 := mkSlot(0x01), mkSlot(0x02)
	plantBlocks(t, fc, 1, 1000, []tcommon.Address{addrA, addrB}, []tcommon.Hash{slot1, slot2})

	p := New(fc, PrunerConfig{
		Window:    100,
		Interval:  time.Hour, // loop is not started; just satisfies applyDefaults
		BatchSize: 5000,      // big enough to cover the whole [1, 899] range in one pass
	})
	if err := p.PrunePass(); err != nil {
		t.Fatalf("PrunePass: %v", err)
	}

	// cutoff = solidified - window = 900. Rows for blocks < 900 are gone.
	const cutoff uint64 = 900
	for n := uint64(1); n < 1000; n++ {
		present := !(n < cutoff)
		if got := rawdb.HasHistoryMeta(fc.db, n); got != present {
			t.Errorf("block=%d sh-m- present=%v want %v", n, got, present)
		}
		if got := rawdb.HasAccountDelta(fc.db, n, addrA); got != present {
			t.Errorf("block=%d sh-a-(A) present=%v want %v", n, got, present)
		}
		if got := rawdb.HasSlotDelta(fc.db, n, addrA, slot1); got != present {
			t.Errorf("block=%d sh-s-(A,slot1) present=%v want %v", n, got, present)
		}
	}

	// Inverse index for pruned blocks must also be gone — the pass runs
	// the inverse sweep on the first pass (lastInverseAt == 0).
	for n := uint64(1); n < cutoff; n++ {
		if rawdb.HasAddrInverse(fc.db, addrA, n) {
			t.Errorf("sh-i-a- block=%d addr=A still present after prune", n)
		}
		if rawdb.HasSlotInverse(fc.db, addrA, slot1, n) {
			t.Errorf("sh-i-s- block=%d addr=A slot1 still present after prune", n)
		}
	}
	for n := cutoff; n <= 1000; n++ {
		if !rawdb.HasAddrInverse(fc.db, addrA, n) {
			t.Errorf("sh-i-a- block=%d addr=A incorrectly pruned", n)
		}
	}

	// HistoryConfig.FirstBlock must point past the last-pruned cursor.
	cfg, err := rawdb.ReadHistoryConfig(fc.db)
	if err != nil {
		t.Fatalf("ReadHistoryConfig: %v", err)
	}
	if cfg.FirstBlock != cutoff {
		t.Errorf("HistoryConfig.FirstBlock=%d, want %d", cfg.FirstBlock, cutoff)
	}
}

// TestPrunePass_IdempotentResume asserts that a second consecutive pass
// at the same head is a no-op. The pruner must NOT re-scan or re-delete
// rows it already removed in pass #1.
func TestPrunePass_IdempotentResume(t *testing.T) {
	fc := newFakeChain()
	fc.solidified = 500
	addrA := mkAddr(0xAA)
	plantBlocks(t, fc, 1, 500, []tcommon.Address{addrA}, []tcommon.Hash{mkSlot(0x01)})

	p := New(fc, PrunerConfig{Window: 100, BatchSize: 1000})
	if err := p.PrunePass(); err != nil {
		t.Fatalf("PrunePass #1: %v", err)
	}
	firstStats := p.Stats()
	if firstStats.BlocksPruned == 0 {
		t.Fatal("PrunePass #1 didn't prune anything")
	}

	if err := p.PrunePass(); err != nil {
		t.Fatalf("PrunePass #2: %v", err)
	}
	secondStats := p.Stats()
	if secondStats.BlocksPruned != firstStats.BlocksPruned {
		t.Errorf("PrunePass #2 incremented BlocksPruned: %d -> %d (expected no change)",
			firstStats.BlocksPruned, secondStats.BlocksPruned)
	}
}

// TestPrunePass_BatchSizeBound asserts that a pass with batchSize=N
// prunes at most N blocks even when far more are eligible. The leftover
// must be available for the next pass.
func TestPrunePass_BatchSizeBound(t *testing.T) {
	fc := newFakeChain()
	fc.solidified = 1000
	addrA := mkAddr(0xAA)
	plantBlocks(t, fc, 1, 1000, []tcommon.Address{addrA}, []tcommon.Hash{mkSlot(0x01)})

	p := New(fc, PrunerConfig{Window: 10, BatchSize: 50})
	if err := p.PrunePass(); err != nil {
		t.Fatalf("PrunePass: %v", err)
	}

	// Eligible range is [1, solidified-window-1] = [1, 989], i.e. 989
	// blocks. BatchSize=50 means exactly 50 blocks pruned this pass.
	stats := p.Stats()
	if stats.BlocksPruned != 50 {
		t.Errorf("BlocksPruned=%d, want 50", stats.BlocksPruned)
	}
	if stats.LastPrunedBlock != 50 {
		t.Errorf("LastPrunedBlock=%d, want 50", stats.LastPrunedBlock)
	}
	// Block 50 is the highest one pruned this pass; block 51 must still
	// be present.
	if rawdb.HasHistoryMeta(fc.db, 50) {
		t.Error("block 50 should be pruned")
	}
	if !rawdb.HasHistoryMeta(fc.db, 51) {
		t.Error("block 51 should NOT be pruned (waiting for next pass)")
	}
}

// TestPrunePass_ArchiveModeNoOp asserts that constructing a Pruner with
// Window=0 (archive mode equivalent) skips all deletion logic. Stats
// stay at zero and rows survive.
func TestPrunePass_ArchiveModeNoOp(t *testing.T) {
	fc := newFakeChain()
	fc.solidified = 500
	addrA := mkAddr(0xAA)
	plantBlocks(t, fc, 1, 500, []tcommon.Address{addrA}, []tcommon.Hash{mkSlot(0x01)})

	p := New(fc, PrunerConfig{Window: 0})
	// Start with Window=0 short-circuits the loop and closes done. We
	// don't call PrunePass directly here — the contract is that Start
	// must not delete anything in archive mode.
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	for n := uint64(1); n <= 500; n++ {
		if !rawdb.HasHistoryMeta(fc.db, n) {
			t.Errorf("archive mode pruned block %d (must not)", n)
			break
		}
	}
}

// TestPrunePass_PreservesInverseIndexForRecentBlocks asserts the inverse
// sweep only deletes rows whose embedded blockNum is below the cutoff.
// Rows inside the retention window survive even when the same addr was
// also modified in pruned blocks.
func TestPrunePass_PreservesInverseIndexForRecentBlocks(t *testing.T) {
	fc := newFakeChain()
	fc.solidified = 1000
	addrA := mkAddr(0xAA)
	slot1 := mkSlot(0x01)
	plantBlocks(t, fc, 1, 1000, []tcommon.Address{addrA}, []tcommon.Hash{slot1})

	p := New(fc, PrunerConfig{Window: 100, BatchSize: 5000, InverseBatchSize: 10_000})
	if err := p.PrunePass(); err != nil {
		t.Fatalf("PrunePass: %v", err)
	}

	const cutoff uint64 = 900
	for n := cutoff; n <= 1000; n++ {
		if !rawdb.HasAddrInverse(fc.db, addrA, n) {
			t.Errorf("sh-i-a- block=%d incorrectly pruned (within retention window)", n)
		}
		if !rawdb.HasSlotInverse(fc.db, addrA, slot1, n) {
			t.Errorf("sh-i-s- block=%d incorrectly pruned (within retention window)", n)
		}
	}
}

// TestPrunePass_NoSolidifiedNoOp asserts that a chain that hasn't
// produced enough blocks yet (solidified <= window) is a no-op rather
// than an underflow. Defensive check against the int64 - uint64 boundary.
func TestPrunePass_NoSolidifiedNoOp(t *testing.T) {
	fc := newFakeChain()
	fc.solidified = 50
	addrA := mkAddr(0xAA)
	plantBlocks(t, fc, 1, 50, []tcommon.Address{addrA}, nil)

	p := New(fc, PrunerConfig{Window: 100, BatchSize: 5000})
	if err := p.PrunePass(); err != nil {
		t.Fatalf("PrunePass: %v", err)
	}
	stats := p.Stats()
	if stats.BlocksPruned != 0 {
		t.Errorf("BlocksPruned=%d, want 0 (solidified<window)", stats.BlocksPruned)
	}
	// Every planted row must survive.
	for n := uint64(1); n <= 50; n++ {
		if !rawdb.HasHistoryMeta(fc.db, n) {
			t.Errorf("block %d pruned despite solidified<=window", n)
		}
	}
}

// TestPrunePass_ResumesFromPersistedCursor asserts the Start path picks
// up FirstBlock from disk so a restart doesn't re-scan blocks that were
// already pruned. Plant rows for [1..1000], simulate a prior partial
// prune by seeding FirstBlock=400, then run one pass and confirm only
// [400, cutoff) gets deleted in this run.
func TestPrunePass_ResumesFromPersistedCursor(t *testing.T) {
	fc := newFakeChain()
	fc.solidified = 1000
	addrA := mkAddr(0xAA)
	plantBlocks(t, fc, 1, 1000, []tcommon.Address{addrA}, []tcommon.Hash{mkSlot(0x01)})

	// Simulate a prior prune by seeding HistoryConfig.FirstBlock=400
	// AND deleting the rows for blocks 1..399 directly.
	if err := rawdb.PruneHistoryBlockRange(fc.db, 1, 399); err != nil {
		t.Fatalf("seed prune: %v", err)
	}
	if err := rawdb.WriteHistoryConfig(fc.db, &historypb.HistoryConfig{
		Mode:        0,
		PruneWindow: 100,
		FirstBlock:  400,
		SchemaVer:   rawdb.HistorySchemaVersion,
	}); err != nil {
		t.Fatalf("seed config: %v", err)
	}

	// Build the pruner and prime its cursor from the seeded
	// HistoryConfig WITHOUT launching the loop — that way one explicit
	// PrunePass is the only pass we count, and the test is race-free.
	p := New(fc, PrunerConfig{Window: 100, BatchSize: 5000})
	cfg, err := rawdb.ReadHistoryConfig(fc.db)
	if err != nil {
		t.Fatalf("read seeded config: %v", err)
	}
	p.setLastPrunedBlock(cfg.FirstBlock - 1)

	if err := p.PrunePass(); err != nil {
		t.Fatalf("PrunePass: %v", err)
	}
	stats := p.Stats()
	// Eligible: [400, 899]. That's 500 blocks. BatchSize=5000 ⇒ all in
	// one pass.
	if stats.BlocksPruned != 500 {
		t.Errorf("BlocksPruned=%d, want 500", stats.BlocksPruned)
	}

	// Spot-check: block 400 must be gone, block 900 must remain.
	if rawdb.HasHistoryMeta(fc.db, 400) {
		t.Error("block 400 should be pruned")
	}
	if !rawdb.HasHistoryMeta(fc.db, 900) {
		t.Error("block 900 should be preserved")
	}
}

// TestPruner_StartStopClean covers the Lifecycle contract: Start
// returns immediately, Stop blocks until the loop drains. With an
// extremely long Interval the only pass that runs is the initial-on-Start
// one — the test just asserts no goroutine leaks via the done channel.
func TestPruner_StartStopClean(t *testing.T) {
	fc := newFakeChain()
	fc.solidified = 500
	addrA := mkAddr(0xAA)
	plantBlocks(t, fc, 1, 500, []tcommon.Address{addrA}, nil)

	p := New(fc, PrunerConfig{Window: 100, BatchSize: 100, Interval: time.Hour})
	if err := p.Start(); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	// Second Stop is a no-op (sync.Once guards close(quit)).
	if err := p.Stop(); err != nil {
		t.Fatalf("Stop second call: %v", err)
	}
}
