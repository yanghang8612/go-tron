package dpos

import (
	"testing"

	"github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// makeCheatBlock builds a minimal *types.Block at a given height with a
// chosen witness and a parent hash byte that differentiates the block hash
// (since Block.Hash is sha256 over BlockHeader.RawData, varying ParentHash
// uniquely varies the resulting hash).
func makeCheatBlock(num uint64, producer common.Address, parentTag byte) *types.Block {
	parent := make([]byte, 32)
	parent[0] = parentTag
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:         int64(num),
				Timestamp:      int64(num) * 3000,
				WitnessAddress: producer.Bytes(),
				ParentHash:     parent,
			},
		},
	})
}

func cheatAddr(b byte) common.Address {
	var a common.Address
	a[0] = 0x41
	a[20] = b
	return a
}

// TestCheatDetection_DoubleSign_Recorded mirrors java-tron's
// `validWitnessProductTwoBlockTest` (framework/src/test/java/org/tron/core/services/WitnessProductBlockServiceTest.java:60-101):
// produce two distinct blocks at the same height with the same witness, and
// assert exactly one cheat entry is recorded with both block hashes.
func TestCheatDetection_DoubleSign_Recorded(t *testing.T) {
	d := NewCheatDetector()
	d.nowMillis = func() int64 { return 1717000000000 }
	witness := cheatAddr(0x01)

	b1 := makeCheatBlock(1, witness, 0xaa)
	d.CheckBlock(b1)
	if got := len(d.QueryCheatWitnessInfo()); got != 0 {
		t.Fatalf("after first block: cheat map size = %d, want 0", got)
	}

	b2 := makeCheatBlock(1, witness, 0xbb)
	if b1.Hash() == b2.Hash() {
		t.Fatal("test setup error: b1 and b2 must have different hashes")
	}
	d.CheckBlock(b2)

	info := d.QueryCheatWitnessInfo()
	if len(info) != 1 {
		t.Fatalf("after double-sign: cheat map size = %d, want 1", len(info))
	}
	key := witnessHexKey(witness)
	entry, ok := info[key]
	if !ok {
		t.Fatalf("cheat map missing key %s; got keys %v", key, mapKeys(info))
	}
	if entry.Times != 1 {
		t.Errorf("Times = %d, want 1", entry.Times)
	}
	if entry.LatestBlockNum != 1 {
		t.Errorf("LatestBlockNum = %d, want 1", entry.LatestBlockNum)
	}
	if entry.Time != 1717000000000 {
		t.Errorf("Time = %d, want 1717000000000", entry.Time)
	}
	if len(entry.BlockSet) != 2 {
		t.Fatalf("BlockSet size = %d, want 2", len(entry.BlockSet))
	}
	want := map[common.Hash]bool{b1.Hash(): true, b2.Hash(): true}
	for _, h := range entry.BlockSet {
		if !want[h] {
			t.Errorf("unexpected hash in BlockSet: %x", h)
		}
		delete(want, h)
	}
	if len(want) != 0 {
		t.Errorf("missing hashes from BlockSet: %v", want)
	}
	if got := d.CheatEventCount(); got != 1 {
		t.Errorf("CheatEventCount = %d, want 1", got)
	}
}

// TestCheatDetection_NormalProduction_NotRecorded covers the non-cheat cases
// that should leave the cheat map empty: same height re-feed of the same
// block, and different heights for the same witness.
func TestCheatDetection_NormalProduction_NotRecorded(t *testing.T) {
	d := NewCheatDetector()
	witness := cheatAddr(0x02)

	// Same block twice → no cheat (hashes are equal).
	b := makeCheatBlock(5, witness, 0x11)
	d.CheckBlock(b)
	d.CheckBlock(b)

	// Different heights, same witness → no cheat.
	for n := uint64(6); n <= 10; n++ {
		d.CheckBlock(makeCheatBlock(n, witness, byte(n)))
	}

	if got := len(d.QueryCheatWitnessInfo()); got != 0 {
		t.Fatalf("cheat map size = %d, want 0", got)
	}
	if got := d.CheatEventCount(); got != 0 {
		t.Fatalf("CheatEventCount = %d, want 0", got)
	}
	if got := d.HistoryLen(); got != 6 {
		t.Errorf("HistoryLen = %d, want 6 (blocks 5-10)", got)
	}
}

// TestCheatDetection_OldEntriesEvicted asserts that the history cache is
// bounded at HistoryBlockCacheSize (200, matching java-tron's
// `CacheBuilder.maximumSize(200)`) and that the oldest insertion is evicted
// once the bound is exceeded. After eviction, a re-insert at the evicted
// height with a different hash should NOT be flagged as a cheat (because the
// original entry was forgotten) — instead it just rebuilds the cache slot.
func TestCheatDetection_OldEntriesEvicted(t *testing.T) {
	if HistoryBlockCacheSize != 200 {
		t.Fatalf("HistoryBlockCacheSize = %d, want 200 (java-tron parity)", HistoryBlockCacheSize)
	}
	d := NewCheatDetector()
	witness := cheatAddr(0x03)

	// Fill the cache to capacity.
	for n := uint64(1); n <= HistoryBlockCacheSize; n++ {
		d.CheckBlock(makeCheatBlock(n, witness, byte(n)))
	}
	if got := d.HistoryLen(); got != HistoryBlockCacheSize {
		t.Fatalf("HistoryLen after fill = %d, want %d", got, HistoryBlockCacheSize)
	}
	if !d.HasHistoryAt(1) {
		t.Fatal("oldest entry (num=1) should be present before eviction")
	}

	// One more insert: the oldest (num=1) must be evicted.
	d.CheckBlock(makeCheatBlock(HistoryBlockCacheSize+1, witness, 0xff))
	if got := d.HistoryLen(); got != HistoryBlockCacheSize {
		t.Errorf("HistoryLen stayed = %d, want %d", got, HistoryBlockCacheSize)
	}
	if d.HasHistoryAt(1) {
		t.Error("oldest entry (num=1) should have been evicted")
	}
	if !d.HasHistoryAt(uint64(HistoryBlockCacheSize + 1)) {
		t.Error("newest entry not present after insert")
	}

	// Re-insert at the evicted height with a different hash: not a cheat
	// because the original entry is gone — just refills the slot.
	d.CheckBlock(makeCheatBlock(1, witness, 0x77))
	if got := len(d.QueryCheatWitnessInfo()); got != 0 {
		t.Errorf("post-eviction reinsert: cheat map = %d, want 0", got)
	}
	if !d.HasHistoryAt(1) {
		t.Error("re-inserted entry not in cache")
	}
}

// TestCheatDetection_DifferentWitnessSameHeight covers the case where two
// different witnesses produce blocks at the same height (e.g. fork from
// different branches gossiped in any order). java-tron only flags a cheat
// when the witness address matches; a different witness leaves cache state
// alone.
func TestCheatDetection_DifferentWitnessSameHeight(t *testing.T) {
	d := NewCheatDetector()
	w1 := cheatAddr(0x04)
	w2 := cheatAddr(0x05)

	d.CheckBlock(makeCheatBlock(42, w1, 0x01))
	d.CheckBlock(makeCheatBlock(42, w2, 0x02))

	if got := len(d.QueryCheatWitnessInfo()); got != 0 {
		t.Errorf("cheat map = %d, want 0 (different witnesses)", got)
	}
	if got := d.CheatEventCount(); got != 0 {
		t.Errorf("CheatEventCount = %d, want 0", got)
	}
}

// TestCheatDetection_RepeatedDoubleSign_Increments asserts that a second
// cheat event at a different height bumps Times. Mirrors java-tron's
// `CheatWitnessInfo.increment()` accumulation.
func TestCheatDetection_RepeatedDoubleSign_Increments(t *testing.T) {
	d := NewCheatDetector()
	witness := cheatAddr(0x06)

	d.CheckBlock(makeCheatBlock(10, witness, 0x01))
	d.CheckBlock(makeCheatBlock(10, witness, 0x02)) // event 1

	d.CheckBlock(makeCheatBlock(20, witness, 0x03))
	d.CheckBlock(makeCheatBlock(20, witness, 0x04)) // event 2

	info := d.QueryCheatWitnessInfo()
	entry := info[witnessHexKey(witness)]
	if entry == nil {
		t.Fatal("missing cheat entry")
	}
	if entry.Times != 2 {
		t.Errorf("Times = %d, want 2", entry.Times)
	}
	if entry.LatestBlockNum != 20 {
		t.Errorf("LatestBlockNum = %d, want 20", entry.LatestBlockNum)
	}
	// BlockSet is rebuilt on every event (mirror java-tron `clear()`):
	// it should contain only the two blocks of the latest event.
	if len(entry.BlockSet) != 2 {
		t.Fatalf("BlockSet size = %d, want 2 (post-clear)", len(entry.BlockSet))
	}
	if got := d.CheatEventCount(); got != 2 {
		t.Errorf("CheatEventCount = %d, want 2", got)
	}
}

// TestCheatDetection_NilBlock_Ignored asserts that a nil block is silently
// skipped, matching java-tron's try/catch swallow.
func TestCheatDetection_NilBlock_Ignored(t *testing.T) {
	d := NewCheatDetector()
	d.CheckBlock(nil) // must not panic
	if got := d.HistoryLen(); got != 0 {
		t.Errorf("HistoryLen after nil = %d, want 0", got)
	}
}

// TestCheatDetection_ReorgDoesNotCorruptCache covers the parent task's
// "reorg-safe" concern. Witness cheat detection touches no persistent state,
// so a switchFork at the chain layer is invisible to it. We simulate a
// "branch swap" by checking blocks from two divergent histories at the same
// heights and asserting (a) detection still flags double-signs accurately,
// (b) no panic, (c) no consensus state was needed to operate. In particular
// the cheat info recorded on the first branch survives the swap because the
// detector is monitoring-only — there's nothing to roll back.
func TestCheatDetection_ReorgDoesNotCorruptCache(t *testing.T) {
	d := NewCheatDetector()
	witness := cheatAddr(0x07)

	// Branch A: blocks 100-103 by `witness`.
	for n := uint64(100); n <= 103; n++ {
		d.CheckBlock(makeCheatBlock(n, witness, byte(n)))
	}
	if got := len(d.QueryCheatWitnessInfo()); got != 0 {
		t.Fatalf("after branch A: cheat map = %d, want 0", got)
	}

	// Branch B reorg replays heights 102-103 with different parent hashes,
	// same witness — this IS the on-wire shape of a double-sign and must
	// be recorded as such, regardless of whether the chain layer keeps or
	// rewinds the original blocks.
	d.CheckBlock(makeCheatBlock(102, witness, 0xee))
	d.CheckBlock(makeCheatBlock(103, witness, 0xff))

	info := d.QueryCheatWitnessInfo()
	if len(info) != 1 {
		t.Fatalf("after branch B replay: cheat map = %d, want 1", len(info))
	}
	entry := info[witnessHexKey(witness)]
	if entry.Times != 2 {
		t.Errorf("Times = %d, want 2 (one event per replayed height)", entry.Times)
	}
	if entry.LatestBlockNum != 103 {
		t.Errorf("LatestBlockNum = %d, want 103", entry.LatestBlockNum)
	}
}

// TestCheatDetection_ConcurrentCheckBlock asserts that CheckBlock is safe to
// call from multiple goroutines concurrently. Java-tron's service is invoked
// from BlockMsgHandler on the netty event loop; go-tron's TronHandler may
// dispatch handleBlock from multiple peer goroutines.
func TestCheatDetection_ConcurrentCheckBlock(t *testing.T) {
	d := NewCheatDetector()
	witness := cheatAddr(0x08)

	const goroutines = 8
	const blocksPer = 50

	done := make(chan struct{}, goroutines)
	for g := 0; g < goroutines; g++ {
		g := g
		go func() {
			defer func() { done <- struct{}{} }()
			for i := 0; i < blocksPer; i++ {
				num := uint64(g*blocksPer + i + 1)
				d.CheckBlock(makeCheatBlock(num, witness, byte(num)))
			}
		}()
	}
	for i := 0; i < goroutines; i++ {
		<-done
	}

	if got := d.HistoryLen(); got > HistoryBlockCacheSize {
		t.Errorf("HistoryLen = %d, exceeds bound %d", got, HistoryBlockCacheSize)
	}
	if got := len(d.QueryCheatWitnessInfo()); got != 0 {
		t.Errorf("cheat map = %d, want 0 (no double-signs in workload)", got)
	}
}

func mapKeys(m map[string]*CheatWitnessInfo) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
