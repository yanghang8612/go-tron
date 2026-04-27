package core

import (
	"fmt"
	"testing"

	tcommon "github.com/tronprotocol/go-tron/common"
	"github.com/tronprotocol/go-tron/core/types"
	corepb "github.com/tronprotocol/go-tron/proto/core"
)

// makeKhaosBlock builds a minimal Block with a given number and parentHash.
// Each unique (num, parentHash) pair gets a distinct hash because the RawData
// number field differs.
func makeKhaosTestBlock(num uint64, parentHash tcommon.Hash) *types.Block {
	return types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     int64(num),
				ParentHash: parentHash.Bytes(),
			},
		},
	})
}

// chainOfBlocks builds a linear chain of n blocks starting from genesis (num=0, parentHash=zero).
func chainOfBlocks(n int) []*types.Block {
	blocks := make([]*types.Block, n+1)
	blocks[0] = makeKhaosTestBlock(0, tcommon.Hash{})
	for i := 1; i <= n; i++ {
		blocks[i] = makeKhaosTestBlock(uint64(i), blocks[i-1].Hash())
	}
	return blocks
}

func TestKhaosDB_Push_Linear(t *testing.T) {
	k := NewKhaosDB()
	chain := chainOfBlocks(10)
	k.Start(chain[0])

	for i := 1; i <= 10; i++ {
		head, err := k.Push(chain[i])
		if err != nil {
			t.Fatalf("push block %d: %v", i, err)
		}
		if head.Number() != uint64(i) {
			t.Fatalf("head.Number() = %d, want %d", head.Number(), i)
		}
	}

	if k.Head().Number() != 10 {
		t.Fatalf("final head = %d, want 10", k.Head().Number())
	}
}

func TestKhaosDB_Push_Duplicate(t *testing.T) {
	k := NewKhaosDB()
	chain := chainOfBlocks(3)
	k.Start(chain[0])
	k.Push(chain[1])
	k.Push(chain[2])

	// Re-push block 1 — should succeed (idempotent insert doesn't error)
	// In practice the BlockChain deduplicates before calling Push, but KhaosDB
	// should not panic on a duplicate.
	_, err := k.Push(chain[1])
	if err != nil {
		t.Fatalf("re-push of existing block should not error: %v", err)
	}
}

func TestKhaosDB_Push_Unlinked_ThenPromoted(t *testing.T) {
	k := NewKhaosDB()
	chain := chainOfBlocks(5)
	k.Start(chain[0])
	k.Push(chain[1])

	// Push block 3 before block 2 → unlinked.
	_, err := k.Push(chain[3])
	if err != ErrUnlinkedBlock {
		t.Fatalf("want ErrUnlinkedBlock, got %v", err)
	}
	if k.ContainsInMiniStore(chain[3].Hash()) {
		t.Fatal("unlinked block should not be in miniStore")
	}
	if !k.ContainsBlock(chain[3].Hash()) {
		t.Fatal("unlinked block should be in miniUnlinkedStore")
	}

	// Now push block 2 → block 3 should be promoted automatically.
	_, err = k.Push(chain[2])
	if err != nil {
		t.Fatalf("push block 2: %v", err)
	}
	if !k.ContainsInMiniStore(chain[3].Hash()) {
		t.Fatal("block 3 should have been promoted to miniStore after block 2 arrived")
	}
	if k.Head().Number() != 3 {
		t.Fatalf("head should be 3, got %d", k.Head().Number())
	}
}

func TestKhaosDB_GetBranch_NoFork(t *testing.T) {
	k := NewKhaosDB()
	chain := chainOfBlocks(5)
	k.Start(chain[0])
	for i := 1; i <= 5; i++ {
		k.Push(chain[i])
	}

	// Same hash for both arguments → both branches empty.
	b1, b2, err := k.GetBranch(chain[5].Hash(), chain[5].Hash())
	if err != nil {
		t.Fatal(err)
	}
	if len(b1) != 0 || len(b2) != 0 {
		t.Fatalf("same hash: want empty branches, got %d/%d", len(b1), len(b2))
	}
}

func TestKhaosDB_GetBranch_10Block(t *testing.T) {
	// Common stem: blocks 0-5.
	// Fork A from block 5: blocks 6A, 7A, 8A.
	// Fork B from block 5: blocks 6B, 7B, 8B.
	k := NewKhaosDB()
	stem := chainOfBlocks(5)
	k.Start(stem[0])
	for i := 1; i <= 5; i++ {
		k.Push(stem[i])
	}

	// Build fork A (number 6-8, chained from stem[5]).
	forkA := make([]*types.Block, 4) // [0]=stem[5], [1..3]=A blocks
	forkA[0] = stem[5]
	for i := 1; i <= 3; i++ {
		forkA[i] = makeKhaosTestBlock(uint64(5+i), forkA[i-1].Hash())
		_, err := k.Push(forkA[i])
		if err != nil {
			t.Fatalf("push forkA[%d]: %v", i, err)
		}
	}

	// Build fork B (distinct blocks at same heights, also chained from stem[5]).
	forkB := make([]*types.Block, 4)
	forkB[0] = stem[5]
	for i := 1; i <= 3; i++ {
		// Use a different timestamp byte to get a distinct hash.
		forkB[i] = types.NewBlockFromPB(&corepb.Block{
			BlockHeader: &corepb.BlockHeader{
				RawData: &corepb.BlockHeaderRaw{
					Number:     int64(5 + i),
					ParentHash: forkB[i-1].Hash().Bytes(),
					Timestamp:  int64(i * 1000), // distinguishes from forkA
				},
			},
		})
		_, err := k.Push(forkB[i])
		if err != nil {
			t.Fatalf("push forkB[%d]: %v", i, err)
		}
	}

	// GetBranch(tipA, tipB) should return 3 blocks on each side (6A/7A/8A and 6B/7B/8B).
	b1, b2, err := k.GetBranch(forkA[3].Hash(), forkB[3].Hash())
	if err != nil {
		t.Fatalf("GetBranch: %v", err)
	}
	if len(b1) != 3 {
		t.Fatalf("branch A: want 3 blocks, got %d", len(b1))
	}
	if len(b2) != 3 {
		t.Fatalf("branch B: want 3 blocks, got %d", len(b2))
	}
	// LCA must be stem[5].
	lcaHash := b1[len(b1)-1].ParentHash()
	if lcaHash != stem[5].Hash() {
		t.Fatalf("LCA hash = %x, want stem[5] = %x", lcaHash, stem[5].Hash())
	}
}

func TestKhaosDB_GetBranch_AsymmetricDepth(t *testing.T) {
	// Common stem 0-5; branch A extends 3 more (6A/7A/8A);
	// branch B extends only 1 (6B). LCA = stem[5].
	k := NewKhaosDB()
	stem := chainOfBlocks(5)
	k.Start(stem[0])
	for i := 1; i <= 5; i++ {
		k.Push(stem[i])
	}

	branchA := make([]*types.Block, 4)
	branchA[0] = stem[5]
	for i := 1; i <= 3; i++ {
		branchA[i] = makeKhaosTestBlock(uint64(5+i), branchA[i-1].Hash())
		k.Push(branchA[i])
	}

	branchB1 := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     6,
				ParentHash: stem[5].Hash().Bytes(),
				Timestamp:  99999,
			},
		},
	})
	k.Push(branchB1)

	b1, b2, err := k.GetBranch(branchA[3].Hash(), branchB1.Hash())
	if err != nil {
		t.Fatalf("GetBranch: %v", err)
	}
	if len(b1) != 3 {
		t.Fatalf("branch A: want 3, got %d", len(b1))
	}
	if len(b2) != 1 {
		t.Fatalf("branch B: want 1, got %d", len(b2))
	}
}

func TestKhaosDB_Eviction(t *testing.T) {
	const cap = 10
	k := NewKhaosDB()
	k.SetMaxSize(cap)

	chain := chainOfBlocks(cap + 5)
	k.Start(chain[0])
	for i := 1; i <= cap+5; i++ {
		k.Push(chain[i])
	}

	// Blocks with num ≤ head.num - cap should be evicted.
	headNum := uint64(cap + 5)
	threshold := headNum - uint64(cap)
	for i := 1; i <= int(threshold); i++ {
		if k.ContainsInMiniStore(chain[i].Hash()) {
			t.Errorf("block %d should have been evicted (threshold=%d)", i, threshold)
		}
	}
	// Blocks above the threshold must still be present.
	for i := int(threshold) + 1; i <= cap+5; i++ {
		if !k.ContainsInMiniStore(chain[i].Hash()) {
			t.Errorf("block %d should still be in miniStore", i)
		}
	}
}

func TestKhaosDB_RemoveBlk_UpdatesHead(t *testing.T) {
	k := NewKhaosDB()
	chain := chainOfBlocks(5)
	k.Start(chain[0])
	for i := 1; i <= 5; i++ {
		k.Push(chain[i])
	}
	if k.Head().Number() != 5 {
		t.Fatal("pre-condition: head should be 5")
	}

	k.RemoveBlk(chain[5].Hash())
	if k.ContainsInMiniStore(chain[5].Hash()) {
		t.Fatal("block 5 should be removed")
	}
	// Head should now be the highest remaining block (4).
	if k.Head().Number() != 4 {
		t.Fatalf("head after removing block 5: got %d, want 4", k.Head().Number())
	}
}

func TestKhaosDB_Pop(t *testing.T) {
	k := NewKhaosDB()
	chain := chainOfBlocks(3)
	k.Start(chain[0])
	for i := 1; i <= 3; i++ {
		k.Push(chain[i])
	}

	ok := k.Pop()
	if !ok {
		t.Fatal("Pop should return true when parent exists")
	}
	if k.Head().Number() != 2 {
		t.Fatalf("head after Pop: got %d, want 2", k.Head().Number())
	}
}

func TestKhaosDB_BadBlockNumber(t *testing.T) {
	k := NewKhaosDB()
	chain := chainOfBlocks(2)
	k.Start(chain[0])
	k.Push(chain[1])

	// Block claims number 5 but parent is block 1 (num=1 → expected 2).
	bad := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     5,
				ParentHash: chain[1].Hash().Bytes(),
			},
		},
	})
	_, err := k.Push(bad)
	if err != ErrBadBlockNumber {
		t.Fatalf("want ErrBadBlockNumber, got %v", err)
	}
}

// TestKhaosDB_GetBranch_WindowExceeded verifies that GetBranch returns
// ErrNonCommonBlock when the LCA has been evicted from the window.
func TestKhaosDB_GetBranch_WindowExceeded(t *testing.T) {
	const cap = 5
	k := NewKhaosDB()
	k.SetMaxSize(cap)

	// Build stem of 20 blocks; LCA will be evicted.
	chain := chainOfBlocks(20)
	k.Start(chain[0])
	for i := 1; i <= 10; i++ {
		k.Push(chain[i])
	}

	// Fork from block 3 — but first push 20 more blocks on main chain to evict it.
	// (We can't actually fork from block 3 since it's already evicted after head=15.)
	// Instead: start a fresh KhaosDB with cap=5, build common stem of 3 blocks,
	// then fork, then push 6 more on main to evict the LCA.
	k2 := NewKhaosDB()
	k2.SetMaxSize(5)
	stem2 := chainOfBlocks(3)
	k2.Start(stem2[0])
	k2.Push(stem2[1])
	k2.Push(stem2[2])
	k2.Push(stem2[3])

	// Fork at block 3: forkTip at height 4.
	forkTip := types.NewBlockFromPB(&corepb.Block{
		BlockHeader: &corepb.BlockHeader{
			RawData: &corepb.BlockHeaderRaw{
				Number:     4,
				ParentHash: stem2[3].Hash().Bytes(),
				Timestamp:  7777,
			},
		},
	})
	k2.Push(forkTip)

	// Extend main chain 8 more blocks — this evicts blocks 0-3 (LCA is block 3).
	ext := make([]*types.Block, 9)
	ext[0] = stem2[3]
	for i := 1; i <= 8; i++ {
		ext[i] = makeKhaosTestBlock(uint64(3+i), ext[i-1].Hash())
		k2.Push(ext[i])
	}

	// LCA (block 3) should now be evicted from mainStore; GetBranch must error.
	_, _, err := k2.GetBranch(ext[8].Hash(), forkTip.Hash())
	if err != ErrNonCommonBlock {
		t.Fatalf("want ErrNonCommonBlock after LCA eviction, got %v", err)
	}
}

// Verify ContainsBlock and GetBlock cover both stores.
func TestKhaosDB_GetBlock(t *testing.T) {
	k := NewKhaosDB()
	chain := chainOfBlocks(2)
	k.Start(chain[0])
	k.Push(chain[1])

	// Unlinked block.
	orphan := makeKhaosTestBlock(99, tcommon.HexToHash(fmt.Sprintf("%064d", 99)))
	k.Push(orphan) // lands in unlinked store

	if k.GetBlock(chain[1].Hash()) == nil {
		t.Error("block 1 should be retrievable from miniStore")
	}
	if k.GetBlock(orphan.Hash()) == nil {
		t.Error("orphan should be retrievable from miniUnlinkedStore")
	}
	if k.GetBlock(tcommon.Hash{0xff}) != nil {
		t.Error("unknown hash should return nil")
	}
}
