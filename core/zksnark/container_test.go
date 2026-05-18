package zksnark

import (
	"bytes"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	"github.com/tronprotocol/go-tron/core/rawdb"
	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// TestContainerEmptyFallback covers MerkleContainer.GetCurrent /
// GetBest with no prior writes — both fall back to the empty tree.
func TestContainerEmptyFallback(t *testing.T) {
	db := memorydb.New()
	c := NewMerkleContainer(db)

	if t1, t2 := c.GetCurrent(), c.GetBest(); t1.Size() != 0 || t2.Size() != 0 {
		t.Fatalf("empty container should give size-0 trees: current=%d best=%d", t1.Size(), t2.Size())
	}
}

// TestContainerResetFromBest covers the pre-tx lifecycle hook: CURRENT_TREE
// becomes a copy of LAST_TREE.
func TestContainerResetFromBest(t *testing.T) {
	db := memorydb.New()
	c := NewMerkleContainer(db)

	// Plant a non-trivial LAST_TREE.
	leaf := PedersenHash{0x11, 0x22, 0x33}
	best := NewTree()
	if err := best.Append(leaf); err != nil {
		t.Fatalf("seed best: %v", err)
	}
	if err := rawdb.WriteLastMerkleTree(db, best.Proto()); err != nil {
		t.Fatal(err)
	}

	// Reset should populate CURRENT_TREE with the same left slot.
	if err := c.ResetCurrent(); err != nil {
		t.Fatalf("ResetCurrent: %v", err)
	}
	cur := c.GetCurrent()
	if !bytes.Equal(cur.Proto().GetLeft().GetContent(), leaf[:]) {
		t.Errorf("CURRENT_TREE.left = %x, want %x", cur.Proto().GetLeft().GetContent(), leaf[:])
	}
}

// TestContainerAppendPersists covers AppendCommitment: the cm lands in
// CURRENT_TREE and a subsequent GetCurrent reflects it. Stays within one
// tx (≤ 2 cms) so no internal Combine fires; works under the default
// no-sapling build.
func TestContainerAppendPersists(t *testing.T) {
	db := memorydb.New()
	c := NewMerkleContainer(db)

	cm1 := PedersenHash{0xaa, 0xbb}
	cm2 := PedersenHash{0xcc, 0xdd}

	if err := c.AppendCommitment(cm1); err != nil {
		t.Fatalf("Append cm1: %v", err)
	}
	if err := c.AppendCommitment(cm2); err != nil {
		t.Fatalf("Append cm2: %v", err)
	}
	cur := c.GetCurrent()
	if !bytes.Equal(cur.Proto().GetLeft().GetContent(), cm1[:]) ||
		!bytes.Equal(cur.Proto().GetRight().GetContent(), cm2[:]) {
		t.Fatalf("CURRENT_TREE not populated as expected: left=%x right=%x",
			cur.Proto().GetLeft().GetContent(), cur.Proto().GetRight().GetContent())
	}
	if got := cur.Size(); got != 2 {
		t.Fatalf("Size after 2 appends: got %d, want 2", got)
	}
}

// TestContainerSaveRequiresPedersen documents the stub-build failure mode:
// SaveCurrentAsBest must compute a tree root (Pedersen Combine) which
// returns ErrPedersenUnimplemented under the no-sapling build. The test
// pins that contract so a future refactor doesn't silently regress
// (e.g. by caching an empty root and silently saving invalid state).
func TestContainerSaveRequiresPedersen(t *testing.T) {
	db := memorydb.New()
	c := NewMerkleContainer(db)

	if err := c.AppendCommitment(PedersenHash{0xfe}); err != nil {
		t.Fatal(err)
	}
	err := c.SaveCurrentAsBest(123)
	if err == nil {
		// Real Pedersen backend is wired (`-tags=sapling`); SaveCurrentAsBest
		// must succeed and the round-trip should be verifiable. Spot-check the
		// blockNum index landed.
		if root := rawdb.ReadMerkleTreeRootByBlock(db, 123); root == nil {
			t.Fatal("SaveCurrentAsBest succeeded but blockNum→root index missing")
		}
		return
	}
	if !errors.Is(err, ErrPedersenUnimplemented) {
		t.Fatalf("SaveCurrentAsBest error: got %v, want ErrPedersenUnimplemented", err)
	}
}

// TestContainerSaveReusesPreviousRootWhenTreeUnchanged covers the common
// no-shielded-receive block path: java-tron still records blockNum→root every
// block, but an unchanged CURRENT_TREE can reuse the previous block's root
// without recomputing Pedersen hashes.
func TestContainerSaveReusesPreviousRootWhenTreeUnchanged(t *testing.T) {
	db := memorydb.New()
	c := NewMerkleContainer(db)

	best := NewTree()
	if err := best.Append(PedersenHash{0x11}); err != nil {
		t.Fatalf("seed best: %v", err)
	}
	if err := rawdb.WriteLastMerkleTree(db, best.Proto()); err != nil {
		t.Fatal(err)
	}
	root := make([]byte, len(PedersenHash{}))
	root[0] = 0xaa
	if err := rawdb.WriteIncrMerkleTree(db, root, best.Proto()); err != nil {
		t.Fatal(err)
	}
	if err := rawdb.WriteMerkleTreeRootByBlock(db, 10, root); err != nil {
		t.Fatal(err)
	}
	if err := c.ResetCurrent(); err != nil {
		t.Fatalf("ResetCurrent: %v", err)
	}
	if err := c.SaveCurrentAsBest(11); err != nil {
		t.Fatalf("SaveCurrentAsBest should reuse previous root without Pedersen: %v", err)
	}
	if got := rawdb.ReadMerkleTreeRootByBlock(db, 11); !bytes.Equal(got, root) {
		t.Fatalf("block 11 root: got %x, want %x", got, root)
	}
}

// TestContainerAnchorExists covers the spend-validation path: a previously
// saved root is reported as a valid anchor; an unrelated root is not.
//
// Doesn't compute roots itself (which would need Pedersen) — manually plants
// a tree under an arbitrary root key via the underlying rawdb accessor.
func TestContainerAnchorExists(t *testing.T) {
	db := memorydb.New()
	c := NewMerkleContainer(db)

	root := make([]byte, 32)
	root[0] = 0xab
	if c.AnchorExists(root) {
		t.Fatal("expected absent before write")
	}
	if err := rawdb.WriteIncrMerkleTree(db, root, &shieldpb.IncrementalMerkleTree{}); err != nil {
		t.Fatal(err)
	}
	if !c.AnchorExists(root) {
		t.Fatal("expected present after write")
	}

	other := make([]byte, 32)
	other[0] = 0xcd
	if c.AnchorExists(other) {
		t.Fatal("unrelated root reported as anchor")
	}
}
