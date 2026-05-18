package rawdb

import (
	"testing"

	"github.com/ethereum/go-ethereum/ethdb/memorydb"
	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// ---- ZKProof tests --------------------------------------------------------

func TestZKProof_HasWriteDelete(t *testing.T) {
	db := memorydb.New()
	txID := make([]byte, 32)
	txID[0] = 0xAB
	txID[1] = 0xCD

	if HasZKProof(db, txID) {
		t.Fatal("expected absent before write")
	}
	if _, ok := ReadZKProofResult(db, txID); ok {
		t.Fatal("expected no cached result before write")
	}

	if err := WriteZKProof(db, txID); err != nil {
		t.Fatalf("WriteZKProof: %v", err)
	}
	if !HasZKProof(db, txID) {
		t.Fatal("expected present after write")
	}
	if result, ok := ReadZKProofResult(db, txID); !ok || !result {
		t.Fatalf("cached result: got (%v,%v), want (true,true)", result, ok)
	}

	if err := DeleteZKProof(db, txID); err != nil {
		t.Fatalf("DeleteZKProof: %v", err)
	}
	if HasZKProof(db, txID) {
		t.Fatal("expected absent after delete")
	}
}

func TestZKProof_DifferentTransactionsDoNotCollide(t *testing.T) {
	db := memorydb.New()
	txID1 := []byte("transaction-one")
	txID2 := []byte("transaction-two")

	if err := WriteZKProof(db, txID1); err != nil {
		t.Fatal(err)
	}
	if HasZKProof(db, txID2) {
		t.Fatal("txID2 should not be present after writing txID1")
	}
}

func TestZKProof_FailedResultIsCached(t *testing.T) {
	db := memorydb.New()
	txID := []byte("failed-shielded-tx")
	if err := WriteZKProofResult(db, txID, false); err != nil {
		t.Fatal(err)
	}
	if !HasZKProof(db, txID) {
		t.Fatal("failed result should still create a cache entry")
	}
	if result, ok := ReadZKProofResult(db, txID); !ok || result {
		t.Fatalf("cached result: got (%v,%v), want (false,true)", result, ok)
	}
}

// ---- IncrementalMerkleTree tests ------------------------------------------

func TestIncrMerkleTree_RoundTrip(t *testing.T) {
	db := memorydb.New()
	root := make([]byte, 32)
	root[0] = 0xDE
	root[31] = 0xAD

	if HasIncrMerkleTree(db, root) {
		t.Fatal("expected absent before write")
	}

	tree := &shieldpb.IncrementalMerkleTree{
		Left:  &shieldpb.PedersenHash{Content: []byte("left-hash")},
		Right: &shieldpb.PedersenHash{Content: []byte("right-hash")},
	}

	if err := WriteIncrMerkleTree(db, root, tree); err != nil {
		t.Fatalf("WriteIncrMerkleTree: %v", err)
	}

	if !HasIncrMerkleTree(db, root) {
		t.Fatal("expected present after write")
	}

	got := ReadIncrMerkleTree(db, root)
	if got == nil {
		t.Fatal("ReadIncrMerkleTree returned nil")
	}
	if got.Left == nil || string(got.Left.Content) != "left-hash" {
		t.Errorf("Left hash mismatch: got %v", got.Left)
	}
	if got.Right == nil || string(got.Right.Content) != "right-hash" {
		t.Errorf("Right hash mismatch: got %v", got.Right)
	}
}

func TestIncrMerkleTree_Absent(t *testing.T) {
	db := memorydb.New()
	root := make([]byte, 32)
	if got := ReadIncrMerkleTree(db, root); got != nil {
		t.Fatalf("expected nil for absent root, got %v", got)
	}
}

func TestIncrMerkleTree_Delete(t *testing.T) {
	db := memorydb.New()
	root := []byte("anchor-root-32bytes-padded-xxxxx")
	tree := &shieldpb.IncrementalMerkleTree{}

	if err := WriteIncrMerkleTree(db, root, tree); err != nil {
		t.Fatal(err)
	}
	if err := DeleteIncrMerkleTree(db, root); err != nil {
		t.Fatal(err)
	}
	if HasIncrMerkleTree(db, root) {
		t.Fatal("expected absent after delete")
	}
}

func TestIncrMerkleTree_MultipleRoots(t *testing.T) {
	db := memorydb.New()
	roots := [][]byte{
		{0x01},
		{0x02},
		{0x03},
	}
	for i, root := range roots {
		tree := &shieldpb.IncrementalMerkleTree{
			Parents: []*shieldpb.PedersenHash{
				{Content: []byte{byte(i)}},
			},
		}
		if err := WriteIncrMerkleTree(db, root, tree); err != nil {
			t.Fatalf("root %d: %v", i, err)
		}
	}
	for i, root := range roots {
		got := ReadIncrMerkleTree(db, root)
		if got == nil {
			t.Fatalf("root %d: got nil", i)
		}
		if len(got.Parents) != 1 || got.Parents[0].Content[0] != byte(i) {
			t.Errorf("root %d: parent mismatch", i)
		}
	}
}

// ---- MerkleContainer LAST_TREE / CURRENT_TREE / blocknum-index tests --

func TestLastMerkleTree_RoundTrip(t *testing.T) {
	db := memorydb.New()

	if got := ReadLastMerkleTree(db); got != nil {
		t.Fatalf("expected nil before write, got %v", got)
	}
	tree := &shieldpb.IncrementalMerkleTree{
		Left: &shieldpb.PedersenHash{Content: []byte("best-left")},
	}
	if err := WriteLastMerkleTree(db, tree); err != nil {
		t.Fatalf("WriteLastMerkleTree: %v", err)
	}
	got := ReadLastMerkleTree(db)
	if got == nil || got.Left == nil || string(got.Left.Content) != "best-left" {
		t.Fatalf("ReadLastMerkleTree mismatch: %v", got)
	}
}

func TestCurrentMerkleTree_RoundTripAndDelete(t *testing.T) {
	db := memorydb.New()

	if got := ReadCurrentMerkleTree(db); got != nil {
		t.Fatalf("expected nil before write, got %v", got)
	}
	tree := &shieldpb.IncrementalMerkleTree{
		Right: &shieldpb.PedersenHash{Content: []byte("current-right")},
	}
	if err := WriteCurrentMerkleTree(db, tree); err != nil {
		t.Fatalf("WriteCurrentMerkleTree: %v", err)
	}
	got := ReadCurrentMerkleTree(db)
	if got == nil || got.Right == nil || string(got.Right.Content) != "current-right" {
		t.Fatalf("ReadCurrentMerkleTree mismatch: %v", got)
	}

	if err := DeleteCurrentMerkleTree(db); err != nil {
		t.Fatalf("DeleteCurrentMerkleTree: %v", err)
	}
	if got := ReadCurrentMerkleTree(db); got != nil {
		t.Fatalf("expected nil after delete, got %v", got)
	}
}

func TestMerkleTreeRootByBlock_RoundTrip(t *testing.T) {
	db := memorydb.New()
	const blockNum = int64(1_685_793)
	root := make([]byte, 32)
	root[0] = 0x9a
	root[31] = 0xbc

	if got := ReadMerkleTreeRootByBlock(db, blockNum); got != nil {
		t.Fatalf("expected nil before write, got %x", got)
	}
	if err := WriteMerkleTreeRootByBlock(db, blockNum, root); err != nil {
		t.Fatalf("WriteMerkleTreeRootByBlock: %v", err)
	}
	got := ReadMerkleTreeRootByBlock(db, blockNum)
	if string(got) != string(root) {
		t.Fatalf("root mismatch: got %x, want %x", got, root)
	}

	// Distinct block numbers do not collide.
	other := int64(1_628_391)
	if got := ReadMerkleTreeRootByBlock(db, other); got != nil {
		t.Fatalf("unrelated blockNum collided: %x", got)
	}

	if err := DeleteMerkleTreeRootByBlock(db, blockNum); err != nil {
		t.Fatalf("DeleteMerkleTreeRootByBlock: %v", err)
	}
	if got := ReadMerkleTreeRootByBlock(db, blockNum); got != nil {
		t.Fatalf("expected nil after delete, got %x", got)
	}
}

// Sanity: the "LAST_TREE"/"CURRENT_TREE" sentinels live inside the imt-
// namespace but must not be picked up by a root-keyed lookup. A 32-byte
// root whose hex spells "LAST_TREE..." is structurally impossible to
// collide because the sentinel keys are 13/16 bytes — but verify the
// negative case to lock the invariant.
func TestMerkleSentinels_DoNotCollideWithRoots(t *testing.T) {
	db := memorydb.New()
	last := &shieldpb.IncrementalMerkleTree{
		Left: &shieldpb.PedersenHash{Content: []byte("sentinel")},
	}
	if err := WriteLastMerkleTree(db, last); err != nil {
		t.Fatal(err)
	}
	// 32-byte root that, when hex-encoded, starts with "LAST_TREE": the
	// raw-byte key would be "imt-LAST_TREE" || padding (45 bytes total),
	// which can't collide with the 13-byte sentinel.
	root := []byte("LAST_TREE\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00")
	if HasIncrMerkleTree(db, root) {
		t.Fatal("root lookup hit the LAST_TREE sentinel — namespace bug")
	}
}
