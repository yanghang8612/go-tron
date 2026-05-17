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
