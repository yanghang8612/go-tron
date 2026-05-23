package state

import (
	"bytes"
	"testing"

	contractpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

func TestShieldedStoreRoundTripAtRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	txOK := []byte("tx-ok")
	txFail := []byte("tx-fail")
	nullifier := []byte("nullifier")
	commitment1 := []byte("commitment-1")
	commitment2 := []byte("commitment-2")

	if sdb.HasNullifier(nullifier) {
		t.Fatal("nullifier should be absent before write")
	}
	if _, ok := sdb.ReadZKProofResult(txOK); ok {
		t.Fatal("proof cache should be absent before write")
	}
	if err := sdb.WriteNullifier(nullifier); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteZKProofResult(txOK, true); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteZKProofResult(txFail, false); err != nil {
		t.Fatal(err)
	}
	if err := sdb.AppendNoteCommitment(commitment1); err != nil {
		t.Fatal(err)
	}
	if err := sdb.AppendNoteCommitment(commitment2); err != nil {
		t.Fatal(err)
	}

	root, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(root, sdb.db)
	if err != nil {
		t.Fatal(err)
	}

	if !reopened.HasNullifier(nullifier) {
		t.Fatal("nullifier missing after reopen")
	}
	if ok, exists := reopened.ReadZKProofResult(txOK); !exists || !ok {
		t.Fatalf("proof ok = (%v,%v), want (true,true)", ok, exists)
	}
	if ok, exists := reopened.ReadZKProofResult(txFail); !exists || ok {
		t.Fatalf("proof fail = (%v,%v), want (false,true)", ok, exists)
	}
	if got := reopened.NoteCommitmentCount(); got != 2 {
		t.Fatalf("note commitment count = %d, want 2", got)
	}
	if got := reopened.ReadNoteCommitment(0); !bytes.Equal(got, commitment1) {
		t.Fatalf("commitment[0] = %x, want %x", got, commitment1)
	}
	if got := reopened.ReadNoteCommitment(1); !bytes.Equal(got, commitment2) {
		t.Fatalf("commitment[1] = %x, want %x", got, commitment2)
	}
}

func TestShieldedMerkleStoreRoundTripAtRoot(t *testing.T) {
	sdb := newTestStateDB(t)
	root := bytes.Repeat([]byte{0x42}, 32)
	tree := &contractpb.IncrementalMerkleTree{
		Left: &contractpb.PedersenHash{Content: bytes.Repeat([]byte{0x11}, 32)},
	}

	if err := sdb.WriteIncrMerkleTree(root, tree); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteLastMerkleTree(tree); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteCurrentMerkleTree(tree); err != nil {
		t.Fatal(err)
	}
	if err := sdb.WriteMerkleTreeRootByBlock(123, root); err != nil {
		t.Fatal(err)
	}
	if !sdb.HasIncrMerkleTree(root) {
		t.Fatal("root should be present before commit")
	}
	if err := sdb.DeleteCurrentMerkleTree(); err != nil {
		t.Fatal(err)
	}

	stateRoot, err := sdb.Commit()
	if err != nil {
		t.Fatal(err)
	}
	reopened, err := New(stateRoot, sdb.db)
	if err != nil {
		t.Fatal(err)
	}

	if !reopened.HasIncrMerkleTree(root) {
		t.Fatal("root should be present after reopen")
	}
	if got := reopened.ReadIncrMerkleTree(root); !proto.Equal(got, tree) {
		t.Fatalf("root tree = %v, want %v", got, tree)
	}
	if got := reopened.ReadLastMerkleTree(); !proto.Equal(got, tree) {
		t.Fatalf("last tree = %v, want %v", got, tree)
	}
	if got := reopened.ReadCurrentMerkleTree(); got != nil {
		t.Fatalf("current tree should have been deleted, got %v", got)
	}
	if got := reopened.ReadMerkleTreeRootByBlock(123); !bytes.Equal(got, root) {
		t.Fatalf("block root = %x, want %x", got, root)
	}
}
