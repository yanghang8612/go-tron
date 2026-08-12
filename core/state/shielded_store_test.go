package state

import (
	"bytes"
	"strings"
	"testing"

	"github.com/tronprotocol/go-tron/core/rawdb"
	"github.com/tronprotocol/go-tron/core/state/kvdomains"
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
	if ok, err := reopened.HasNullifierStrict(nullifier); err != nil || !ok {
		t.Fatalf("HasNullifierStrict = %v/%v, want true/nil", ok, err)
	}
	if ok, exists := reopened.ReadZKProofResult(txOK); !exists || !ok {
		t.Fatalf("proof ok = (%v,%v), want (true,true)", ok, exists)
	}
	if ok, exists, err := reopened.ReadZKProofResultStrict(txOK); err != nil || !exists || !ok {
		t.Fatalf("ReadZKProofResultStrict ok = (%v,%v,%v), want true/true/nil", ok, exists, err)
	}
	if ok, exists := reopened.ReadZKProofResult(txFail); !exists || ok {
		t.Fatalf("proof fail = (%v,%v), want (false,true)", ok, exists)
	}
	if got := reopened.NoteCommitmentCount(); got != 2 {
		t.Fatalf("note commitment count = %d, want 2", got)
	}
	if got, ok, err := reopened.NoteCommitmentCountStrict(); err != nil || !ok || got != 2 {
		t.Fatalf("NoteCommitmentCountStrict = %d/%v/%v, want 2/true/nil", got, ok, err)
	}
	if got := reopened.ReadNoteCommitment(0); !bytes.Equal(got, commitment1) {
		t.Fatalf("commitment[0] = %x, want %x", got, commitment1)
	}
	if got, ok, err := reopened.ReadNoteCommitmentStrict(0); err != nil || !ok || !bytes.Equal(got, commitment1) {
		t.Fatalf("ReadNoteCommitmentStrict(0) = %x/%v/%v, want commitment/true/nil", got, ok, err)
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
	if ok, err := reopened.HasIncrMerkleTreeStrict(root); err != nil || !ok {
		t.Fatalf("HasIncrMerkleTreeStrict = %v/%v, want true/nil", ok, err)
	}
	if got := reopened.ReadIncrMerkleTree(root); !proto.Equal(got, tree) {
		t.Fatalf("root tree = %v, want %v", got, tree)
	}
	if got, ok, err := reopened.ReadIncrMerkleTreeStrict(root); err != nil || !ok || !proto.Equal(got, tree) {
		t.Fatalf("ReadIncrMerkleTreeStrict = %v/%v/%v, want tree/true/nil", got, ok, err)
	}
	if got := reopened.ReadLastMerkleTree(); !proto.Equal(got, tree) {
		t.Fatalf("last tree = %v, want %v", got, tree)
	}
	if got, ok, err := reopened.ReadLastMerkleTreeStrict(); err != nil || !ok || !proto.Equal(got, tree) {
		t.Fatalf("ReadLastMerkleTreeStrict = %v/%v/%v, want tree/true/nil", got, ok, err)
	}
	if got := reopened.ReadCurrentMerkleTree(); got != nil {
		t.Fatalf("current tree should have been deleted, got %v", got)
	}
	if got, ok, err := reopened.ReadCurrentMerkleTreeStrict(); err != nil || ok || got != nil {
		t.Fatalf("ReadCurrentMerkleTreeStrict deleted = %v/%v/%v, want nil/false/nil", got, ok, err)
	}
	if got := reopened.ReadMerkleTreeRootByBlock(123); !bytes.Equal(got, root) {
		t.Fatalf("block root = %x, want %x", got, root)
	}
	if got, ok, err := reopened.ReadMerkleTreeRootByBlockStrict(123); err != nil || !ok || !bytes.Equal(got, root) {
		t.Fatalf("ReadMerkleTreeRootByBlockStrict = %x/%v/%v, want root/true/nil", got, ok, err)
	}
}

func TestShieldedStoreReadIncrMerkleTreeStrictSurfacesCorruptPayload(t *testing.T) {
	sdb := newTestStateDB(t)
	root := make([]byte, 32)
	root[0] = 0x5a

	if err := sdb.SystemKVPut(kvdomains.SystemShielded, rawdb.IncrMerkleTreeStateKey(root), []byte{0x80}); err != nil {
		t.Fatalf("write corrupt shielded tree: %v", err)
	}
	if sdb.HasIncrMerkleTree(root) {
		t.Fatal("corrupt tree must not satisfy the existence predicate")
	}
	if got := sdb.ReadIncrMerkleTree(root); got != nil {
		t.Fatalf("compat ReadIncrMerkleTree corrupt payload = %v, want nil", got)
	}
	if sdb.Error() == nil {
		t.Fatal("corrupt shielded tree did not poison StateDB")
	}
	got, ok, err := sdb.ReadIncrMerkleTreeStrict(root)
	if err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode incremental merkle tree") {
		t.Fatalf("strict corrupt tree = %v ok=%v err=%v, want decode error", got, ok, err)
	}
}

func TestShieldedStoreStrictReadersSurfaceCorruptRows(t *testing.T) {
	sdb := newTestStateDB(t)
	txID := []byte("bad-proof")
	blockRoot := bytes.Repeat([]byte{0x77}, 32)

	if err := sdb.SystemKVPut(kvdomains.SystemShielded, rawdb.NoteCommitmentCountStateKey(), []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if got := sdb.NoteCommitmentCount(); got != 0 {
		t.Fatalf("compat NoteCommitmentCount corrupt row = %d, want 0", got)
	}
	if _, ok, err := sdb.NoteCommitmentCountStrict(); err == nil || ok || !strings.Contains(err.Error(), "length 1, want 8") {
		t.Fatalf("NoteCommitmentCountStrict corrupt ok=%v err=%v, want length error", ok, err)
	}

	sdb = newTestStateDB(t)
	if err := sdb.SystemKVPut(kvdomains.SystemShielded, rawdb.ZKProofStateKey(txID), []byte{0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	if ok, exists := sdb.ReadZKProofResult(txID); exists || ok {
		t.Fatalf("compat ReadZKProofResult corrupt row = %v/%v, want fail-closed false/false", ok, exists)
	}
	if _, ok, err := sdb.ReadZKProofResultStrict(txID); err == nil || ok || !strings.Contains(err.Error(), "length 2, want 1") {
		t.Fatalf("ReadZKProofResultStrict corrupt ok=%v err=%v, want length error", ok, err)
	}

	sdb = newTestStateDB(t)
	if err := sdb.SystemKVPut(kvdomains.SystemShielded, rawdb.IncrMerkleLastTreeStateKey(), []byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if got := sdb.ReadLastMerkleTree(); got != nil {
		t.Fatalf("compat ReadLastMerkleTree corrupt row = %v, want nil", got)
	}
	if got, ok, err := sdb.ReadLastMerkleTreeStrict(); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode last incremental merkle tree") {
		t.Fatalf("ReadLastMerkleTreeStrict corrupt = %v/%v/%v, want decode error", got, ok, err)
	}

	sdb = newTestStateDB(t)
	if err := sdb.SystemKVPut(kvdomains.SystemShielded, rawdb.IncrMerkleCurrentTreeStateKey(), []byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if got := sdb.ReadCurrentMerkleTree(); got != nil {
		t.Fatalf("compat ReadCurrentMerkleTree corrupt row = %v, want nil", got)
	}
	if got, ok, err := sdb.ReadCurrentMerkleTreeStrict(); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode current incremental merkle tree") {
		t.Fatalf("ReadCurrentMerkleTreeStrict corrupt = %v/%v/%v, want decode error", got, ok, err)
	}

	sdb = newTestStateDB(t)
	if err := sdb.SystemKVPut(kvdomains.SystemShielded, rawdb.MerkleTreeIndexStateKey(9), []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if got := sdb.ReadMerkleTreeRootByBlock(9); got != nil {
		t.Fatalf("compat ReadMerkleTreeRootByBlock corrupt row = %x, want nil", got)
	}
	if got, ok, err := sdb.ReadMerkleTreeRootByBlockStrict(9); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "length 1, want 32") {
		t.Fatalf("ReadMerkleTreeRootByBlockStrict corrupt = %x/%v/%v, want length error", got, ok, err)
	}

	sdb = newTestStateDB(t)
	if err := sdb.SystemKVPut(kvdomains.SystemShielded, rawdb.MerkleTreeIndexStateKey(10), blockRoot); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := sdb.ReadMerkleTreeRootByBlockStrict(10); err != nil || !ok || !bytes.Equal(got, blockRoot) {
		t.Fatalf("ReadMerkleTreeRootByBlockStrict valid = %x/%v/%v, want root/true/nil", got, ok, err)
	}
}
