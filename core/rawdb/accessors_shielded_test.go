package rawdb

import (
	"errors"
	"strings"
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

func TestZKProofStrictSurfacesStorageAndMalformedRows(t *testing.T) {
	db := memorydb.New()
	txID := []byte("strict-shielded-tx")
	if err := WriteZKProofResult(db, txID, true); err != nil {
		t.Fatal(err)
	}
	if ok, err := HasZKProofStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, txID); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("HasZKProofStrict has error = %v/%v, want presence error", ok, err)
	}
	if _, ok, err := ReadZKProofResultStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, txID); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadZKProofResultStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadZKProofResultStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, txID); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadZKProofResultStrict get error ok=%v err=%v, want get error", ok, err)
	}

	if err := db.Put(ZKProofStateKey(txID), []byte{0x01, 0x02}); err != nil {
		t.Fatal(err)
	}
	if result, ok := ReadZKProofResult(db, txID); !ok || !result {
		t.Fatalf("compat ReadZKProofResult malformed row = %v/%v, want first-byte result", result, ok)
	}
	if _, ok, err := ReadZKProofResultStrict(db, txID); err == nil || ok || !strings.Contains(err.Error(), "length 2, want 1") {
		t.Fatalf("ReadZKProofResultStrict malformed ok=%v err=%v, want length error", ok, err)
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
	if got, ok, err := ReadIncrMerkleTreeStrict(db, root); got != nil || ok || err != nil {
		t.Fatalf("strict absent = %v ok=%v err=%v, want nil false nil", got, ok, err)
	}
}

func TestIncrMerkleTree_StrictSurfacesCorruptPayload(t *testing.T) {
	db := memorydb.New()
	root := make([]byte, 32)
	root[0] = 0xC0

	if err := db.Put(IncrMerkleTreeStateKey(root), []byte{0x80}); err != nil {
		t.Fatalf("write corrupt tree: %v", err)
	}
	if got := ReadIncrMerkleTree(db, root); got != nil {
		t.Fatalf("compat ReadIncrMerkleTree corrupt payload = %v, want nil", got)
	}
	got, ok, err := ReadIncrMerkleTreeStrict(db, root)
	if err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode incremental merkle tree") {
		t.Fatalf("strict corrupt tree = %v ok=%v err=%v, want decode error", got, ok, err)
	}
}

func TestIncrMerkleTreeStrictSurfacesStorageErrors(t *testing.T) {
	db := memorydb.New()
	root := make([]byte, 32)
	root[0] = 0xc1
	if err := WriteIncrMerkleTree(db, root, &shieldpb.IncrementalMerkleTree{}); err != nil {
		t.Fatal(err)
	}
	if ok, err := HasIncrMerkleTreeStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, root); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("HasIncrMerkleTreeStrict has error = %v/%v, want presence error", ok, err)
	}
	if _, ok, err := ReadIncrMerkleTreeStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, root); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadIncrMerkleTreeStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadIncrMerkleTreeStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, root); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadIncrMerkleTreeStrict get error ok=%v err=%v, want get error", ok, err)
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

func TestShieldedNoteCommitmentStrictReaders(t *testing.T) {
	db := memorydb.New()
	commitment := []byte("commitment")
	if got, ok, err := NoteCommitmentCountStrict(db); err != nil || ok || got != 0 {
		t.Fatalf("NoteCommitmentCountStrict absent = %d/%v/%v, want 0/false/nil", got, ok, err)
	}
	if got, ok, err := ReadNoteCommitmentStrict(db, 0); err != nil || ok || got != nil {
		t.Fatalf("ReadNoteCommitmentStrict absent = %x/%v/%v, want nil/false/nil", got, ok, err)
	}
	if err := AppendNoteCommitment(db, commitment); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := NoteCommitmentCountStrict(db); err != nil || !ok || got != 1 {
		t.Fatalf("NoteCommitmentCountStrict = %d/%v/%v, want 1/true/nil", got, ok, err)
	}
	if got, ok, err := ReadNoteCommitmentStrict(db, 0); err != nil || !ok || string(got) != string(commitment) {
		t.Fatalf("ReadNoteCommitmentStrict = %x/%v/%v, want commitment/true/nil", got, ok, err)
	}
	if _, ok, err := NoteCommitmentCountStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("NoteCommitmentCountStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := NoteCommitmentCountStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("NoteCommitmentCountStrict get error ok=%v err=%v, want get error", ok, err)
	}
	if _, ok, err := ReadNoteCommitmentStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, 0); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadNoteCommitmentStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadNoteCommitmentStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, 0); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadNoteCommitmentStrict get error ok=%v err=%v, want get error", ok, err)
	}

	if err := db.Put(NoteCommitmentCountStateKey(), []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if got := NoteCommitmentCount(db); got != 0 {
		t.Fatalf("compat NoteCommitmentCount malformed = %d, want 0", got)
	}
	if _, ok, err := NoteCommitmentCountStrict(db); err == nil || ok || !strings.Contains(err.Error(), "length 1, want 8") {
		t.Fatalf("NoteCommitmentCountStrict malformed ok=%v err=%v, want length error", ok, err)
	}
}

func TestMerkleSentinelStrictReaders(t *testing.T) {
	db := memorydb.New()
	tree := &shieldpb.IncrementalMerkleTree{
		Left: &shieldpb.PedersenHash{Content: []byte("left")},
	}
	if got, ok, err := ReadLastMerkleTreeStrict(db); err != nil || ok || got != nil {
		t.Fatalf("ReadLastMerkleTreeStrict absent = %v/%v/%v, want nil/false/nil", got, ok, err)
	}
	if got, ok, err := ReadCurrentMerkleTreeStrict(db); err != nil || ok || got != nil {
		t.Fatalf("ReadCurrentMerkleTreeStrict absent = %v/%v/%v, want nil/false/nil", got, ok, err)
	}
	if err := WriteLastMerkleTree(db, tree); err != nil {
		t.Fatal(err)
	}
	if err := WriteCurrentMerkleTree(db, tree); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadLastMerkleTreeStrict(db); err != nil || !ok || got == nil || got.Left == nil || string(got.Left.Content) != "left" {
		t.Fatalf("ReadLastMerkleTreeStrict = %v/%v/%v, want tree/true/nil", got, ok, err)
	}
	if got, ok, err := ReadCurrentMerkleTreeStrict(db); err != nil || !ok || got == nil || got.Left == nil || string(got.Left.Content) != "left" {
		t.Fatalf("ReadCurrentMerkleTreeStrict = %v/%v/%v, want tree/true/nil", got, ok, err)
	}
	if _, ok, err := ReadLastMerkleTreeStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadLastMerkleTreeStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadCurrentMerkleTreeStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadCurrentMerkleTreeStrict get error ok=%v err=%v, want get error", ok, err)
	}

	if err := db.Put(IncrMerkleLastTreeStateKey(), []byte{0x80}); err != nil {
		t.Fatal(err)
	}
	if got := ReadLastMerkleTree(db); got != nil {
		t.Fatalf("compat ReadLastMerkleTree corrupt payload = %v, want nil", got)
	}
	if got, ok, err := ReadLastMerkleTreeStrict(db); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "decode last incremental merkle tree") {
		t.Fatalf("ReadLastMerkleTreeStrict corrupt = %v/%v/%v, want decode error", got, ok, err)
	}
	if err := db.Put(IncrMerkleCurrentTreeStateKey(), nil); err != nil {
		t.Fatal(err)
	}
	if got := ReadCurrentMerkleTree(db); got != nil {
		t.Fatalf("compat ReadCurrentMerkleTree empty payload = %v, want nil", got)
	}
	if got, ok, err := ReadCurrentMerkleTreeStrict(db); err != nil || !ok || got == nil {
		t.Fatalf("ReadCurrentMerkleTreeStrict empty payload = %v/%v/%v, want empty tree/true/nil", got, ok, err)
	}
}

func TestMerkleTreeRootByBlockStrict(t *testing.T) {
	db := memorydb.New()
	const blockNum = int64(77)
	root := make([]byte, 32)
	root[31] = 0x77
	if got, ok, err := ReadMerkleTreeRootByBlockStrict(db, blockNum); err != nil || ok || got != nil {
		t.Fatalf("ReadMerkleTreeRootByBlockStrict absent = %x/%v/%v, want nil/false/nil", got, ok, err)
	}
	if err := WriteMerkleTreeRootByBlock(db, blockNum, root); err != nil {
		t.Fatal(err)
	}
	if got, ok, err := ReadMerkleTreeRootByBlockStrict(db, blockNum); err != nil || !ok || string(got) != string(root) {
		t.Fatalf("ReadMerkleTreeRootByBlockStrict = %x/%v/%v, want root/true/nil", got, ok, err)
	}
	if _, ok, err := ReadMerkleTreeRootByBlockStrict(failingStateDomainReader{reader: db, hasErr: errors.New("has boom")}, blockNum); err == nil || ok || !strings.Contains(err.Error(), "presence") {
		t.Fatalf("ReadMerkleTreeRootByBlockStrict has error ok=%v err=%v, want presence error", ok, err)
	}
	if _, ok, err := ReadMerkleTreeRootByBlockStrict(failingStateDomainReader{reader: db, getErr: errors.New("get boom")}, blockNum); err == nil || ok || !strings.Contains(err.Error(), "get boom") {
		t.Fatalf("ReadMerkleTreeRootByBlockStrict get error ok=%v err=%v, want get error", ok, err)
	}
	if err := db.Put(MerkleTreeIndexStateKey(blockNum), []byte{0x01}); err != nil {
		t.Fatal(err)
	}
	if got := ReadMerkleTreeRootByBlock(db, blockNum); string(got) != string([]byte{0x01}) {
		t.Fatalf("compat ReadMerkleTreeRootByBlock malformed = %x, want raw", got)
	}
	if got, ok, err := ReadMerkleTreeRootByBlockStrict(db, blockNum); err == nil || !ok || got != nil || !strings.Contains(err.Error(), "length 1, want 32") {
		t.Fatalf("ReadMerkleTreeRootByBlockStrict malformed = %x/%v/%v, want length error", got, ok, err)
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
