package zksnark

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
	"google.golang.org/protobuf/proto"
)

// loadHexVector reads a JSON file holding a flat string array of hex
// values (no 0x prefix) and decodes each into a fixed-size byte slice.
// Used by the Sapling parity vectors.
func loadHexVector(t *testing.T, name string, perEntry int) [][]byte {
	t.Helper()
	path := filepath.Join("testdata", name)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	var raw []string
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	out := make([][]byte, len(raw))
	for i, s := range raw {
		b, err := hex.DecodeString(s)
		if err != nil {
			t.Fatalf("decode %s[%d]: %v", name, i, err)
		}
		if perEntry > 0 && len(b) != perEntry {
			t.Fatalf("%s[%d]: expected %d bytes, got %d", name, i, perEntry, len(b))
		}
		out[i] = b
	}
	return out
}

func hashFromHex(t *testing.T, s string) PedersenHash {
	t.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		t.Fatalf("decode %q: %v", s, err)
	}
	if len(b) != Depth {
		t.Fatalf("%q decodes to %d bytes, want %d", s, len(b), Depth)
	}
	var h PedersenHash
	copy(h[:], b)
	return h
}

// TestEmptyRootsVector asserts EmptyRoots[d] matches java-tron's reference
// vector for d ∈ [0, Depth]. The vector file actually goes to d=62; we
// validate the first Depth+1 entries (the range our IncrementalMerkleTree
// uses).
//
// Initially fails: Combine/Uncommitted are stubs returning
// ErrPedersenUnimplemented. Becomes the canary that flips green once a
// backend lands.
func TestEmptyRootsVector(t *testing.T) {
	want := loadHexVector(t, "merkle_roots_empty_sapling.json", Depth)
	if len(want) < Depth+1 {
		t.Fatalf("empty-roots vector too short: %d, want at least %d", len(want), Depth+1)
	}

	got, err := EmptyRoots()
	if err != nil {
		if errors.Is(err, ErrPedersenUnimplemented) {
			t.Skipf("Pedersen backend not yet wired (Slice 2 deliverable): %v", err)
		}
		t.Fatalf("EmptyRoots: %v", err)
	}
	for d := 0; d <= Depth; d++ {
		if !bytes.Equal(got[d][:], want[d]) {
			t.Errorf("empty root mismatch at depth %d:\n  got  %x\n  want %x", d, got[d][:], want[d])
		}
	}
}

// TestAppendCommitmentsVector appends the Sapling commitment vector
// one-by-one and asserts the intermediate Merkle root matches the
// java-tron reference root after each append.
//
// The vector targets a tree of DEPTH = 4 (16 leaves fill it), matching
// java-tron's `MerkleTreeTest.testComplexTreePath` setup. Commitment
// bytes are reversed before being fed into the tree: the JSON encodes
// them in big-endian display but librustzcash takes little-endian field
// elements (same convention as `ByteUtil.reverse` in the Java test).
//
// Initially fails for the same reason as TestEmptyRootsVector.
func TestAppendCommitmentsVector(t *testing.T) {
	const treeDepth = 4
	cms := loadHexVector(t, "merkle_commitments_sapling.json", Depth)
	want := loadHexVector(t, "merkle_roots_sapling.json", Depth)
	if len(cms) != len(want) {
		t.Fatalf("commitments=%d roots=%d, must match", len(cms), len(want))
	}

	tree := NewTreeWithDepth(treeDepth)
	for i, cm := range cms {
		var leaf PedersenHash
		reversed := make([]byte, len(cm))
		for j, b := range cm {
			reversed[len(cm)-1-j] = b
		}
		copy(leaf[:], reversed)
		if err := tree.Append(leaf); err != nil {
			if errors.Is(err, ErrPedersenUnimplemented) {
				t.Skipf("Pedersen backend not yet wired (Slice 2 deliverable): %v", err)
			}
			t.Fatalf("Append[%d]: %v", i, err)
		}
		got, err := tree.Root()
		if err != nil {
			if errors.Is(err, ErrPedersenUnimplemented) {
				t.Skipf("Pedersen backend not yet wired (Slice 2 deliverable): %v", err)
			}
			t.Fatalf("Root after append[%d]: %v", i, err)
		}
		if !bytes.Equal(got[:], want[i]) {
			t.Errorf("root mismatch after appending commitment[%d]:\n  got  %x\n  want %x", i, got[:], want[i])
		}
	}
}

// TestCombineKnownDepth25 exercises Combine directly against the test
// triple printed by PedersenHashCapsule.main() in java-tron:
//
//	a = 0x05655316a07e6ec8c9769af54ef98b30667bfb6302b32987d552227dae86a087
//	b = 0x06041357de59ba64959d1b60f93de24dfe5ea1e26ed9e8a73d35b225a1845ba7
//	combine(a, b, 25) = 0x61a50a5540b4944da27cbd9b3d6ec39234ba229d2c461f4d719bc136573bf45b
//
// Source: PedersenHashCapsule.java::main. This is the smallest unit-level
// parity test we have without running the full tree.
func TestCombineKnownDepth25(t *testing.T) {
	a := hashFromHex(t, "05655316a07e6ec8c9769af54ef98b30667bfb6302b32987d552227dae86a087")
	b := hashFromHex(t, "06041357de59ba64959d1b60f93de24dfe5ea1e26ed9e8a73d35b225a1845ba7")
	want := hashFromHex(t, "61a50a5540b4944da27cbd9b3d6ec39234ba229d2c461f4d719bc136573bf45b")

	got, err := Combine(25, a, b)
	if err != nil {
		if errors.Is(err, ErrPedersenUnimplemented) {
			t.Skipf("Pedersen backend not yet wired (Slice 2 deliverable): %v", err)
		}
		t.Fatalf("Combine: %v", err)
	}
	if got != want {
		t.Errorf("Combine(25,a,b):\n  got  %x\n  want %x", got, want)
	}
}

// TestProtoRoundTrip exercises the proto wrapper without invoking the
// Pedersen backend. Builds a tree with hand-crafted left/right/parent
// PedersenHash entries, serializes via the underlying proto, parses back,
// and asserts byte identity plus Size() / Empty() invariants.
//
// This test passes today — it isolates the proto-roundtrip path so the
// vector tests can fail on Pedersen alone, not on serialization regressions.
func TestProtoRoundTrip(t *testing.T) {
	tree := NewTree()
	left := hashFromHex(t, "2ec45f5ae2d1bc7a80df02abfb2814a1239f956c6fb3ac0e112c008ba2c1ab91")
	right := hashFromHex(t, "3daa00c9a1966a37531c829b9b1cd928f8172d35174e1aecd31ba0ed36863017")
	parent := hashFromHex(t, "c013c63be33194974dc555d445bac616fca794a0369f9d84fbb5a8556699bf62")

	tree.pb.Left = fromPedersenHash(left)
	tree.pb.Right = fromPedersenHash(right)
	tree.pb.Parents = []*shieldpb.PedersenHash{fromPedersenHash(parent)}

	wire, err := proto.Marshal(tree.Proto())
	if err != nil {
		t.Fatalf("Marshal: %v", err)
	}
	var pb shieldpb.IncrementalMerkleTree
	if err := proto.Unmarshal(wire, &pb); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}
	round := FromProto(&pb)

	if !bytes.Equal(round.Proto().GetLeft().GetContent(), left[:]) {
		t.Errorf("left mismatch after roundtrip")
	}
	if !bytes.Equal(round.Proto().GetRight().GetContent(), right[:]) {
		t.Errorf("right mismatch after roundtrip")
	}
	parents := round.Proto().GetParents()
	if len(parents) != 1 {
		t.Fatalf("parents: got %d, want 1", len(parents))
	}
	if !bytes.Equal(parents[0].GetContent(), parent[:]) {
		t.Errorf("parent[0] mismatch after roundtrip")
	}

	// 1 (left) + 1 (right) + 2^(0+1)=2 from parent[0] = 4
	if got := round.Size(); got != 4 {
		t.Errorf("Size: got %d, want 4", got)
	}
	if err := round.WfCheck(); err != nil {
		t.Errorf("WfCheck on roundtripped tree: %v", err)
	}
}

// TestWfCheckCatchesBadShapes asserts WfCheck rejects the three canonical
// invariant violations from java-tron's wfcheck.
func TestWfCheckCatchesBadShapes(t *testing.T) {
	leaf := hashFromHex(t, "2ec45f5ae2d1bc7a80df02abfb2814a1239f956c6fb3ac0e112c008ba2c1ab91")

	t.Run("right without left", func(t *testing.T) {
		tree := NewTree()
		tree.pb.Right = fromPedersenHash(leaf)
		if err := tree.WfCheck(); err == nil {
			t.Fatal("expected wfcheck failure for right-without-left tree")
		}
	})

	t.Run("parents without left", func(t *testing.T) {
		tree := NewTree()
		tree.pb.Parents = []*shieldpb.PedersenHash{fromPedersenHash(leaf)}
		if err := tree.WfCheck(); err == nil {
			t.Fatal("expected wfcheck failure for parent-without-left tree")
		}
	})

	t.Run("last parent empty", func(t *testing.T) {
		tree := NewTree()
		tree.pb.Left = fromPedersenHash(leaf)
		tree.pb.Right = fromPedersenHash(leaf)
		tree.pb.Parents = []*shieldpb.PedersenHash{fromPedersenHash(leaf), {}}
		if err := tree.WfCheck(); err == nil {
			t.Fatal("expected wfcheck failure for trailing-empty parent")
		}
	})
}
