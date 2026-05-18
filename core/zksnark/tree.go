package zksnark

import (
	"errors"

	shieldpb "github.com/tronprotocol/go-tron/proto/core/contract"
)

// IncrementalMerkleTree is a Sapling Pedersen-hash incremental commitment
// tree. Mirrors java-tron's IncrementalMerkleTreeContainer / Capsule pair:
// the proto carries the full state (left, right, parents) and the Go type
// holds a pointer to it so we can mutate in place and re-serialize.
//
// Empty slots serialize as a PedersenHash with empty content (`len == 0`),
// matching the proto default and java's `isPresent` semantics. We keep the
// proto as the source of truth so round-trips through rawdb are byte-stable.
type IncrementalMerkleTree struct {
	pb *shieldpb.IncrementalMerkleTree
}

// NewTree returns an empty tree.
func NewTree() *IncrementalMerkleTree {
	return &IncrementalMerkleTree{pb: &shieldpb.IncrementalMerkleTree{}}
}

// FromProto wraps an existing proto (may be nil, in which case an empty tree
// is returned).
func FromProto(pb *shieldpb.IncrementalMerkleTree) *IncrementalMerkleTree {
	if pb == nil {
		return NewTree()
	}
	return &IncrementalMerkleTree{pb: pb}
}

// Proto returns the underlying proto for serialization. The returned pointer
// aliases internal state — callers MUST NOT mutate it directly.
func (t *IncrementalMerkleTree) Proto() *shieldpb.IncrementalMerkleTree {
	return t.pb
}

func (t *IncrementalMerkleTree) leftIsPresent() bool {
	return t.pb.GetLeft() != nil && len(t.pb.GetLeft().GetContent()) > 0
}

func (t *IncrementalMerkleTree) rightIsPresent() bool {
	return t.pb.GetRight() != nil && len(t.pb.GetRight().GetContent()) > 0
}

func parentIsPresent(p *shieldpb.PedersenHash) bool {
	return p != nil && len(p.GetContent()) > 0
}

func toPedersenHash(p *shieldpb.PedersenHash) (PedersenHash, bool) {
	var h PedersenHash
	if p == nil || len(p.GetContent()) != Depth {
		return h, false
	}
	copy(h[:], p.GetContent())
	return h, true
}

func fromPedersenHash(h PedersenHash) *shieldpb.PedersenHash {
	return &shieldpb.PedersenHash{Content: append([]byte(nil), h[:]...)}
}

// WfCheck validates the tree's well-formedness invariants. Mirrors
// IncrementalMerkleTreeContainer.wfcheck.
func (t *IncrementalMerkleTree) WfCheck() error {
	if len(t.pb.GetParents()) >= Depth {
		return errors.New("zksnark: tree has too many parents")
	}
	if n := len(t.pb.GetParents()); n > 0 {
		if !parentIsPresent(t.pb.GetParents()[n-1]) {
			return errors.New("zksnark: tree has non-canonical representation of parent")
		}
	}
	if !t.leftIsPresent() && t.rightIsPresent() {
		return errors.New("zksnark: tree has non-canonical representation; right should not exist")
	}
	if !t.leftIsPresent() && len(t.pb.GetParents()) > 0 {
		return errors.New("zksnark: tree has non-canonical representation; parents should be empty")
	}
	return nil
}

// Size returns the number of leaves represented by the tree. Mirrors
// IncrementalMerkleTreeContainer.size.
func (t *IncrementalMerkleTree) Size() int {
	n := 0
	if t.leftIsPresent() {
		n++
	}
	if t.rightIsPresent() {
		n++
	}
	for i, p := range t.pb.GetParents() {
		if parentIsPresent(p) {
			n += 1 << (i + 1)
		}
	}
	return n
}

// IsComplete reports whether the tree is full at Depth. Mirrors
// IncrementalMerkleTreeContainer.isComplete.
func (t *IncrementalMerkleTree) IsComplete() bool {
	if !t.leftIsPresent() || !t.rightIsPresent() {
		return false
	}
	if len(t.pb.GetParents()) != Depth-1 {
		return false
	}
	for _, p := range t.pb.GetParents() {
		if !parentIsPresent(p) {
			return false
		}
	}
	return true
}

// Append adds a new leaf. Mirrors IncrementalMerkleTreeContainer.append.
//
// Propagates errors from Combine, so until a Pedersen backend is wired this
// returns ErrPedersenUnimplemented once the tree fills its left+right slot
// and triggers a parent-level combine.
func (t *IncrementalMerkleTree) Append(leaf PedersenHash) error {
	if t.IsComplete() {
		return errors.New("zksnark: tree is full")
	}

	if !t.leftIsPresent() {
		t.pb.Left = fromPedersenHash(leaf)
		return nil
	}
	if !t.rightIsPresent() {
		t.pb.Right = fromPedersenHash(leaf)
		return nil
	}

	// Both bottom slots full — combine and bubble up.
	left, _ := toPedersenHash(t.pb.GetLeft())
	right, _ := toPedersenHash(t.pb.GetRight())
	combined, err := Combine(0, left, right)
	if err != nil {
		return err
	}

	t.pb.Left = fromPedersenHash(leaf)
	t.pb.Right = nil

	for i := 0; i < Depth; i++ {
		if i < len(t.pb.GetParents()) {
			parent := t.pb.GetParents()[i]
			if parentIsPresent(parent) {
				p, _ := toPedersenHash(parent)
				next, err := Combine(i+1, p, combined)
				if err != nil {
					return err
				}
				combined = next
				t.pb.Parents[i] = &shieldpb.PedersenHash{}
			} else {
				t.pb.Parents[i] = fromPedersenHash(combined)
				return nil
			}
		} else {
			t.pb.Parents = append(t.pb.Parents, fromPedersenHash(combined))
			return nil
		}
	}
	return errors.New("zksnark: tree is full")
}

// Root returns the Sapling commitment-tree root. Mirrors
// IncrementalMerkleTreeContainer.root() — extends the tree with empty
// fillers up to Depth.
//
// Propagates Combine errors.
func (t *IncrementalMerkleTree) Root() (PedersenHash, error) {
	empties, err := EmptyRoots()
	if err != nil {
		return PedersenHash{}, err
	}

	var left, right PedersenHash
	if l, ok := toPedersenHash(t.pb.GetLeft()); ok {
		left = l
	} else {
		left = empties[0]
	}
	if r, ok := toPedersenHash(t.pb.GetRight()); ok {
		right = r
	} else {
		right = empties[0]
	}
	root, err := Combine(0, left, right)
	if err != nil {
		return PedersenHash{}, err
	}

	d := 1
	for _, parent := range t.pb.GetParents() {
		if parentIsPresent(parent) {
			p, _ := toPedersenHash(parent)
			root, err = Combine(d, p, root)
			if err != nil {
				return PedersenHash{}, err
			}
		} else {
			root, err = Combine(d, root, empties[d])
			if err != nil {
				return PedersenHash{}, err
			}
		}
		d++
	}
	for d < Depth {
		root, err = Combine(d, root, empties[d])
		if err != nil {
			return PedersenHash{}, err
		}
		d++
	}
	return root, nil
}

// MerkleTreeKey returns Root() as a byte slice — the key used to index the
// tree in the IncrementalMerkleTreeStore. Mirrors
// IncrementalMerkleTreeContainer.getMerkleTreeKey.
func (t *IncrementalMerkleTree) MerkleTreeKey() ([]byte, error) {
	root, err := t.Root()
	if err != nil {
		return nil, err
	}
	out := make([]byte, len(root))
	copy(out, root[:])
	return out, nil
}
