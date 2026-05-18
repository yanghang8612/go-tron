// Package zksnark mirrors java-tron's `org.tron.common.zksnark` surface for
// Sapling shielded transactions: Pedersen hash primitives and the incremental
// Merkle tree state machine that tracks commitment roots.
//
// Implementation status: the Pedersen hash backend is unimplemented in this
// slice. See docs/dev/shielded-merkle-audit.md for the path choice that gates
// Slice 2. Until a backend is wired, Combine and Uncommitted both return
// ErrPedersenUnimplemented; downstream callers (IncrementalMerkleTree.Append /
// Root, EmptyRoots) propagate that error.
package zksnark

import "errors"

// Depth is the Sapling commitment-tree depth used by java-tron. Mirrors
// IncrementalMerkleTreeContainer.DEPTH.
const Depth = 32

// PedersenHash is the 32-byte content of a Pedersen tree node, matching the
// `bytes content` field of `protocol.PedersenHash` on the wire.
//
// A "present" hash has a non-zero byte slice; in proto terms that is a
// length-32 content. Slots in the incremental tree that are not yet filled
// serialize as an empty `content` (length 0).
type PedersenHash [32]byte

// ErrPedersenUnimplemented is returned by Combine / Uncommitted until a
// backend lands. Downstream callers wrap it with their own context.
var ErrPedersenUnimplemented = errors.New("zksnark: Pedersen hash backend not implemented (see docs/dev/shielded-merkle-audit.md)")

// Combine returns librustzcashMerkleHash(depth, left, right). Mirrors
// PedersenHashCapsule.combine.
//
// The reference implementation is the Rust `librustzcash` crate that
// java-tron binds via JNI; there is no Java-side computation. Matching it
// requires either binding the same crate via cgo or porting the
// Jubjub/Pedersen primitives to Go. Until one of those lands this is a
// stub returning ErrPedersenUnimplemented.
func Combine(depth int, left, right PedersenHash) (PedersenHash, error) {
	_ = depth
	_ = left
	_ = right
	return PedersenHash{}, ErrPedersenUnimplemented
}

// Uncommitted returns the librustzcashTreeUncommitted constant — the value
// of an empty leaf in the Sapling commitment tree.
//
// Same stub status as Combine.
func Uncommitted() (PedersenHash, error) {
	return PedersenHash{}, ErrPedersenUnimplemented
}

// EmptyRoots returns the array of empty subtree roots at each depth
// d ∈ [0, Depth]. Mirrors EmptyMerkleRoots:
//
//	empty[0] = Uncommitted()
//	empty[d] = Combine(empty[d-1], empty[d-1], d-1)
//
// Returns the first error encountered; callers should treat any non-nil
// error as a missing backend.
func EmptyRoots() ([Depth + 1]PedersenHash, error) {
	var out [Depth + 1]PedersenHash
	u, err := Uncommitted()
	if err != nil {
		return out, err
	}
	out[0] = u
	for d := 1; d <= Depth; d++ {
		c, err := Combine(d-1, out[d-1], out[d-1])
		if err != nil {
			return out, err
		}
		out[d] = c
	}
	return out, nil
}
