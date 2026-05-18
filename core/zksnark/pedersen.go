// Package zksnark mirrors java-tron's `org.tron.common.zksnark` surface for
// Sapling shielded transactions: Pedersen hash primitives and the incremental
// Merkle tree state machine that tracks commitment roots.
//
// The Pedersen hash backend is build-tag-gated:
//
//   - default (`!sapling`): pedersen_stub.go returns ErrPedersenUnimplemented
//     so the package compiles everywhere and shielded tests skip.
//   - `-tags=sapling`: pedersen_cgo.go links against a C-ABI build of the
//     `librustzcash` Rust crate (the same crate java-tron's zksnark-java-sdk
//     bundles via JNI). Requires CGO_ENABLED=1 and a built static lib —
//     see docs/dev/shielded-merkle-audit.md.
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

// ErrPedersenUnimplemented is returned by the stub backend (no `-tags=sapling`)
// and by EmptyRoots when its primitives fail. Downstream callers wrap it with
// their own context.
var ErrPedersenUnimplemented = errors.New("zksnark: Pedersen hash backend not built (rebuild with -tags=sapling and a librustzcash static lib; see docs/dev/shielded-merkle-audit.md)")

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
