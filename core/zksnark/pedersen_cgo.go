//go:build sapling

package zksnark

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -L${SRCDIR}/../../third_party/librustzcash/target/release -lrustzcash -ldl -lm

#include <stddef.h>
#include "zksnark_capi.h"
*/
import "C"

import "unsafe"

// Combine wraps librustzcash_merkle_hash. The Rust crate at
// third_party/librustzcash must be built via `make zksnark-deps` (or
// `cargo build --release` inside third_party/librustzcash/librustzcash)
// before this can link.
func Combine(depth int, left, right PedersenHash) (PedersenHash, error) {
	var out PedersenHash
	C.librustzcash_merkle_hash(
		C.size_t(depth),
		(*C.uchar)(unsafe.Pointer(&left[0])),
		(*C.uchar)(unsafe.Pointer(&right[0])),
		(*C.uchar)(unsafe.Pointer(&out[0])),
	)
	return out, nil
}

// Uncommitted wraps librustzcash_tree_uncommitted.
func Uncommitted() (PedersenHash, error) {
	var out PedersenHash
	C.librustzcash_tree_uncommitted((*C.uchar)(unsafe.Pointer(&out[0])))
	return out, nil
}
