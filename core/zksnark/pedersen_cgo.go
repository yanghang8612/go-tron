//go:build sapling

package zksnark

/*
#cgo CFLAGS: -I${SRCDIR}
#cgo LDFLAGS: -lzksnark_capi

#include <stdint.h>
#include "zksnark_capi.h"
*/
import "C"

import (
	"errors"
	"unsafe"
)

// Combine wraps zksnark_merkle_hash from librustzcash. See zksnark_capi.h for
// the C ABI contract. The Rust crate must be built into a static or shared
// `libzksnark_capi` that exports the symbols declared in the header.
func Combine(depth int, left, right PedersenHash) (PedersenHash, error) {
	var out PedersenHash
	rc := C.zksnark_merkle_hash(
		C.uint64_t(depth),
		(*C.uint8_t)(unsafe.Pointer(&left[0])),
		(*C.uint8_t)(unsafe.Pointer(&right[0])),
		(*C.uint8_t)(unsafe.Pointer(&out[0])),
	)
	if rc != 0 {
		return PedersenHash{}, errors.New("zksnark: librustzcash merkle_hash rejected input")
	}
	return out, nil
}

// Uncommitted wraps zksnark_tree_uncommitted from librustzcash.
func Uncommitted() (PedersenHash, error) {
	var out PedersenHash
	C.zksnark_tree_uncommitted((*C.uint8_t)(unsafe.Pointer(&out[0])))
	return out, nil
}
