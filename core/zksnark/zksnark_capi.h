// SPDX-License-Identifier: MIT
//
// C ABI surface for the Sapling Pedersen primitives that gtron consumes
// from the Rust `librustzcash` crate (the same crate java-tron bundles
// via `tronprotocol/zksnark-java-sdk`'s JNI shim).
//
// The Rust library is expected to expose two C-callable functions with
// the signatures below. They mirror librustzcash's internal helpers
// `librustzcash_merkle_hash` and `librustzcash_tree_uncommitted`.
//
// All buffers are 32 bytes, little-endian Jubjub field encoding to match
// the librustzcash output exactly. `depth` is `uint64_t` to match the
// upstream `size_t`-based ABI on 64-bit targets.
//
// See docs/dev/shielded-merkle-audit.md for the build/sourcing plan
// (submodule / vendor / external — TBD).

#ifndef GTRON_ZKSNARK_CAPI_H
#define GTRON_ZKSNARK_CAPI_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// zksnark_merkle_hash computes the Sapling MerkleCRH:
//   result := MerkleCRH^Sapling(depth, left, right)
// `a` and `b` point to the 32-byte left/right node contents. `result`
// must point to a writable 32-byte buffer. The function returns 0 on
// success and non-zero on validation failure (rejected field element).
int32_t zksnark_merkle_hash(uint64_t depth,
                            const uint8_t *a,
                            const uint8_t *b,
                            uint8_t *result);

// zksnark_tree_uncommitted writes the 32-byte Uncommitted^Sapling
// constant to `result`. The Sapling spec defines this as repr_J(1).
void zksnark_tree_uncommitted(uint8_t *result);

#ifdef __cplusplus
}
#endif

#endif // GTRON_ZKSNARK_CAPI_H
