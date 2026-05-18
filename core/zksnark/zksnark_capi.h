// SPDX-License-Identifier: MIT
//
// C ABI surface for the Sapling Pedersen primitives that gtron consumes
// from the Rust `librustzcash` crate (vendored as a git submodule under
// third_party/librustzcash, branch release_vm_zksnarks_4.0). The original
// header at third_party/librustzcash/librustzcash/include/librustzcash.h is
// wrapped in `extern "C" {` without a `__cplusplus` guard — not C-callable
// as-is — so we re-declare the two symbols we need with proper C linkage.
//
// All buffers are 32 bytes, little-endian Jubjub field encoding to match
// the librustzcash output exactly. `depth` is `size_t` matching the
// upstream signature; per librustzcash.h it must not exceed 62.
//
// See docs/dev/shielded-merkle-audit.md for the build + parity story.

#ifndef GTRON_ZKSNARK_CAPI_H
#define GTRON_ZKSNARK_CAPI_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

// librustzcash_merkle_hash computes the Sapling MerkleCRH:
//   result := MerkleCRH^Sapling(depth, a, b)
// `a` and `b` point to the 32-byte left/right node contents (must be valid
// scalars of BLS12-381). `result` must point to a writable 32-byte buffer.
// Upstream returns void — does not signal validation failure.
void librustzcash_merkle_hash(size_t depth,
                              const unsigned char *a,
                              const unsigned char *b,
                              unsigned char *result);

// librustzcash_tree_uncommitted writes the 32-byte Uncommitted^Sapling
// constant to `result`. The Sapling spec defines this as repr_J(1).
void librustzcash_tree_uncommitted(unsigned char *result);

#ifdef __cplusplus
}
#endif

#endif // GTRON_ZKSNARK_CAPI_H
