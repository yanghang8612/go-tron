# Shielded Merkle-tree parity audit

Slice 1 deliverable from
`docs/superpowers/plans/2026-05-18-shielded-merkle-tree-parity.md`. Records the
authoritative java-tron sources we are matching, the proto types, and what
gtron currently has (or doesn't have) on the same surface.

## java-tron sources

| java-tron file | Purpose |
|---|---|
| [chainbase/.../common/zksnark/IncrementalMerkleTreeContainer.java](../../../tron/java-tron/chainbase/src/main/java/org/tron/common/zksnark/IncrementalMerkleTreeContainer.java) | The Sapling incremental tree state machine. `append`, `root`, `path`, `size`, `wfcheck`, `nextDepth`, `getMerkleTreeKey`. Fixed `DEPTH = 32`. `EmptyMerkleRoots` builds the per-depth empty cache eagerly via `PedersenHashCapsule.combine`. |
| [chainbase/.../common/zksnark/MerkleContainer.java](../../../tron/java-tron/chainbase/src/main/java/org/tron/common/zksnark/MerkleContainer.java) | Orchestrator that holds `LAST_TREE` (best) and `CURRENT_TREE` (mutable per-block) under `IncrementalMerkleTreeStore`, plus the root-keyed entries that back `merkleRootExist(anchor)`. `resetCurrentMerkleTree`, `saveCmIntoMerkleTree`, `saveCurrentMerkleTreeAsBestMerkleTree`. |
| [chainbase/.../capsule/PedersenHashCapsule.java](../../../tron/java-tron/chainbase/src/main/java/org/tron/core/capsule/PedersenHashCapsule.java) | Thin wrapper around `JLibrustzcash.librustzcashMerkleHash` (depth-keyed Pedersen combine) and `librustzcashTreeUncommitted` (the empty-leaf constant). Both call into Rust. |
| [chainbase/.../capsule/IncrementalMerkleTreeCapsule.java](../../../tron/java-tron/chainbase/src/main/java/org/tron/core/capsule/IncrementalMerkleTreeCapsule.java) | Proto carrier for `ShieldContract.IncrementalMerkleTree` (left, right, parents). Cleared parents become an empty PedersenHash, not removed. |
| [chainbase/.../core/store/IncrementalMerkleTreeStore.java](../../../tron/java-tron/chainbase/src/main/java/org/tron/core/store/IncrementalMerkleTreeStore.java) | Pebble-backed store: keyed by `LAST_TREE`, `CURRENT_TREE`, or a 32-byte merkle root. |
| [actuator/.../actuator/ShieldedTransferActuator.java](../../../tron/java-tron/actuator/src/main/java/org/tron/core/actuator/ShieldedTransferActuator.java) | Calls `merkleContainer.merkleRootExist(spend.anchor)` in `validate`, and `merkleContainer.saveCmIntoMerkleTree(currentTree, cm)` for every receive cm in `executeShielded`. Persists the mutated `currentTree` back into the store. |
| `chainbase/.../core/db/Manager.java::processBlock` | Calls `merkleContainer.resetCurrentMerkleTree()` before tx execution and `merkleContainer.saveCurrentMerkleTreeAsBestMerkleTree(blockNum)` after. |

## Proto surface (already shared)

`proto/core/contract/shield_contract.proto` defines:

- `PedersenHash { bytes content }` — 32-byte content, empty when "not present"
- `IncrementalMerkleTree { PedersenHash left; PedersenHash right; repeated PedersenHash parents }`
- `IncrementalMerkleVoucher { IncrementalMerkleTree tree; repeated PedersenHash filled; IncrementalMerkleTree cursor; int64 cursor_depth; bytes rt; OutputPoint output_point }`
- `MerklePath { repeated AuthenticationPath authentication_paths; repeated bool index; bytes rt }`

These are imported verbatim from java-tron. We must NOT change them — wire-format identity with java-tron requires the same proto bytes.

## gtron baseline (pre-implementation)

| Surface | Current state |
|---|---|
| `actuator/shielded_transfer.go::Validate` | Calls `rawdb.HasIncrMerkleTree(ctx.DB, spend.Anchor)` — but nothing populates the store, so every shielded spend after Nile activation block 1,628,391 fails with `"Rt is invalid."` |
| `actuator/shielded_transfer.go::Execute` | Calls `rawdb.AppendNoteCommitment` only. No Merkle tree state mutation. No CURRENT_TREE write. |
| `core/rawdb/accessors_shielded.go` | Has `WriteIncrMerkleTree`/`ReadIncrMerkleTree`/`HasIncrMerkleTree`/`DeleteIncrMerkleTree` keyed by 32-byte root. No `LAST_TREE`/`CURRENT_TREE` accessors. No block-number index. |
| `core/types/merkle.go` | SHA-256 binary tree for `tx_trie_root` — NOT the Sapling Pedersen tree. Unrelated; kept for genesis-hash parity. |
| `vm/precompile_tron.go` | `shieldedMerkleHash`, `verifyMintProof`, `verifyTransferProof`, `verifyBurnProof` all return java-tron's failure payload until librustzcash equivalents are wired (their TODO comments say so verbatim). |
| `core/zksnark/` | **New** — landing point for this work. |

## Pedersen hash implementation — deferred

java-tron calls `JLibrustzcash.librustzcashMerkleHash`/`librustzcashTreeUncommitted` via JNI into the Rust `librustzcash` crate. The native function is **the** source of truth — there is no Java implementation; the value must match the Rust output byte-for-byte.

gtron's Makefile defaults to `CGO_ENABLED=0` (Makefile:11) so the build runs anywhere without a C toolchain. Wiring librustzcash adds a Rust toolchain dependency and a CGO requirement. Three feasible paths:

1. **CGO + librustzcash** (preferred for exact parity). Add `cgo` build tag; `gtron` builds without it lose shielded-Merkle support and refuse shielded consensus replay. Requires Rust toolchain in CI.
2. **Pure-Go Sapling/Jubjub/Pedersen port**. Multi-week cryptography effort. Risk: subtle field-arithmetic bugs producing wrong roots; vectors-only validation insufficient unless we cover every code path.
3. **Operator bypass flag `--unsafe.skip-shielded-root-check`**. Default off; logs every skipped check. Plan-compliant temporary path; not a replacement for (1) or (2).

This decision is **escalated to the user**. Slice 2 begins only once chosen.

## Test vectors

Copied from `java-tron/framework/src/test/resources/json/` into `core/zksnark/testdata/`:

| File | Shape | Purpose |
|---|---|---|
| `merkle_roots_empty_sapling.json` | 33 hex strings | Empty tree root at each depth d ∈ [0, 32]. `empty[0]` = `librustzcashTreeUncommitted`. `empty[d]` = `combine(empty[d-1], empty[d-1], d-1)`. |
| `merkle_commitments_sapling.json` | 16 hex strings | Note commitments to append in order. |
| `merkle_roots_sapling.json` | 16 hex strings | Root of the tree after appending commitments[0..i] (DEPTH = 32). |
| `merkle_path_sapling.json` | 122 lines | Merkle authentication paths after each append (consumed when wallet path queries land). |

Vector tests landing in this slice **must fail** without a Pedersen implementation. That failure is the Slice 1 contract — it proves the test wiring works and the expected interface is well-defined.
