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

## Pedersen hash implementation — Path A (CGO + librustzcash)

**Decision (2026-05-19)**: build-tag-gated CGO bindings to a C-ABI build
of the Rust `librustzcash` crate. Default builds stay pure-Go and the
shielded tests skip; `-tags=sapling` opt-in builds link the native code
and the tests must pass.

**Rust source**: git submodule `third_party/librustzcash` →
[`tronprotocol/librustzcash`](https://github.com/tronprotocol/librustzcash)
branch `release_vm_zksnarks_4.0`. Same upstream `tronprotocol/zksnark-java-sdk`
pulls in via its own submodule, so we match java-tron exactly.

On fresh clone:

```
git submodule update --init --recursive
make zksnark-deps          # cargo build --release inside the submodule
make gtron-sapling         # CGO_ENABLED=1 go build -tags=sapling
```

### File layout

```
core/zksnark/
├── pedersen.go         # types, ErrPedersenUnimplemented, EmptyRoots
├── pedersen_stub.go    # //go:build !sapling — Combine/Uncommitted return ErrPedersenUnimplemented
├── pedersen_cgo.go     # //go:build sapling   — cgo calls into libzksnark_capi
├── zksnark_capi.h      # C header declaring zksnark_merkle_hash + zksnark_tree_uncommitted
├── tree.go             # IncrementalMerkleTree (Append / Root / WfCheck / …)
└── testdata/           # 4 java-tron JSON vectors
```

### C ABI surface

The upstream `third_party/librustzcash/librustzcash/include/librustzcash.h`
wraps its declarations in `extern "C" {` *without* a `__cplusplus` guard —
so it isn't C-callable as-is. We re-declare just the two symbols we need
in our own `zksnark_capi.h` with proper guards. Upstream signatures
([source](https://github.com/tronprotocol/librustzcash/blob/release_vm_zksnarks_4.0/librustzcash/include/librustzcash.h)):

```c
void librustzcash_merkle_hash(size_t depth,
                              const unsigned char *a,
                              const unsigned char *b,
                              unsigned char *result);
void librustzcash_tree_uncommitted(unsigned char *result);
```

Buffers are 32 bytes. `depth` must not exceed 62 (we use ≤ 32). Both
return `void` — neither flags errors.

### Build flow

| Step | Status |
|---|---|
| `core/zksnark/pedersen_cgo.go` declares cgo directives + calls | ✅ landed |
| `core/zksnark/zksnark_capi.h` declares the two C functions | ✅ landed |
| `make zksnark-deps` placeholder that prints required steps | ✅ landed |
| `make gtron-sapling` (`CGO_ENABLED=1 go build -tags=sapling`) | ✅ landed; needs lib to actually link |
| **Pick Rust source location** (submodule / vendor / external) | open |
| **Vendor / pull the Rust crate** + write Cargo C-ABI shim | open |
| **CI: install Rust toolchain + run zksnark-deps + run sapling tests** | open |

Without the Rust crate landed, `make gtron-sapling` will fail to link
(`-lzksnark_capi: not found`). That's the expected error path; default
builds are unaffected.

### Toolchain status (2026-05-19 verified)

The crate is from 2019-era Rust (`rand = "0.4"`, `blake2-rfc` pinned to
a specific git rev, `bellman`/`pairing` as path deps). Stable Rust 1.95.0
builds it with 15 warnings (mostly `shared reference to mutable static`
on `SAPLING_*_PARAMS` — internal Rust API drift, not a behavior change).
Build time: ~21s release.

Output artifacts under `third_party/librustzcash/target/release/`:

- `librustzcash.a` ≈ 2.4 MB (static lib used by cgo)
- `librustzcash.dylib` ≈ 944 KB (dynamic lib)

### Verification (2026-05-19)

All 5 tests pass under `CGO_ENABLED=1 go test -tags=sapling`:

| Test | What it checks |
|---|---|
| `TestEmptyRootsVector` | empty-tree root at each depth d ∈ [0, 32] matches the 33-entry java-tron vector |
| `TestAppendCommitmentsVector` | Incremental tree at DEPTH=4 with 16 commitments: every intermediate root matches `merkle_roots_sapling.json` (commitments reversed before insert — matches `ByteUtil.reverse` in java's `MerkleTreeTest`) |
| `TestCombineKnownDepth25` | `combine(25, a, b) = 61a50a55…` from `PedersenHashCapsule.main()` |
| `TestProtoRoundTrip` | `IncrementalMerkleTree` proto wrapper preserves left/right/parents byte-for-byte through marshal+unmarshal |
| `TestWfCheckCatchesBadShapes` | `WfCheck` rejects the three canonical non-canonical proto shapes |

Default builds (no `-tags=sapling`) still skip the vector tests via
`errors.Is(err, ErrPedersenUnimplemented)`.



java-tron calls `JLibrustzcash.librustzcashMerkleHash`/`librustzcashTreeUncommitted` via the [zksnark-java-sdk](https://github.com/tronprotocol/zksnark-java-sdk) JAR, which bundles a JNI binary (`libzksnarkjni.so` / `.jnilib`) wrapping the Rust `librustzcash` crate. The JNI symbols are not C-ABI callable from CGO — they require a JVM `JNIEnv*`. The Rust crate behind the JNI is **the** source of truth; values must match its output byte-for-byte.

gtron's Makefile defaults to `CGO_ENABLED=0` ([Makefile:11](../../Makefile:11)) so the build runs anywhere without a C toolchain. Three feasible paths:

### Path A — CGO + librustzcash (exact parity)

Wire a non-JNI C ABI build of the same Rust crate java-tron uses. Add `//go:build sapling` tag; `gtron` builds without it lose shielded Merkle support. Sub-paths for Rust source:

| Sub-path | Trade-off |
|---|---|
| Git submodule → `tronprotocol/zksnark-java-sdk` | Pinned commit; matches java-tron exactly; `git clone --recursive` required |
| Vendor snapshot under `vendor/zksnark-capi/` | Single repo; +1–2k LOC Rust; sync to upstream manual |
| External cargo install + env-var path | Cleanest repo; CI/dev gate is high |

All three need Rust toolchain in CI. Setup cost ~2–3 days.

### Path B — Pure-Go via jadeydi/jubjub

Found in ecosystem scan (`pkg.go.dev` search): [`github.com/jadeydi/jubjub`](https://github.com/jadeydi/jubjub) (also the MixinNetwork fork) ships a pure-Go Jubjub curve and a Sapling-shaped Pedersen hash primitive:

- Personalization `"Zcash_PH"` (Sapling spec)
- 3-bit Bowe-Hopwood encoding with 63 chunks/generator, 5 generators
- Returns a `JubjubPoint` (need `.x` for the 32-byte hash output)

What's still missing if we adopt this:

1. **Sapling `MerkleCRH^Sapling(depth, left, right)` wrapper** — encodes `(MERKLE_DEPTH - 1 - depth)` as 6 bits, concatenates with `left[0..255] || right[0..255]`, runs Pedersen, extracts the x-coordinate. ~50 LOC.
2. **`Uncommitted^Sapling` constant** — the 32-byte Jubjub representation of integer 1 (`repr_J(1)`).
3. **Parity verification** — validate against our 16+33 vectors. If any byte disagrees, fall back to Path A.

Risks:
- jadeydi/jubjub: 24 commits total, latest activity 2021. Unmaintained.
- No public Sapling test-vector validation. Subtle EC arithmetic bug could pass our small vector set and break on production input.
- Crypto code we ship would become gtron's responsibility forever.

Setup cost ~1–2 days for the wrapper + validation. Wins only if vectors pass.

### Path C — Operator bypass flag `--unsafe.skip-shielded-root-check`

Default off; loud startup warning; logs every skipped check with block/tx/anchor. Plan-compliant tactical unblock for Nile sync; not a replacement for (A) or (B).

### Path D — Defer shielded parity

Drop this plan from the roadmap until CGO/build-policy priorities are clearer. Slice 1 deliverables stay (audit doc + interface + failing tests) as the eventual landing pad.

The decision is **escalated to the user**. Slice 2 begins only once chosen.

## Ecosystem scan — references

- [`zcash/librustzcash`](https://github.com/zcash/librustzcash) — official Rust workspace; the Pedersen hash + MerkleCRH live here. Source of all test vectors. No public Go binding.
- [`tronprotocol/zksnark-java-sdk`](https://github.com/tronprotocol/zksnark-java-sdk) — java-tron's bundled JNI wrapper; ships `libzksnarkjni.{so,jnilib}` for linux64/osx64/aarch64. Internal Rust source presumed forked from `zcash/librustzcash`.
- [`jadeydi/jubjub`](https://github.com/jadeydi/jubjub) ([MixinNetwork fork](https://github.com/MixinNetwork/jubjub)) — pure-Go Jubjub + Sapling-shaped Pedersen primitive. No MerkleCRH wrapper. Last commit 2021.
- [`gtank/jubjub`](https://github.com/gtank/jubjub) — pure-Go Jubjub for note decryption. No Pedersen hash exposed. Last commit January 2021.
- Other Sapling-using chains (Penumbra, Namada, Aleo, Aztec) all keep crypto on the Rust side; no pure-Go reference impl found.

## Out of scope for this plan

The shielded **proof verification** precompiles (`verifyMintProof` / `verifyTransferProof` / `verifyBurnProof` at addresses `0x01000003`–`0x01000005`) and `shieldedMerkleHash` at `0x01000006` are separate axes that also depend on librustzcash. They currently return java-tron's failure payload ([vm/precompile_tron.go:380–437](../../vm/precompile_tron.go:380)). Wiring them is a follow-on plan, not part of Merkle-root parity.

## Test vectors

Copied from `java-tron/framework/src/test/resources/json/` into `core/zksnark/testdata/`:

| File | Shape | Purpose |
|---|---|---|
| `merkle_roots_empty_sapling.json` | 33 hex strings | Empty tree root at each depth d ∈ [0, 32]. `empty[0]` = `librustzcashTreeUncommitted`. `empty[d]` = `combine(empty[d-1], empty[d-1], d-1)`. |
| `merkle_commitments_sapling.json` | 16 hex strings | Note commitments to append in order. |
| `merkle_roots_sapling.json` | 16 hex strings | Root of the tree after appending commitments[0..i] (DEPTH = 32). |
| `merkle_path_sapling.json` | 122 lines | Merkle authentication paths after each append (consumed when wallet path queries land). |

Vector tests landing in this slice **must fail** without a Pedersen implementation. That failure is the Slice 1 contract — it proves the test wiring works and the expected interface is well-defined.
