# Shielded Sapling Merkle-tree parity — plan

**Problem first observed:** 2026-05-18, Nile replay paused at block
`1,685,793` with:

```text
process block: tx 0: validate: Rt is invalid.
```

## Problem

Nile proposal `ALLOW_SHIELDED_TRANSACTION` activates shielded transfers at
block `1,628,391`. After that point, shielded spend descriptions validate
their Sapling anchor/root against the incremental Merkle-tree store.

go-tron currently checks the root:

- `actuator/shielded_transfer.go::Validate`
  calls `rawdb.HasIncrMerkleTree(ctx.DB, spend.Anchor)`.

But go-tron does not maintain the same Merkle-tree root store java-tron does:

- `actuator/shielded_transfer.go::Execute` writes nullifiers.
- It records receive note commitments with `rawdb.AppendNoteCommitment`.
- It does not append note commitments into a Sapling incremental Merkle tree.
- It does not reset the current tree at block start.
- It does not save the block's current tree as best and index it by root at
  block end.

java-tron behavior to mirror:

- `Manager.processBlock`: `merkleContainer.resetCurrentMerkleTree()` before
  executing block transactions.
- `ShieldedTransferActuator.executeShielded`: append each receive note
  commitment with `MerkleContainer.saveCmIntoMerkleTree`.
- `Manager.processBlock`: `merkleContainer.saveCurrentMerkleTreeAsBestMerkleTree`
  after executing transactions.
- `ShieldedTransferActuator.validate`: reject a spend when
  `merkleContainer.merkleRootExist(anchor)` is false.

## Non-goals

- Do not silently skip `Rt is invalid.` in consensus validation.
- Do not make shielded root validation weaker by default.
- Do not treat this as a P2P sync issue; peers are delivering blocks, local
  block execution is rejecting them.
- Do not hand-roll incompatible placeholder roots. The root bytes must match
  java-tron's Sapling Pedersen tree roots.

## Slice 1 — Java parity audit and vectors

- [ ] Record exact java-tron source references for:
  - `MerkleContainer`
  - `IncrementalMerkleTreeContainer`
  - `PedersenHashCapsule`
  - `ShieldedTransferActuator`
  - `Manager.processBlock`
- [ ] Import or reference Nile/java-tron test vectors:
  - `merkle_roots_sapling.json`
  - `merkle_path_sapling.json`
  - `merkle_roots_empty_sapling.json`
  - `merkle_commitments_sapling.json`
- [ ] Add Go tests that initially fail against these vectors:
  - empty Sapling root at each depth
  - append one commitment
  - append multiple commitments
  - generated root key matches java-tron
  - Merkle path generation matches java-tron, if RPC/API coverage needs it

## Slice 2 — Sapling Pedersen hash implementation

- [ ] Decide implementation path:
  - preferred: bind the same librustzcash primitives java-tron uses, with
    explicit build documentation and CI coverage where available
  - fallback: use a reviewed pure-Go Sapling/Jubjub/Pedersen implementation
    only if vector parity is proven
- [ ] Implement wrappers equivalent to java-tron:
  - `librustzcashTreeUncommitted`
  - `librustzcashMerkleHash(depth, left, right)`
- [ ] Keep `CGO_ENABLED=0` behavior explicit:
  - either shielded Merkle support is unavailable and node refuses shielded
    consensus replay
  - or the pure-Go path is used and vector-parity tests still pass
- [ ] Add tests proving hash parity against java-tron vectors.

## Slice 3 — Incremental Merkle tree container

- [ ] Add a Go container mirroring java-tron's
      `IncrementalMerkleTreeContainer` state machine.
- [ ] Preserve proto compatibility with
      `proto/core/contract/shield_contract.proto::IncrementalMerkleTree`.
- [ ] Implement:
  - empty tree construction
  - `wfcheck`
  - append note commitment
  - root calculation
  - tree key calculation
  - Merkle path generation, if needed by wallet APIs
- [ ] Add table tests covering:
  - left/right/parents transitions
  - append boundary behavior
  - max depth behavior
  - serialized proto round trip

## Slice 4 — Rawdb accessors and block-buffer safety

- [ ] Extend `core/rawdb/accessors_shielded.go` with java-tron equivalent
      keys for:
  - current tree (`CURRENT_TREE`)
  - last/best tree (`LAST_TREE`)
  - root-keyed tree entries
  - optional block-number to root index, matching java-tron's
    `MerkleTreeIndexStore`
- [ ] Ensure every write accepts the buffer-aware DB interface, not only the
      persistent Pebble store.
- [ ] Add rollback/reorg tests proving shielded Merkle writes are discarded
      on failed block apply and rewound on `switchFork`.

## Slice 5 — Block lifecycle integration

- [ ] In block application, before transaction execution, reset current
      shielded Merkle tree from best tree.
- [ ] In `ShieldedTransferActuator.Execute`, replace
      `rawdb.AppendNoteCommitment`-only behavior with:
  - append note commitment to the current incremental Merkle tree
  - persist current tree under `CURRENT_TREE`
  - preserve existing note commitment index storage if APIs/tests depend on it
- [ ] After transaction execution succeeds, save current tree as best and store
      it by root.
- [ ] If block execution fails, do not commit current/best/root writes.
- [ ] Keep `Rt is invalid.` validation unchanged once the tree store is
      correctly populated.

## Slice 6 — Existing datadir recovery

- [ ] Add a documented recovery path for nodes that already replayed past
      shielded activation without Merkle roots.
- [ ] Preferred recovery:
  - stop the node
  - reset/replay from just before Nile shielded activation block `1,628,391`
  - rebuild roots deterministically during block import
- [ ] Optional tool:
  - scan historical shielded receive commitments from local block data
  - rebuild and persist Merkle-tree roots without redownloading blocks
  - verify rebuilt root at known failing spend block before resuming sync
- [ ] Do not claim a patched binary alone fixes an old datadir unless roots
      are rebuilt.

## Slice 7 — Integration and interop tests

- [ ] Add a Nile replay fixture around:
  - proposal `27` activation at block `1,628,391`
  - first shielded receive after activation
  - failing spend at block `1,685,793`
- [ ] Add a java-tron comparison test:
  - feed the same note commitments
  - assert all intermediate root keys match
  - assert the spend anchor at `1,685,793` exists before validation
- [ ] Add a full block import test proving tx 0 in block `1,685,793` no
      longer fails with `Rt is invalid.`.
- [ ] Run:
  - `go test ./actuator ./core/rawdb ./core/... -count=1`
  - `make test`
  - `make gtron`
  - Nile live sync smoke test from pre-activation height through `1,685,793`

## Temporary bypass policy

If production sync must continue before the full Merkle implementation lands,
the only acceptable temporary bypass is an explicit unsafe operator flag, for
example:

```text
--unsafe.skip-shielded-root-check
```

Requirements for any bypass:

- [ ] Default is off.
- [ ] Logs a clear consensus-safety warning at startup.
- [ ] Logs every skipped root check with block/tx/anchor.
- [ ] Is not used in tests as the expected compatibility path.
- [ ] Is removed or kept outside production defaults after proper Merkle
      parity lands.

## Acceptance criteria

- [ ] go-tron validates shielded spend anchors using a populated Merkle root
      store, not a skipped check.
- [ ] Merkle roots match java-tron vectors byte-for-byte.
- [ ] Nile replay passes block `1,685,793`.
- [ ] Existing shielded-transfer tests still pass.
- [ ] Reorg and failed-apply paths do not leak Merkle writes.
- [ ] Operator documentation explains datadir recovery for nodes that synced
      without historical roots.
