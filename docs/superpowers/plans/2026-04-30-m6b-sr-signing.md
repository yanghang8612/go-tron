# M6b SR-side PBFT signing — Plan

**Spec:** [2026-04-30-m6b-sr-signing-design.md](../specs/2026-04-30-m6b-sr-signing-design.md)
**Slice:** 1 of 2 (scaffolding: key + builders + no-op hook)

## Slice 1 — Scaffolding

- [ ] **`net/pbft_producer.go`**
    - [ ] `PbftProducer` struct holding `chain`, `db`, `server`, `sync`, `srKey`, `srAddr`.
    - [ ] `NewPbftProducer(chain, db, server, sync, key)` constructor.
    - [ ] `signPbftRaw(raw, key)` internal helper: marshal raw → SHA-256 → `crypto.Sign` → wrap in `PBFTMessage` → marshal.
    - [ ] `BuildBlockPrePrepareMsg(block, epoch) ([]byte, error)`: msg_type=PREPREPARE, data_type=BLOCK, view_n=block.Number(), data=block.ID().Hash[:].
    - [ ] `BuildPrepareMsg(parentRaw) ([]byte, error)`: clone parentRaw fields, msg_type=PREPARE.
    - [ ] `BuildCommitMsg(parentRaw) ([]byte, error)`: clone parentRaw fields, msg_type=COMMIT.
    - [ ] `allowPBFT()` helper (matches `pbft_handler.allowPBFT` pattern).
    - [ ] `isLocalSR()` helper: `srAddr` ∈ current ∪ previous shuffled witnesses.
    - [ ] `OnBlockApplied(block)` no-op: gate on `allowPBFT && !syncing && isLocalSR`, then `log.Printf` and return.

- [ ] **`net/pbft_producer_test.go`**
    - [ ] `TestBuildBlockPrePrepareMsg_RoundTrip` — proto.Unmarshal → fields match; `pbftSigToAddress` recovers `crypto.PubkeyToAddress(&key.PublicKey)`.
    - [ ] `TestBuildPrepareMsg_DerivesFromParent` — fields cloned, MsgType=PREPARE, sig recovers SR addr.
    - [ ] `TestBuildCommitMsg_DerivesFromParent` — same with MsgType=COMMIT.
    - [ ] `TestBuildPrepareMsg_DifferentSignatureFromPrePrepare` — sanity that flipping msg_type produces a different signature.
    - [ ] `TestOnBlockApplied_NoOp_DoesNotPanic` — call hook with a synthetic block; no DB writes, no server sends.

- [ ] **`cmd/gtron/main.go`**
    - [ ] In the `if ctx.Bool("witness")` branch, after `producer.New(...)` + before `stack.RegisterLifecycle(prod)`, add:
        - [ ] `pbftProducer := tnet.NewPbftProducer(bc, db, p2pServer, syncService, key)`
        - [ ] `bc.AddBlockHook(pbftProducer.OnBlockApplied)`
    - [ ] No registration in non-witness mode.

- [ ] **Verification**
    - [ ] `make test` green across all packages.
    - [ ] `core/blockchain.go` diff = 0.
    - [ ] `net/pbft_handler.go` diff = 0.
    - [ ] `net/pbft_data_sync.go` diff = 0.
    - [ ] Commit subject: `feat(consensus,net): SR-side PBFT signing scaffolding (M6b slice 1)`. GPG sign with `E3673E008F6D506E`.

## Out of slice 1 (deferred to slice 2)

- Three-phase state-machine producer side: on receiving / locally generating a PREPREPARE, derive PREPARE for each local SR; on PREPARE quorum, derive COMMIT.
- Peer broadcast (`p2p.Server.Peers().Send(MsgPbftMsg, payload)`) and self-loop into `PbftHandler.onPrePrepare/onPrepare/onCommit`.
- `BuildSrlPrePrepareMsg(currentWitnesses, epoch)` triggered at maintenance boundary.
- `Epoch` derivation from `DynamicProperties.MaintenanceTimeInterval()` (slice 1 takes epoch as explicit arg).
- Multi-SR-key support in `cmd/gtron/main.go` (currently single `--witness.key`).
- Live cross-impl byte-level verification against running java-tron, results recorded in `docs/dev/p2p-interop-status.md`.
- Decision on whether to fold `PbftProducer` into `PbftHandler` as a `PbftCoordinator` (probably yes — sender path naturally lives in the same callbacks).
