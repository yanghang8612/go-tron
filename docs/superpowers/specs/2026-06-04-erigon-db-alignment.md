# Erigon Database Alignment Assessment

Date: 2026-06-04

Status: active assessment and roadmap

Scope: compare go-tron's current database, state, snapshot, pruning, archive,
and sync-storage architecture with Erigon's current database implementation.
The target is to import the Erigon ideas that materially improve sync speed,
hot-state size, historical-state size, and archive-node support while preserving
java-tron wire and consensus compatibility.

## External Baseline

Erigon source checked against `erigontech/erigon` main at:

```text
d78922b0a260d156627eeb5ca399e6b20546c1b6
d78922b 2026-06-04 [perf] execution: optimise engine_getPayload with max blobs 2.4X (#21615)
```

The current Erigon tree has moved from the older `erigon-lib/...` layout to:

| Erigon area | Current path |
| --- | --- |
| hot KV and temporal DB | `db/kv`, `db/kv/mdbx`, `db/kv/temporal` |
| state domains and aggregator | `db/state` |
| snapshots and block freezer | `db/snapshotsync`, `db/snapshotsync/freezeblocks` |
| segment compression | `db/seg` |
| hash/accessor indexes | `db/recsplit` |
| sorted ETL ingestion | `db/etl` |
| staged execution | `execution/stagedsync` |

Operator documentation also says Erigon 3 datadirs are split into hot
`chaindata` plus `snapshots/domain`, `snapshots/history`, `snapshots/idx`, and
`snapshots/accessor`, with four state domains: account, storage, code, and
commitment. The same docs state Erigon 3 supports `full`, `minimal`, `blocks`,
and `archive` prune modes, with archive retaining historical state.

## Erigon Mechanisms To Preserve Conceptually

Erigon's performance comes from a stack, not from one database choice.

| Mechanism | Erigon implementation | Why it matters |
| --- | --- | --- |
| Hot mutable DB | MDBX tables under `db/kv/mdbx` | Small hot working set, memory-mapped BTree reads, transactional stage writes. |
| Temporal wrapper | `db/kv/temporal.DB` | Presents `GetLatest`, `GetAsOf`, `RangeAsOf`, `HistorySeek`, and `IndexRange` over hot DB plus files. |
| Explicit domains | `db/state.Domain` registered by `db/state/statecfg` | Latest values, history, indexes, and accessors are lifecycle-managed per domain. |
| Cold immutable files | Domain `.kv/.bt/.kvei`, history `.v/.vi`, inverted `.ef/.efi` | Keeps old state out of hot DB and enables cheap archive reads. |
| Shared execution domains | `db/state/execctx.SharedDomains` | Batches latest writes, changesets, commitment touches, and block overlays. |
| TxNum time axis | `db/kv/rawdbv3/txnum` | State history is transaction-granular, not just block-granular. |
| Staged sync | `execution/stagedsync` stages | Headers, bodies, execution, tx lookup, finish, and pruning progress can resume independently. |
| Build/merge/prune lifecycle | `db/state.Aggregator` | Collates hot rows into files, merges files, publishes visible views, then prunes hot tables. |
| Prune modes | `db/kv/prune.Mode` | Full/minimal/blocks/archive choose state and block retention at datadir creation. |
| Snapshot bootstrapping | README/OtterSync plus `db/snapshotsync` | Initial sync can download most latest/history files instead of executing from genesis. |
| Sorted ETL | `db/etl` | Large unordered changes are sorted into temp files before DB insertion to reduce write amplification. |

## go-tron Current Alignment

go-tron is already aligned with many Erigon storage ideas, but the alignment is
not complete.

| Area | go-tron status | Alignment |
| --- | --- | --- |
| Hot DB engine | Pebble via `core/rawdb/pebbledb`, tuned memtables and L0 thresholds. | Partial. Engine differs; architecture can still copy Erigon's temporal/file model. |
| Latest state domains | `state-account-latest-v1`, `state-kv-latest-v2`, `state-kv-generation-v2`, `state-code-v1`, `state-commitment-*`. | Strong. Domain shape is Erigon-style and TRON-adapted. |
| Domain registry | `core/state/snapshots/domain_registry.go` registers account, account-KV, generation, code, commitment root/checkpoint/branch, and history. | Strong. Lifecycle is config-driven. |
| Temporal history | `StateDomainChange`, `StateTxRange`, inverse indexes, hot as-of readers. | Strong for consensus state covered by the flat domains. |
| Cold history | Binary `history/state-domain-change-*.seg` plus `.idx` and `.kv`; new history builds and compactions emit block-compressed `.seg`/`.kv` by default, with `--snapshot.compress-history=false` / `GTRON_SNAPSHOT_COMPRESS_HISTORY=false` as the legacy raw-emission kill switch. | Strong functionally, different file names from Erigon. Source-audit coverage now pins low-level record readers behind compressed-aware openers. |
| Latest files | Binary `.seg` plus `.lidx` and `.bt`. | Strong for point lookup and prefix iteration. Not recsplit, intentionally. |
| Commitment domain | Staged hex-patricia branch rows, checkpoints, cold branch restore, java root adapter. | Strong. Internal root is decoupled from java-tron header root. |
| Code retention | Content-addressed CodeDomain latest snapshots selected by account-envelope history. | Strong, with a deliberate no-temporal-code policy. |
| Cold/hot lifecycle | `SnapshotLifecycle` runs builder, compactor, and pruner in order. | Moderate to strong. Local lifecycle and operator remote fetch/restore exist; automated remote publish/handoff is still missing. |
| Pruning modes | `archive`, `full`, `snap`, `blocks`, and `minimal` are accepted through `--prune.mode`; `--gcmode` remains a deprecated alias. | Moderate. The CLI vocabulary matches Erigon; `blocks` keeps complete local block freezer history while pruning hot state/lookup rows, and `minimal` adds freezer virtual-tail enforcement plus physical shard reclamation gated by cold coverage. Longer benchmark/soak evidence is still needed. |
| Chain freezer | `core/freezer` plus `core/rawdb/freezer`, `ChainDB` fall-through, and cold `chain-index` sidecars. | Moderate. Old block bodies/tx infos/state roots can be served from freezer, and verified sidecars cover block/tx lookup rows after hot prune. |
| Staged execution | Hash-bound `Headers/Bodies/Execution/Commitment/Finish`, `InsertBlocks`, `canonicalRangeExecutor`, reusable `CommitScope`. | Partial. Range-shaped and stage-tracked, but not a full Erigon staged-sync loop. |
| Parallel execution | Async commitment can overlap fold with next block in bulk sync. | Partial. No Erigon-style parallel transaction executor. |
| Snapshot bootstrapping | Local snapshot build/restore plus signed remote fetch exist. | Moderate. Preverified HTTP(S) catalog/manifest/segment download, local reset/resync, and bootstrap restore are covered; production hosting/defaults remain. |
| Derived history domains | Some blooms/traces/receipts are still rawdb or planned. | Weak to partial. Erigon has receipts/log/traces indexes as registered domains or indexes. |
| ETL sorted ingestion | Streaming snapshot builders, batches, and `core/rawdb/etl` collector support exist. | Partial. Latest-domain, state-domain history, chain-freezer hot lookup snapshot restore, chain-index sidecar build, rawdb derived-index bulk loads, transaction lookup/info rebuild, section-bloom rebuild, account-trace rebuild, replayed balance-trace backfill, and cold event-log/section-bloom/balance-trace segment builds now use the collector. Snapshot restore/build CLIs now expose `--snapshot.etl.*` scratch controls; larger benchmark evidence is still needed. |

## Important Non-Alignments By Design

- go-tron must not copy Ethereum consensus behavior. Erigon's account/storage
  domains are Ethereum-specific; go-tron's `AccountKVDomain` plus TRON logical
  domains is the correct adaptation.
- go-tron must keep java-tron protobuf, P2P, DPoS, maintenance-cycle, TAPOS,
  proposal, and actuator semantics. Storage refactors can only change internal
  persistence.
- Erigon's header `stateRoot` is the consensus root. go-tron has a separate
  java-tron `accountStateRoot` compatibility boundary plus an internal
  CommitmentDomain root. Keeping that adapter explicit is required.
- A direct Pebble to MDBX swap is not the first-order task. Erigon's largest
  gains come from hot/cold separation, txNum history, immutable files, staged
  flushing, and pruning. Engine migration should be benchmark-driven after the
  interfaces are stable.

## Remaining Gaps

### P0: Compatibility Gates

The storage refactor is only acceptable if java-tron compatibility remains
provable.

- Expand fixture replay around contracts, account delete/recreate, code updates,
  maintenance, fork gates, PBFT/solid APIs, and archive reads.
- Keep internal CommitmentDomain roots out of block wire validation except
  through the explicit `StateRootAdapter`.
- Add soak checks that delete hot history after cold snapshot coverage, then
  compare archive `Account/Storage/Code` reads against live-at-height fixtures.

### P1: Snapshot Bootstrap

go-tron has local snapshots, but not Erigon's "download most of latest/history"
sync path.

Status:

- The local production manifest already records every segment's relative path,
  size, and checksum.
- The manifest now has optional chain identity (`chainId`, `networkId`,
  `genesisHash`, `forkConfigHash`) plus explicit validation for remote
  installers to reject the wrong chain before installing files.
- `snapshot-catalog.json` can now authenticate the manifest with an Ed25519
  signature over the manifest checksum, chain identity, and visible tx range.
  `VerifySignedSnapshotCatalog` checks trusted signers before manifest/file
  verification, and `RestoreSnapshotFromVerifiedCatalog` uses that catalog gate
  before installing a snapshot.
- `gtron snapshot publish-catalog` signs the local production manifest, and
  `gtron snapshot verify` preflights a local signed catalog/manifest/segment
  set before any install. `gtron snapshot restore` restores from a local signed
  catalog after the operator supplies trusted Ed25519 catalog public keys. The
  restore command is bootstrap-only: it refuses non-genesis datadirs and only
  advances canonical chain stages after chain-freezer data proves the boundary
  block hash.
- Snapshot catalog trust can now be supplied by `--snapshot.trusted-key-file`
  or `GTRON_SNAPSHOT_TRUSTED_KEY_FILE` across fetch, verify, bootstrap,
  restore, and chain-lookup prune commands. Inline trusted keys can also be
  supplied by `--snapshot.trusted-key` or `GTRON_SNAPSHOT_TRUSTED_KEY`. The
  file supports comments, comma-separated entries, and overlap windows for
  Ed25519 key rotation.
- `docs/dev/snapshot-bootstrap.md` records the operator runbook for signed
  remote bootstrap: trusted key file format, key rotation steps, preflight
  verify, fetch+restore, one-step bootstrap, and safety notes around
  `--snapshot.reset`, `GTRON_SNAPSHOT_URL`, `GTRON_SNAPSHOT_TRUSTED_KEY_FILE`,
  `GTRON_SNAPSHOT_TRUSTED_KEY`, and fork config hashes.
- `gtron snapshot fetch` downloads a signed HTTP(S) snapshot catalog, verifies
  the catalog signature before trusting manifest paths, checks the catalog
  manifest checksum, downloads active segments, and then re-runs strict
  registered-segment/checksum verification on the local snapshot directory.
  `--snapshot.reset` deletes the target local snapshot directory first so an
  operator can discard stale local views and resync from the latest remote
  catalog. The remote URL can be supplied by `--snapshot.url` or the
  `GTRON_SNAPSHOT_URL` environment variable. Trusted catalog keys and optional
  fork config hashes have matching `GTRON_SNAPSHOT_*` environment defaults,
  giving operators one default bootstrap surface without hard-coding unofficial
  production URL or key material.
- `gtron snapshot bootstrap` is the integrated operator entry point for remote
  snapshot bootstrap: it fetches the signed remote catalog/manifest/segments
  and then runs the same signed-catalog restore path into the local datadir.
  CLI regression coverage proves the command can reset a stale local snapshot
  directory, download chain-freezer plus `chain-index` and state-history files,
  restore them, advance canonical stage/head rows to the verified boundary, and
  re-open `BlockChainWithAncient` so the downloader chain summary/common-block
  helpers use that boundary as the local sync head. The same regression inserts
  a boundary+1 block after restart, proving the restored head can accept normal
  tail import from the snapshot/freezer boundary.
- `TestSyncServiceImportsTailBlockAfterSnapshotFreezerBoundary` covers the live
  sync receive path after a snapshot/freezer restore boundary. It freezes a
  fully executed boundary block, installs the canonical boundary, restarts
  `BlockChainWithAncient`, then drives `SyncService.HandleBlock` with the
  boundary+1 block so `InsertBlocks` imports the tail block instead of falling
  back to genesis or pausing.
- `TestTwoNodeSyncFromSnapshotFreezerBoundary` covers the P2P soak for the
  same handoff: node B starts from a restored snapshot/freezer boundary, connects
  to node A's longer chain, and syncs boundary+1..tip over the real local P2P
  server path.
- `TestSnapshotFetchCmdDownloadsSignedRemoteSnapshot` covers the operator-facing
  `gtron snapshot fetch` CLI path against an HTTP snapshot source and verifies
  the downloaded catalog, manifest, and active segment bytes match the signed
  source before local signed-catalog verification succeeds.
- Remote snapshot fetch now keeps catalog and manifest download/verification
  serial, then downloads unique active or retired segment files through a
  bounded worker pool (`--snapshot.fetch.concurrency`, default worker count 4).
  `TestFetchRemoteSnapshotDownloadsSegmentsConcurrently` proves multiple
  segment HTTP requests are in flight before final signed-catalog verification.
- `VerifyRemoteManifestFiles` now performs the strict pre-install check for a
  snapshot directory: chain identity, registered segment family, checksum, size,
  and format-aware segment validation.
- `RestoreLatestFromVerifiedManifest` verifies a remote-style snapshot directory
  and restores its latest domains at the manifest boundary, including staged
  CommitmentBranch rows.
- `RestoreHistoryFromVerifiedManifest` verifies a remote-style snapshot
  directory and restores StateDomainChange hot history rows, inverse indexes,
  and block tx-range rows derivable from the history segments.
- `RestoreSnapshotFromVerifiedManifest` performs the strict check once, restores
  latest + StateDomainChange history, and records `SnapshotInstall` plus derived
  snapshot-domain stage progress at the manifest `visibleTxEnd`. It deliberately
  does not advance canonical Headers/Bodies/Execution stages before chain data
  is installed.
- Verified restore now reads back commitment boundary metadata after latest
  restore: when commitment-root/checkpoint segments are present, the hot restored
  root and latest checkpoint pointer must match the already verified snapshot
  files.
- `gtron snapshot restore` now additionally performs an independent staged
  CommitmentDomain rebuild from restored hot latest rows and rejects the
  snapshot if the rebuilt root differs from the signed snapshot boundary root.
  The lower-level restore APIs expose this as an injectable boundary verifier so
  packages that cannot import the commitment engine directly avoid cycles.
- Binary StateDomainChange history segment v2 now stores an explicit block
  tx-range table before record payloads, so restore can preserve block ranges
  even when a block has no state-domain changes. v1 readers remain accepted.
- Chain-freezer snapshots now have a registered `chain-freezer` manifest family
  and binary segment format for continuous block ranges containing the current
  ancient `bodies`, `tx_infos`, and `state_roots` tables. Strict manifest
  verification checks the segment's registered format, size, and checksum, and
  the installer only appends when all target ancient tables are exactly at the
  segment's start block.
- `gtron snapshot restore` opens the datadir's rawdb/freezer store and installs
  signed-catalog chain-freezer segments after state/history restore. It still
  advances canonical chain stages only when the restored StateTxRange at
  `visibleTxEnd` maps to a block whose body is available in hot KV or ancient
  freezer and whose hash matches the signed history boundary.
- Chain-freezer restore also derives the legacy hot lookup indexes from the
  verified segment payload: block hash to number (`bh-`), transaction hash to
  block (`tx-`), and per-transaction info (`ti-`). This makes historical
  block/transaction RPC usable after remote bootstrap even before a cold recsplit
  style accessor lands.
- `InstallCanonicalBoundaryFromVerifiedCatalog` exposes the signed-catalog
  boundary gate to non-CLI callers. It verifies catalog signature, chain
  identity, manifest checksum, and registered segment files before advancing
  canonical stage/head rows to the snapshot boundary, and `gtron snapshot
  restore` now uses this helper for its freezer-to-sync handoff.
- `gtron snapshot build-freezer --snapshot.from-block --snapshot.to-block`
  builds local chain-freezer files from existing ancient rows and integrates
  them into the production manifest, preserving any existing chain identity or
  writing the current chain/dev identity before `publish-catalog` signs the
  manifest.
- Chain-freezer snapshot builds now also emit a registered `chain-index`
  sidecar for the same block range. The sidecar is checksum-covered by the
  manifest and supports cold binary-search lookup for block hash to block number
  and tx hash to block number/tx index without writing those rows to Pebble.
- Runtime chain reads now attach the snapshot manager as an optional `ChainDB`
  cold index reader. `ReadBlockNumber`, `ReadTransactionIndex`,
  `ReadTransactionInfo`, and the freezer fall-through branch of
  `ReadBlockStateRoot` first prefer hot Pebble rows, then use the cold
  `chain-index` sidecar for historical rows. `ReadTransactionInfo` now also
  consumes the sidecar's tx hash to block-local index mapping, so archive
  receipt reads can recover from pruned per-tx rows even when old
  `TransactionRet` payloads lack a populated `TransactionInfo.Id`; when the
  matching block body remains readable through hot KV, ancient freezer, or cold
  freezer fallback, the lookup cross-checks that the sidecar position names the
  requested transaction hash before trusting the block-local receipt row.
  Strict transaction-info reads are now exported for backend/API callers: they
  surface malformed hot rows, corrupt per-block `TransactionRet` payloads, and
  block-body/index mismatches as archive data errors, while still using readable
  block bodies to locate legacy receipt rows whose `TransactionInfo.Id` is
  absent. Hot `ti-<txid>` and `tib-<block>` read failures are also surfaced in
  strict mode instead of being collapsed into misses before consulting cold
  lookup fallbacks.
- Raw freezer accessors now share the same cold path where appropriate:
  `ReadBlockRaw`, `ReadTransactionInfosRaw`, `ReadBlockHashByNumber`, and
  `ReadBlockStateRootRaw` can read through `ChainDB` instead of assuming the
  copied rows are still present in hot KV.
- State-root freezer reads now also have strict variants:
  `ReadBlockStateRootRawStrict` and `ReadBlockStateRootStrict` surface
  malformed hot `bsr-` rows, malformed ancient `state_roots` rows, and cold
  `chain-index` lookup errors. The live freezer runner uses the strict raw
  accessor and aborts the pass on those errors, so corruption cannot be copied
  into ancient storage as an empty state-root row.
- Block-body reads now also have strict variants:
  `ReadBlockRawStrict` surfaces freezer/hot read errors and
  `ReadBlockStrict` surfaces malformed or key-number-mismatched block protos.
  Snapshot canonical-boundary installation, event-log segment builds, and
  rawdb derived-index rebuilds now use the strict block accessor, so archive
  sidecar generation fails on corrupt canonical block rows instead of treating
  them as ordinary missing blocks.
- Canonical block-hash and stage verification now have strict forms too:
  `ReadBlockHashByNumberStrict` and
  `ReadVerifiedStageProgressBlockWithHashLookup` preserve canonical block read
  and decode errors. The default hash-bound stage verifier now uses the strict
  canonical hash path, so `StageFinish` and related gates used by snapshot and
  freezer builders fail with the underlying corrupt-block reason instead of a
  generic "canonical block unavailable" mismatch. The live chain-freezer runner
  now gets its Finish-stage verifier and ChainFreezer-stage writer canonical
  hashes through the strict source too, so corrupt block/hash lookup data aborts
  the freezer pass instead of being collapsed to a zero-hash missing result.
- The live chain-freezer append path now consumes strict raw source methods:
  `ReadBlockRawStrict` and `ReadTransactionInfosRawStrict`. A block-body or
  tx-info storage read error aborts the `ModifyAncients` batch and leaves hot
  rows intact, instead of being collapsed into a missing block or empty
  tx-info row before ancient append.
- Stage progress reads now distinguish missing rows from storage read failures:
  `ReadStageProgressRow` probes with `Has` before `Get` and propagates either
  error. This keeps sync/freezer/snapshot/prune gates from silently treating an
  unreadable stage boundary as if no boundary existed.
- Flat state-domain readers now follow the same missing-vs-error rule for
  archive-critical rows: `ReadStateAccountLatest`, `ReadStateKVLatest`,
  `ReadStateKVGeneration`, `ReadStateTxRange`, and `ReadStateDomainChange`
  probe with `Has` before `Get` and propagate storage errors. Archive as-of
  reads, snapshot builders, and hot-history pruning therefore stop on
  unreadable latest/history rows instead of rolling back from an incorrectly
  assumed absence.
- Account-KV latest snapshot builders now apply the implicit generation-0 rule
  strictly: missing `KVGenerationDomain` rows allow only generation-0 KV rows,
  not every physical generation, and builders fail once KV rows are present if
  their hot source cannot point-read the current generation. This keeps stale
  incarnation data out of cold latest files and reduces snapshot bloat after
  account reset/recreate cycles.
- Commitment-domain readers now do the same for `StateCommitmentDomain`,
  staged commitment branch rows, and commitment engine state. Branch reads keep
  the fast `Get`/`GetNoCopy` hit path and only probe `Has` after a read error,
  so bulk commitment folding does not pay an extra KV lookup for normal hits
  while storage errors still abort instead of being treated as empty branches.
- CodeDomain hot reads now expose the same strict path through
  `ReadStateCodeStrict` and the registered `ReadHotCode` hook. Archive
  reconstruction and pruning coverage checks therefore surface unreadable hot
  content-addressed contract bytecode rows when no CodeDomain snapshot already
  proves coverage, instead of treating them as missing code, while still
  keeping the legacy `ReadStateCode` compatibility wrapper.
- CodeDomain cold coverage checks now also fail loudly: pruning/checker paths
  propagate `GetCodeAtOrBefore` snapshot read errors instead of treating a
  corrupt or unreadable cold code segment as "not covered" and falling back to
  hot code. Snap pruning therefore cannot delete or retain rows based on a
  silently corrupted CodeDomain segment.
- The current-cycle reward pending accumulator also probes presence before
  decoding, so startup and reorg reloads stop on unreadable accumulator storage
  instead of silently resetting in-memory pending rewards to empty.
- Sync staged body reads now follow the same rule: `ReadSyncStagedBlock` and
  `ReadSyncStagedBlockRaw` separate a missing staged body row from `Has`/`Get`
  storage errors, so downloader restart and ready-frontier validation stop on
  unreadable body staging data instead of assuming the table has a gap.
- The production sync body writer now validates supplied raw wire bytes before
  publishing `SyncBodies`: `WriteSyncStagedBlockRawAndProgress` rejects raw
  payloads that decode to a different block number or hash than the block being
  staged, rejects same-number hash conflicts with an existing staged body row,
  and rejects same-height hash conflicts with an existing hash-bound
  `SyncBodies` row before replacing the numbered staged body. It also preserves
  a higher legacy unbound `SyncBodies` watermark instead of regressing it while
  still upgrading same-height legacy rows to hash-bound progress. Lower-level
  raw writes remain available for corruption fixtures and repair tests.

Needed:

- Mainnet/testnet publication policy: official trusted catalog keys, rotation,
  and release workflow.
- Production HTTP catalog/segment hosting and official operator defaults;
  BitTorrent or WebSeed can come later.
- Restore pipeline handoff from snapshot/freezer boundary into downloader,
  `SyncService.HandleBlock`, and local two-node P2P tail sync is now covered by
  regression tests. Remaining production work is longer-running soak/metrics
  around real hosted snapshots and heterogeneous peers.
- Operator publication still needs the official latest remote snapshot URL and
  trusted key release artifacts. The local CLI and runbook now cover a
  `GTRON_SNAPSHOT_URL` default source knob, trusted key environment defaults,
  trusted key files, key rotation, fetch, verify, bootstrap, restore, and reset
  primitives.

This is the largest sync-speed win because it avoids replaying all historical
blocks from genesis.

### P1: Full Staged Sync Loop

`InsertBlocks` is range-shaped, but go-tron still executes and commits block by
block under the canonical chain path.

Status:

- `SyncService` now persists lightweight downloader/body/import pipeline watermarks:
  `SyncInventory` records the highest peer inventory target observed,
  hash-bound `SyncBodies` records the highest block body accepted into the
  transient downloader staging table without regressing on out-of-order
  arrivals, hash-bound `SyncBodiesReady` records the contiguous staged-body
  frontier drainable from the current head and is refreshed after imported
  staged rows are deleted. The import drain now treats a hash-verified
  `SyncBodiesReady` row as the upper bound for the next body batch, and
  hash-bound `SyncImport`,
  `SyncExecution`, `SyncCommitment`, and `SyncFinish` record the latest
  sync-driven block observed at the matching canonical `Bodies`, `Execution`,
  `Commitment`, and `Finish` stage boundaries for the applied range. Partial
  range failures write only the newest observed stage rows at or below the last
  successfully applied block, so a failed block that advanced an early
  canonical stage cannot overwrite the sync-stage handoff boundary. New sync
  sessions restore `SyncInventory` when it is ahead of the current head,
  preserving restart diagnostics/remaining-block estimates without advancing
  canonical chain state; that restore decision is shared through `core/rawdb`
  so stale inventory rows cannot move the target backward. CHAIN_INVENTORY
  target/window derivation now lives in `net/sync/downloader`, keeping
  `remainNum` handling and fetch-window bounds out of `SyncService`; peer
  completion and drained-queue re-poll/wait decisions are helper-owned there
  too. Disconnected-peer retry recovery now uses the same downloader queue
  helpers to requeue pending in-flight IDs in stable block/hash order before
  the peer's local fetch list
  while `SyncService` supplies only the canonical/requested/block-path
  availability filter. CHAIN_INVENTORY, retry-list, and peer-local fetch-list
  candidate decisions are also helper-owned, so the service loop gathers
  chain/cache/path facts while the downloader package decides
  drop/keep/assign/accept behavior. The same package now plans eligible-peer
  fetch actions (`wait local head`, re-poll inventory, delay for rate-limit, or
  send the wire batch), leaving `SyncService` to apply timers and network sends.
  Outbound fetch batches also build their in-flight
  pending/requested-hash bookkeeping through downloader helpers, and received
  block acks use a downloader receipt-session helper to validate pending
  hashes, delete peer-local pending rows, decrement in-flight counts, and
  order buffer planning/refill side effects before `SyncService` updates the
  global requested marks. The accepted-body buffer decision now also gathers
  current-head, duplicate-buffer, and path-reservation facts through a
  downloader reader boundary before staging the raw block body.
  Timeout/disconnect failover now also uses a downloader plan for reset vs
  mirror vs fresh-peer search after remaining peers have had a chance to fill
  fetch slots. Parallel peer join capacity and join-attempt throttling are
  likewise planned in `net/sync/downloader` before `SyncService` queries
  handler candidates. These are
  intentionally outside `CanonicalExecutionStages()` so
  peer-advertised, downloaded, or sync-imported progress cannot masquerade as
  executed canonical chain progress.
- Received sync block bodies are now written to a `sync-staged-block-v1-`
  staging table before they enter the in-memory drain buffer. A restarted sync
  session reloads contiguous staged bodies from `head+1`; successful
  `InsertBlocks` deletes the matching staging rows, while active sync reset
  clears unimported rows so peer failover does not inherit a stale path. Session
  restore scans that ordered staged table as a contiguous range and keeps the
  stored raw body bytes in the in-memory drain buffer instead of decoding and
  re-marshalling them. It also truncates gapped staged-body tails and rewinds
  the `SyncBodies` watermark to the last continuous recovered block, keeping
  persisted downloader state hash-bound and contiguous after restart without
  rejecting out-of-order bodies in the still-running sync session. A
  `SyncBodies` watermark whose first expected body is missing is dropped rather
  than left pointing at an unusable staged range. The restart-time contiguous
  restore scanner now lives in `net/sync/downloader` and returns an explicit
  prune-tail decision for `SyncService` to apply to the persistent stage rows;
  session startup also consumes a downloader plan for the inventory floor,
  imported-body cleanup boundary, staged-body restore start/limit, stale-tail
  pruning flag, and peer-join throttle reset. The storage-level tail delete
  plus `SyncBodies` rewind/delete rule is shared through `core/rawdb`.
  Accepted body staging now uses the same storage layer to persist raw body
  rows and advance `SyncBodies` only when the watermark would not regress,
  batching the raw body plus hash-bound watermark write in the normal forward
  path,
  while imported-body cleanup uses `core/rawdb` to delete the applied raw body
  rows through one rawdb batch flush. Active reset now uses a shared
  `core/rawdb` cleanup helper to clear staged bodies plus
  `SyncBodies`/`SyncBodiesReady` rows in one batch, and range cleanup for startup
  imported-through deletes, stale-tail deletes, and full-reset staged body
  deletes now flushes through rawdb batches where the backing store supports
  them.
  `SyncBodiesReady` refresh and drain-limit reads are downloader helpers now:
  refresh recomputes the contiguous staged-body frontier and writes or deletes
  the ready row, while the drain-limit helper reads the hash-bound ready row
  plus matching staged body before `SyncService` logs or refreshes stale rows.
  Accepted body handling also asks the downloader helper whether the new staged
  body can start or extend that ready frontier, so distant out-of-order bodies
  no longer force a full staged-table scan.
- The downloader's wire batch and local import batch are now separate knobs:
  `FETCH_INV_DATA` still requests java-tron-compatible 100-block windows, while
  the staged-body drain restores/pops at most a smaller local import chunk per
  `InsertBlocksWithStageHook` call. The local chunk defaults to 32 blocks and
  is operator-tunable through `--sync.import-batch` within the wire-safe
  1..100 range. This keeps peer throughput high but bounds the decoded block
  range, state execution, commitment folding, and stage-row observation done in
  one local pass. Regression coverage proves a range larger than one local
  import chunk drains through multiple chunks, custom chunk limits are honored,
  invalid limits are rejected, session startup still prunes gapped staged-body
  tails beyond the import chunk, and hash-bound `SyncImport`, `SyncExecution`,
  `SyncCommitment`, and `SyncFinish` still advance to the final block. The
  local drain frontier clamp, stale-ready refresh request, and contiguous
  buffer pop/release logic now live in `net/sync/downloader`, and the
  applied-prefix import summary now derives tx counts plus the stage-progress
  boundary there as well. Insert range failure resolution also maps
  `InsertBlocksError` indexes back to buffered blocks in the downloader
  package; import outcome planning there decides applied-prefix recording,
  failure pause target, and drain-loop stop behavior; empty local-drain
  settlement after fetch-slot refill now uses a
  downloader session plan to choose finish vs peer-join probing; and the same
  package now owns raw-buffer decode actions so a decoded prefix can be imported
  while a first-entry decode failure simply continues the drain loop. The same
  applied-prefix summary derives the staged body delete descriptors used for
  imported-body cleanup, and those deletes are committed with the hash-bound
  sync import/execution/commitment/finish rows through one `core/rawdb` batch
  when the backing store supports batched writes. The downloader package now
  owns the complete imported-batch storage plan and builds explicit
  bodies/execution/commitment/finish import-stage tasks for the applied
  block/hash boundary. Stage rows are published only as a contiguous
  sync-stage prefix and only when the canonical hook observation hits that
  exact boundary, so missing or mismatched execution progress prevents later
  commitment/finish rows from making restart diagnostics look more advanced
  than the applied pipeline. `SyncService` is left to orchestrate logging,
  pausing, ready-frontier refresh, and canonical insertion.
- Imported-batch record/report scheduling now treats persisted sync-stage
  progress as the boundary for throughput diagnostics: if the staged-body
  delete or hash-bound `SyncImport`/`SyncExecution`/`SyncCommitment`/`SyncFinish`
  write fails after canonical insertion, the downloader record plan stops
  before stats/report emission so operators do not see an imported-segment
  summary for a boundary that was not durably recorded.
- The rawdb import-progress batch writer now validates its progress rows before
  deleting imported staged bodies: rows must be hash-bound and limited to the
  sync import pipeline stages (`SyncImport`, `SyncExecution`, `SyncCommitment`,
  `SyncFinish`). This keeps non-batch stores from half-committing body deletes
  before rejecting an invalid stage row.
- For stores without batch support, the same writer now publishes validated
  sync import progress before deleting imported staged-body rows. If the
  progress write fails, the recoverable staged-body prefix remains intact; if
  the later cleanup delete fails, restart recovery can still see the durable
  sync-stage boundary and retry cleanup.
- The same import-progress writer now validates imported staged-body deletes
  against the staged table before committing any side effect: each delete
  descriptor's block hash must match the stored raw body row. Missing,
  unreadable, corrupt, or hash-mismatched staged rows stop the whole
  delete/progress batch, so sync-stage progress cannot advance after deleting
  the wrong downloader body.
- Imported sync progress rows are now validated as a single stage prefix before
  publication too: `SyncImport`, `SyncExecution`, `SyncCommitment`, and
  `SyncFinish` must appear in order, downstream stages cannot be ahead of
  upstream stages, and every published stage boundary must match a block/hash in
  the staged-body delete prefix. Progress-only import-stage writes without a
  staged-body delete proof are rejected as well. This prevents rawdb callers
  from publishing a sync-stage boundary that does not belong to the body prefix
  they just removed.
  The same writer also rejects normal import writes that would move an existing
  sync import stage to a lower block, or replace a hash-bound same-height row
  with a different hash; explicit startup repair/unwind remains the path for
  clearing stale or forked progress.
  It also validates the merged view of existing and newly written import-stage
  rows before deleting staged bodies: previously persisted downstream
  `SyncExecution`/`SyncCommitment`/`SyncFinish` rows may lag the new upstream
  boundary, but cannot be unbound, missing their upstream row, ahead of it, or
  same-height hash-mismatched.
- Sync pipeline order checks now treat same-height hash mismatches as ordering
  violations too. Startup repair and `db stage-status --db.stage.verify` no
  longer accept, for example, `SyncImport=N/hashA` with
  `SyncExecution=N/hashB`; the downstream tail is deleted/reported the same way
  as a downstream stage that is numerically ahead.
- The local import run settlement now also stops the drain loop when that
  persisted progress write/delete fails, even if canonical insertion succeeded.
  This prevents the sync loop from advancing into another staged-body chunk
  after the Erigon-style stage boundary failed to become durable.
- The imported-batch progress applier now stops before refreshing
  `SyncBodiesReady` when the staged-body delete or sync-stage progress write
  fails, so the ready frontier is also downstream of a durable import-stage
  boundary.
- `SyncBodiesReady` refresh failures are now treated as imported-progress
  failures too: the record scheduler suppresses stats/report output and the
  local drain loop stops before attempting another chunk, preserving the same
  stage-before-downstream-view ordering for ready-frontier writes.
- The post-drain resume-phase publish gate is now downloader-owned too:
  no-phase, commit-barrier failure, paused-loop, and publish decisions are
  planned before the same downloader-owned run applies the verified read/write
  publish path.
- Import-batch drain-loop finalization now also lives in downloader: the
  post-import last-peer update, pause flag, resume-phase suffix, and
  stop/continue branch are derived from the import run before `SyncService`
  applies the concrete loop mechanics.
- Empty-drain and drain-end insert-session commit barriers now use a downloader
  plan for `finishOK`, paused state, and one-shot pause publication before
  resume-phase progress can be published.
- Local staged-body drain startup uses the same rule when repairing an invalid
  `SyncBodiesReady` row before import: if the repair refresh fails, the drain
  does not restore or pop buffered bodies against a stale or unverified ready
  frontier.
- Sync-pipeline startup order repair also reuses the same validated
  `SyncBodiesReady` drain-limit check before repairing `SyncBodies` from the
  ready row, so a ready row whose staged body is missing or hash-mismatched is
  deleted as downstream tail instead of promoted.
- Session startup now records the `SyncBodiesReady` refresh result and stops
  before downstream order repair/check/cursor derivation if that refresh cannot
  be persisted, preserving stage-before-view ordering across restart recovery.
- The same startup scheduler now stops after sync-stage repair read/delete
  failures, current-head completion write failures, imported staged-body
  cleanup failures, and order-repair read/write/delete failures before
  restoring or deriving any downstream view.
- Storage-level staged-body tail pruning now reads and validates the
  `SyncBodies` watermark before deleting any stale tail rows, then commits the
  tail delete plus `SyncBodies` rewind/delete in one rawdb batch. A corrupt or
  unreadable watermark no longer deletes staged body rows first and leaves the
  persisted downloader boundary pointing at missing data.
- The higher local-drain session branch now treats that pre-import ready repair
  failure as a stop condition instead of an empty drain, so it will not refill
  fetch slots or probe peers as though the stage table were merely empty.
- Fetched-body receipt settlement now applies the same stage-before-downstream
  rule at the body-ingress side: a raw body write, `SyncBodies` progress, or
  `SyncBodiesReady` refresh failure stops post-buffer refill/drain, keeps the
  block out of the in-memory drain buffer, and lets `SyncService` sticky-pause
  instead of continuing as if the downloader body stage were durable.
  Net-level regression coverage now corrupts the ready frontier and proves the
  real `HandleBlock` path pauses, sends no follow-up fetch frames, leaves no
  in-memory drain entry, and clears the staged-body progress rows during reset.
- Sync pipeline startup repair now keeps only hash-bound `SyncImport`,
  `SyncExecution`, `SyncCommitment`, and `SyncFinish` rows that still resolve to
  the current canonical chain; rows that point past the head, lack a hash, or
  name an old fork hash are deleted. It now repairs the four rows as one
  ordered pipeline too: downstream stages that are ahead of their upstream
  stage, or present after a missing upstream stage, are deleted so restart
  diagnostics remain a contiguous sync-stage prefix. This keeps
  downloader/import diagnostics from masquerading as resumable canonical stages
  after restart or fork repair. The row validation, order validation, and delete
  decision now live in `net/sync/downloader`, with `SyncService` supplying only
  the canonical hash reader and logs.
- `BlockChain` startup now verifies `Headers/Bodies/Execution/Commitment/Finish`
  stage rows against the persisted head block and repairs missing, legacy, or
  mismatched rows back to that hash-bound head. This makes canonical stage
  progress a stable restart boundary for later prune/freezer/snapshot consumers,
  while `SyncInventory`, `SyncBodies`, `SyncImport`, `SyncExecution`,
  `SyncCommitment`, and `SyncFinish` remain non-canonical downloader/import
  diagnostics.
- The cold history snapshot builder now caps its history cutoff at the verified
  hash-bound `StageFinish` row and fails on finish-stage hash mismatches, so
  immutable history/event/bloom/trace sidecars are not published past the same
  canonical execution boundary used by the pruner and chain freezer.
  Latest-domain snapshot watermarks use the same cap, so full-keyspace latest
  files are also labelled with the verified execution boundary rather than an
  unverified solidified-height estimate.
- The freezer and snapshot builder now share the rawdb
  `ReadVerifiedStageProgressBlock` helper for this hash-bound canonical-stage
  check, keeping StageFinish integrity rules centralized at the DB-accessor
  layer. The freezer runner and cold snapshot builder supply their
  `ChainSource` hash readers to the same verifier, so StageFinish checks still
  pass when old block bodies have already moved out of hot Pebble. Cold snapshot
  builder sources can now expose an error-aware canonical hash lookup too; the
  production snapshot chain source uses the strict ChainDB block-hash accessor,
  so corrupt hot/freezer block rows or cold hash lookup errors abort snapshot
  build-stage publication instead of being collapsed into a zero hash.
- `gtron db stage-status` now gives operators one Erigon-style stage view over
  canonical execution, sync downloader/import diagnostics, snapshot build,
  prune, and chain-freezer progress. Hash-bound rows are checked through the
  freezer-aware chain reader, so old canonical blocks that have left hot Pebble
  do not produce false mismatch diagnostics. `--db.stage.verify` turns the
  same view into an automation gate for canonical, sync-import, and
  cold-coverage boundary stages; it fails on mismatched/missing canonical
  hashes or legacy unbound canonical/freezer/event-log-build/chain-lookup/
  section-bloom-prune/balance-trace-prune/tail-prune stage rows, while
  downloader body stages report `verified=staged` only when
  their hash-bound progress rows still match the staged-body table; failed
  staged-body checks include decoded `stagedBlock`/`stagedHash` evidence when
  available. StateTxRange hash fallback is limited to derived cold
  event-log/section-bloom/balance-trace stages; canonical, freezer, lookup,
  and tail-prune stages still require a readable canonical block hash from the
  chain reader. Canonical boundary checks use the strict block-hash accessor
  and include `canonicalError=...` details when hot Pebble or cold freezer block
  rows fail to read or decode, so automation sees the storage-root cause
  instead of a silent zero-hash collapse. It also
  rejects canonical and sync-stage order violations, e.g. `Execution` ahead of
  `Bodies`, `SyncExecution` ahead of `SyncImport`, or `SyncBodiesReady` ahead
  of `SyncBodies`, cold build/prune/freezer coverage ahead of verified
  `Finish` (including balance-trace and section-bloom hot-row prune stages),
  validates that both `SyncBodies` and `SyncBodiesReady` point to readable
  hash-matching staged body rows before local import can drain them, and
  rejects cold-prune coverage violations such as
  `SnapshotChainLookupPrune` ahead of or present without `ChainFreezer`.
  Canonical/cold coverage stage order checks also reject same-height
  hash-bound mismatches, so `Execution=N/hashB` cannot be considered ordered
  after `Bodies=N/hashA`, and freezer/prune coverage cannot claim the same
  height on a different block hash than its upstream boundary.
  The verification gate now also opens the local snapshot manifest and checks
  actual readable cold coverage for `SnapshotEventLogBuild`,
  `SnapshotChainLookupPrune`, `SnapshotSectionBloomPrune`,
  `SnapshotBalanceTracePrune`, and `SnapshotChainFreezerTailPrune`, catching
  missing/gapped/corrupt sidecars even when the stage rows themselves look
  ordered. Tail-prune verification checks the same chain-freezer, chain-index,
  and non-genesis indexed event-log coverage required by the apply path, while
  genesis-only tail-prune progress is allowed with chain-freezer plus
  chain-index coverage.
  Balance-trace stage coverage starts at block 1 because genesis has no
  replayed `BlockBalanceTrace` row.
  Manifest-backed progress rows (`SnapshotLatest`, `SnapshotHistory`,
  `SnapshotAccessor`, `SnapshotCommitmentFlush`, and `SnapshotHotPrune`) are
  checked against the active manifest `progress` section as well, so automation
  cannot trust DB stage rows that are ahead of or missing from the snapshot
  artifact set that would be used for restore/prune/archive decisions.
  The same verification gate now rejects legacy unbound `SyncImport`,
  `SyncExecution`, `SyncCommitment`, and `SyncFinish` rows, matching the
  hash-bound import-progress writer and startup repair rules.
  The command also supports `--json`, emitting the same stage rows, verification
  evidence, and issue list in a machine-readable form while preserving the text
  output and `--db.stage.verify` failure semantics.
- The state pruner now rejects legacy/unbound `StageFinish` rows instead of
  pruning against an unverifiable height, and its fallback canonical-hash lookup
  uses the freezer-aware rawdb block-hash accessor. The live pruner adapter now
  exposes an error-aware canonical hash source, so corrupt hot/freezer block
  rows or cold hash lookup errors abort Finish-stage and prune-head boundary
  checks instead of being collapsed into "canonical block unavailable". When a
  caller does not provide an explicit canonical hash source but does expose
  `ChainDB()`, the pruner and snapshot lifecycle now resolve the `StageFinish`
  block hash through that hot+ancient chain reader before falling back to hot
  KV, so frozen block bodies do not falsely block safe history pruning. Live
  pruning also writes `SnapshotPrune` as a hash-bound stage row whenever the
  prune head can be tied to the verified finish/canonical boundary, so
  stage-status checks can prove hot-history pruning did not advance onto
  another fork at the same height.
- Regression coverage checks both normal multi-peer sync and snapshot-freezer
  boundary handoff: inventory target progress survives the CHAIN_INVENTORY path,
  downloaded bodies are staged and restored across session startup, gapped
  staged-body tails and stale downloader watermarks are dropped on restart,
  corrupted startup canonical stages are repaired to the stored head, snapshot
  builds are capped at verified finish stage, and imported sync pipeline
  number/hash progress is derived from the canonical stage observer after
  `InsertBlocks` applies the range. Startup recovery now also deletes
  sync-pipeline progress mismatches where execution/commitment/finish are ahead
  of their upstream sync stage.
- `scripts/dev/nile_sync_sample.sh` now emits one JSONL sample for long-running
  Nile sync/soak runs without opening the live Pebble store: HTTP height/block
  ID, peers, elapsed sync time, block rate, git commit, and datadir size split
  across hot `chaindata`, ancient freezer, state snapshots, and replay scratch.
  When the node is stopped, `--offline-db-check` appends the same
  freezer/stage/snapshot alert fields used by the storage benchmark harness,
  including detail arrays for stage verification and snapshot/freezer alerts so
  `SyncBodiesReady` staged-body mismatches and structured stage-alert kinds are
  visible in the JSONL row itself.
  The sampler also parses captured `gtron db stage-status --json` files for the
  live-stage fields and staged-body issue details, with a legacy text fallback
  for older diagnostics. It can also run a live JSON-RPC archive-read probe and
  emit `archiveApi*` evidence for `eth_getBlockByNumber`, `eth_getBalance`,
  `eth_getCode`, `eth_getStorageAt`, `eth_getLogs`, and optional contract
  `eth_call` samples. When the selected historical block contains a
  transaction, the same probe now adds `eth_getTransactionByHash` and
  `eth_getTransactionReceipt` evidence plus `archiveApiTx*` fields, and the
  Nile acceptance gate can require that tx/receipt archive proof with
  `--require-archive-tx-evidence`.
- Imported sync segment stats now include the top transaction contract types
  for the applied window (`txTop` in the runtime log and `syncLogTxTop` in the
  Nile sampler). This keeps staged-sync throughput soaks from conflating
  contract-heavy windows with transfer-heavy windows when diagnosing execution
  and state-commit bottlenecks.
- Phase cursor diagnostics now expose the current phase task count, remaining
  tasks, and the next task phase/canonical/sync stage in both runtime logs and
  the Nile sampler. Long soaks can identify the exact staged-sync task that is
  missing or mismatched without reopening the live database.
- Canonical, snapshot-build/prune, chain-freezer, chain-lookup-prune, and
  freezer-tail-prune stage dependencies are now rawdb-owned
  `StageProgressOrderPairs`, and `gtron db stage-status` consumes that shared
  checker next to the downloader-owned sync-stage checker. This makes the
  post-import storage maintenance stages part of the same machine-verifiable
  staged pipeline model instead of CLI-local validation logic.
- The same rawdb-owned dependency graph now has a scheduler-friendly stage
  pipeline cursor. `gtron db stage-status` reports the next canonical or
  post-`Finish` snapshot/prune/freezer edge that can advance, while
  `stage-status --json` exposes `pipeline.pending`, `pipeline.tasks`, and
  `pipeline.issues` for Nile soak sampling. This makes storage-maintenance
  backlog observable before the runtime loop is fully promoted into a stage
  scheduler.
- The sync-import phase cursor now exposes the runnable suffix of its current
  bodies/execution/commitment/finish phase. This gives a future staged scheduler
  a typed current-phase plan to resume from instead of scraping log fields or
  re-deriving the batch graph from raw diagnostic counters. Runtime logs and
  the Nile sampler now preserve that suffix's block range as
  `syncPhaseCursorCurrentFromBlock`/`syncPhaseCursorCurrentToBlock`. The same
  suffix is now retained on the imported-batch progress plan as a defensive-copy
  `ResumePhasePlan`, so the next scheduler step can consume the typed
  execution/commitment/finish task list directly after a partial import. Import
  settlement now emits a distinct `YieldResumePhase` drain-loop action when
  that resume plan is present, stopping the local drain loop before another body
  chunk can advance past the scheduler-owned phase while keeping the handoff
  distinguishable from canonical failures and storage progress failures. When
  the drain exits on that handoff, `SyncService` now crosses the insert-session
  commit barrier and publishes the resume phase suffix only after each phase's
  canonical stage row is hash-bound to the expected block/hash and the matching
  Sync* diagnostic row would not regress. This closes the async-commit gap where
  commitment/finish hooks could complete after the initial import record had
  already deleted staged-body proof rows; `net` package tests cover both the
  successful barrier publish and the canonical-mismatch / sync-ahead rejection
  paths through the `SyncService` entry point. The same read/verify/write
  sequence is now a downloader-owned resume-phase publish run, leaving
  `SyncService` to provide only stage-progress read/write side effects.
- `gtron db storage-alerts` now carries the same stage pipeline cursor in JSON,
  text, and Prometheus output. The Nile sampler preserves those
  `stageAlertPipeline*` fields during offline DB checks, so production soaks can
  separate storage-maintenance backlog from critical storage integrity alerts.
- `scripts/dev/storage_benchmark.sh` now preserves the same
  `stageAlertPipeline*` fields in benchmark JSONL rows, and the storage
  benchmark acceptance checker verifies matching Prometheus pipeline metrics
  when a row includes the pipeline cursor.
- `scripts/dev/nile_sync_acceptance.py` applies the same check for stopped-node
  Nile samples: if an offline DB-check row carries `stageAlertPipeline*`, the
  referenced storage-alert Prometheus artifact must include the matching
  `gtron_storage_stage_pipeline_*` metric values, next-target/current values,
  and stage/status/upstream labels.
- The cold snapshot builder now writes `SnapshotBuild` and
  `SnapshotLatestBuild` as hash-bound canonical block stages. It resolves the
  boundary hash before publishing the new stage row, and
  `gtron db stage-status --db.stage.verify` rejects legacy unbound or
  same-height mismatched build-stage rows, so snapshot publication cadence and
  history-build progress cannot silently cross a fork boundary.
  Restart seeding for the latest-snapshot cadence also ignores unbound,
  missing-canonical, or hash-mismatched `SnapshotLatestBuild` rows instead of
  using them as a durable cadence boundary; the next latest build can then
  rewrite the row with a verified block hash.
- Derived-index hot-row pruning for balance traces and section blooms now
  writes hash-bound `SnapshotBalanceTracePrune` and
  `SnapshotSectionBloomPrune` stage rows after verified cold segment coverage,
  and `gtron db stage-status --db.stage.verify` treats those rows as canonical
  hash-bound cold-coverage stages. Progress-aware pruning ignores legacy
  unbound resume rows and reprocesses the covered cold range to upgrade them,
  so cold-index reclamation has the same fork-bound proof as chain lookup and
  freezer-tail pruning.
- `gtron db stage-status --json` now preserves backward-compatible
  `issues` strings while also emitting structured `issueDetails` with issue
  kind, stage/order edge, values, missing-upstream, and hash-mismatch flags.
  The Nile sampler carries these into `stageIssue*`/`stageOrderIssue*` JSONL
  fields and marks `stage-status-issue` / `stage-order-issue` in
  `soakHealthIssues`.
- `scripts/dev/nile_sync_acceptance.py` now gates Nile sync JSONL samples for
  full staged-sync evidence. It checks latest-row sample health, captured
  stage-status availability, staged-sync readiness or caught-up status, stage
  monotonicity, complete six-stage presence plus hash-verification evidence,
  hash/staged-body/order/regression counters, optional stopped offline DB
  checks, optional required `stageStalled*`/`stageStalls` diagnostics,
  storage-alert Prometheus artifact shape, and project-specific numeric
  thresholds.

Needed:

- Extend the downloader/body/import watermarks into a fuller staged loop for
  execution, commitment, finish, snapshot build, prune, and freezer.
- Promote the chunked sync drain into a fuller stage scheduler instead of
  coupling download, execution, commitment, finish, and retry policy inside one
  service loop.
- Make range execution flush domain batches by stage-sized chunks while still
  emitting per-block roots and TRON maintenance side effects.
- Preserve fork/reorg safety through hash-bound stage rows and KhaosDB.

This is the largest live-sync throughput win after snapshot bootstrap.

### P1: Chain Freezer Completion

The current freezer narrows Pebble pressure but does not yet move all
historical chain data out of hot KV.

Status:

- Ancient table names are now exported from rawdb so the freezer runner,
  accessors, and snapshot installer use the same table layout.
- A chain-freezer snapshot segment can be built from existing ancient rows and
  installed into an empty or exactly contiguous target ancient store.
- The install path preserves append-only freezer semantics: it rejects
  non-contiguous target heads instead of truncating or filling gaps.
- The snapshot restore CLI now installs signed-catalog chain-freezer segments
  into the datadir freezer, writes the boundary block's hash-to-number index,
  and advances canonical stage progress to the verified boundary only after the
  boundary block hash is re-read from chain data.
- During chain-freezer restore, block and transaction lookup indexes are
  rebuilt from verified freezer segment rows and can be safely retried: if the
  ancient rows already match the segment, restore skips append and rebuilds only
  the indexes.
- Chain-freezer hot index restore now validates any embedded
  `TransactionRet` payload against the canonical block transaction count, block
  number, and tx hash order before writing per-tx `TransactionInfo` rows. This
  keeps older snapshots that restore hot lookup/info rows from publishing
  archive transaction metadata that does not match the frozen block body.
- The snapshot build CLI can now add chain-freezer segments plus matching
  chain-index lookup sidecars from local ancient rows to the production
  manifest.
- The snapshot restore CLI now prefers cold lookup sidecars when a signed
  catalog contains a chain-index for the chain-freezer range. Before skipping
  hot lookup restoration, it cross-checks the sidecar against the freezer
  segment's block and tx hashes; older snapshots without sidecars still rebuild
  hot lookup rows.
- Core/backend regression coverage now exercises the restored cold path with
  historical hot `b-`, `bh-`, `tib-`, `tx-`, `ti-`, and `bsr-` rows removed:
  `BlockChain`/`TronBackend` can still resolve block-by-hash,
  transaction-by-id, transaction-info-by-id, transaction-by-hash, and
  historical block state root through freezer plus `chain-index`.
- The live freezer runner now records `ChainFreezer` stage progress after it
  has durably appended num-keyed chain rows to ancient and deleted the covered
  hot `b-<num>`/`tib-<num>` rows. Crash-leftover reconciliation advances the
  same stage after sweeping rows that were already ancient before a restart.
  On upgrade/restart it also backfills the stage from the local ancient head,
  so existing freezer coverage is visible even when no new blocks need freezing.
  Live freezer passes are now capped at the verified hash-bound `StageFinish`
  row and fail on finish-stage hash mismatches, matching the state pruner's
  guard against moving hot data past the canonical execution boundary.
- Verified chain-freezer snapshot restore writes the same `ChainFreezer` stage
  to the restored datadir after the segment is installed or verified as already
  present, so remote bootstrap and live freezer passes expose one cold-chain
  coverage watermark. Live freezer passes and chain-freezer snapshot restore
  now write that watermark as a hash-bound stage row, using the freezer chain
  reader or the verified segment's boundary block payload. Existing unbound
  rows at the same height are upgraded, while same-height hash mismatches are
  treated as storage integrity errors instead of being overwritten.
  Live freezer passes now also reject `ChainFreezer` rows that are ahead of the
  local ancient `bodies` head instead of silently preserving that downstream
  stage. Runtime and CLI chain-lookup pruning pass a freezer-aware `ChainDB`
  into the progress-aware prune path, which rejects `StageChainFreezer` values
  above the local ancient head before deleting hot hash lookups.
- `PruneHotChainLookups` can now remove historical hot `bh-`, `tx-`, `ti-`,
  and `bsr-` lookup rows for a freezer range after verifying the matching
  `chain-index` sidecar against the chain-freezer segment. Tests prove cold
  reads continue to work through freezer plus sidecar after these hot lookup
  rows are deleted. The progress-aware path now writes
  `SnapshotChainLookupPrune` as a hash-bound stage row using the verified
  chain-freezer segment boundary block hash, requires a hash-bound
  `ChainFreezer` upstream boundary before deleting hot lookup rows, repairs
  legacy unbound `SnapshotChainLookupPrune` rows by reprocessing the covered
  cold range, and rejects same-height hash conflicts before any hot lookup
  row is deleted.
- `gtron snapshot prune-chain-lookups` now exposes that verified prune path to
  operators. It derives the local chain identity, verifies the signed snapshot
  catalog with trusted Ed25519 keys, cross-checks freezer/index sidecars, and
  then removes only the covered hot lookup rows. The progress-aware runtime and
  CLI prune paths also cap deletion at the local `ChainFreezer` stage, so
  hash-key lookup rows are not removed before the matching local ancient rows
  are installed or produced by the live freezer.
- The domain state snapshot/prune lifecycle now has a chain-lookup prune hook.
  Runtime startup wires it to the local production manifest and records
  `SnapshotChainLookupPrune` stage progress, so repeated passes process only
  newly published freezer blocks.
- Nodes without the domain-state prune lifecycle now register a standalone
  chain-lookup prune lifecycle under `full`, `snap`, `blocks`, and `minimal`
  modes. `archive` keeps the hot lookup rows.
- TVM `BLOCKHASH` and pre-optimized `CHAINID` paths now remain compatible after
  hot block-body rows are frozen/pruned: `BLOCKHASH` tries the execution buffer
  first and then a cold `BlockHashByNumber` hook wired to `ChainDB`; `CHAINID`
  falls back to the execution context's genesis hash when hot genesis block data
  is absent. Execution stores now also expose an optional strict block-hash
  reader, so corrupt hot/freezer block rows in the BLOCKHASH window or genesis
  hash lookup abort execution with the storage error instead of being
  translated into hash zero or a numeric chain-id fallback. A rawdb source
  audit now prevents VM code from regressing to the legacy no-error
  `ReadBlockHashByNumber` path, and the chain's shared blockbuffer cold reader
  now implements the strict variant as well as the compatibility no-error
  method.
- Freezer startup repair is covered for table cardinality mismatch: writable
  opens truncate all freezer tables to the common low head, while readonly opens
  reject mismatched heads instead of silently serving a partial ancient view.
  Writable repair passes now leave a structured `Repair` snapshot in freezer
  stats, persist the same last-repair record as freezer-local `repair.json`,
  and emit a warning when any table is truncated. `gtron db freezer-status`
  prints that repair summary, including `repairRecordedAt`, for operator
  diagnostics even after a later readonly reopen. The same signal is exported
  through go-ethereum metrics gauges/counters under `ancient/repair/*` and is
  available on the opt-in debug server via `/debug/metrics?prefix=ancient/repair/`.
- The live freezer runner now mirrors its `Runner.Snapshot` values into
  go-ethereum metrics gauges under `chain/freezer/*`, including the visible
  frozen range, cumulative frozen blocks/passes, latest pass timestamp/duration,
  and sampled hot Pebble block-row footprint. The opt-in debug metrics endpoint
  can therefore expose `/debug/metrics?prefix=chain/freezer/` for sync/freezer
  soak dashboards without scraping logs.
- `gtron db storage-alerts` now provides a single script-friendly alert gate for
  long soaks. It combines the persisted freezer checks from
  `gtron db freezer-alerts` with `stage-status --db.stage.verify`, failing on
  recorded freezer repair, missing, unbound, hash-mismatched, or impossible
  `ChainFreezer` stage progress, freezer/table bound mismatches, virtual-tail
  invariants, missing, unbound, unreadable, hash-mismatched, or contradictory
  `SnapshotChainFreezerTailPrune` proof for the hidden tail,
  hash-mismatched or out-of-order stage rows, and claimed cold coverage that
  the local manifest cannot prove. It also reports hidden freezer bytes and
  retired snapshot bytes that still await physical pruning. Stage alert details
  preserve the structured issue kind and any root-cause detail emitted by
  `stage-status`, including strict canonical block read/decode errors. Text
  output and the Nile/storage-benchmark legacy parsers preserve the same
  stage-alert kind while remaining compatible with older detail-only lines.
- The raw freezer now has a prunable-table virtual tail API: `TruncateTail`
  persists a hidden ancient tail and makes old rows unreadable without changing
  the append head. The production chain-freezer table set marks `bodies`,
  `tx_infos`, and `state_roots` as prunable and has regression coverage proving
  the table set can advance and persist a hidden tail.
- `PlanChainFreezerTailPrune` now provides the boundary calculator for
  minimal-mode block retention. It combines `ChainFreezer`,
  `SnapshotChainLookupPrune`, and `SnapshotEventLogBuild` stage progress,
  converts inclusive coverage block `N` into freezer tail `N+1`, and caps the
  target by the ancient append head plus the recent-block retention window.
  The planner itself floors non-zero retention to the 256-block TVM
  `BLOCKHASH` window, so lower operator windows or direct helper calls cannot
  hide ancient blocks that execution may still read.
  DB-backed planning now reads those dependency stages through hash-bound
  verification, using the freezer/snapshot-aware chain reader when local hot
  block bodies have already moved cold. The event-log build boundary keeps
  minimal-mode physical tail pruning behind cold lookup/log index coverage, so
  archive block/transaction/log queries do not lose their immutable sidecar
  path when local freezer files are hidden or reclaimed. The apply path verifies
  cold chain-freezer, chain-index, and
  indexed event-log coverage before calling runtime `TruncateTail`; indexed
  event-log coverage starts at block 1 because genesis has no transaction logs,
  so genesis-only tail movement is allowed with cold freezer plus chain-index
  coverage while non-genesis tail movement still requires the event-log build
  stage and cold indexed log coverage. Snapshot managers now prove continuous
  chain-freezer, chain-index, and indexed log coverage for the whole tail range
  instead of only probing
  endpoints, and generic cold `AncientReader` fallbacks verify every chain-freezer
  row range with `AncientRange` rather than trusting first/last rows. Tests pin
  missing-stage no-ops, lookup-stage caps, event-log-stage caps,
  retention-window caps, ancient-head caps, short-chain behavior, DB-backed
  stage reads, successful tail truncation, no-op behavior when cold coverage is
  missing, and rejection of gapped cold coverage. Successful tail pruning now
  records `SnapshotChainFreezerTailPrune` as a hash-bound stage row using the
  local freezer boundary block before truncation; legacy unbound rows at the
  same height are upgraded, while same-height hash conflicts or rows ahead of
  the actual prune boundary abort before the freezer tail moves. If a previous
  pass already advanced the virtual tail but failed before writing the stage
  row, the next pass repairs `SnapshotChainFreezerTailPrune` from verified cold
  chain-freezer/chain-index/event-log coverage instead of leaving storage-alerts
  without a hash-bound proof of the hidden freezer range.
- The snapshot `Manager` now implements the rawdb `AncientReader` shape for
  chain-freezer segments, and `rawdb.NewFallbackAncientReader` composes local
  freezer rows with verified cold snapshot files. Runtime startup wraps the
  local freezer reader with that snapshot fallback before genesis/blockchain
  creation. Regression coverage hides the local freezer tail and proves
  `ChainDB` can still read historical block bodies, transaction infos, and
  state roots through chain-freezer segments plus the `chain-index` sidecar.
- Chain-freezer snapshots now also publish a `chain-freezer-accessor` sidecar:
  a block-number to row-offset table for the variable-length freezer segment.
  `Manager.Ancient` prefers that accessor for O(1) block-number point reads
  and falls back to the old scan path only for legacy manifests without the
  sidecar. Snapshot-backed `AncientRange` now also reads through a single
  manifest view and streams sequential rows across active chain-freezer
  segments, using the accessor to jump to the first requested block before
  continuing linearly. Tests prove the accessor build/check/verify path, range
  reads across segment boundaries, `maxBytes` truncation, gap handling, and that
  Manager can serve a row even when full segment scanning would reject trailing
  bytes. `Manager.HasAncient` now proves the requested cold row is actually
  readable instead of trusting only the manifest range, and
  `Manager.AncientCount` verifies the highest advertised cold row before
  reporting a snapshot-backed ancient head. Missing chain-freezer files
  therefore surface as archive data errors before callers treat them as usable
  ancient coverage. Chain-freezer cold coverage gates now cross-check any
  registered accessor sidecar against the freezer segment contents, so
  format-valid but stale offset tables cannot satisfy minimal-mode tail-prune
  or stage-status verification. Signed/local manifest verification
  now runs the same chain-freezer sidecar cross-checks for active
  `chain-index` and `chain-freezer-accessor` refs, so
  `gtron snapshot verify`, fetch, bootstrap, and restore fail before trusting a
  catalog whose sidecars are format-valid but point at different freezer
  contents.
- Minimal-mode runtime now registers a chain-freezer tail-prune lifecycle when
  the local freezer is open. It advances the freezer's virtual tail only after
  the planner allows it and cold segment coverage is readable. When the
  underlying freezer supports it, the same pass now calls `PruneTailFiles` to
  physically delete fully-hidden data shards; table tests prove old files
  disappear, retained in-shard rows remain readable, all-hidden head shards are
  truncated, and the rewritten index survives reopen.
- `gtron snapshot restore` now has CLI-level regression coverage for the cold
  chain path: a signed catalog restores chain-freezer + `chain-index` sidecars,
  installs the canonical boundary, and a restarted `BlockChain`/`TronBackend`
  resolves historical block, transaction, transaction-info, and state-root reads
  with the historical hot lookup rows absent. The same test now also drives real
  `/wallet`, `/walletsolidity`, `/walletpbft`, and JSON-RPC HTTP handlers
  (`getblockbyid`, `getblockbynum`, `gettransactionbyid`,
  `gettransactioninfobyid`, `gettransactioninfobyblocknum`,
  `eth_getBlockByHash`, and `eth_getTransactionReceipt`) against that restored
  cold backend, including solidity/PBFT bound-block paths.
- The same restore regression now seeds minimal latest-account, code, and
  contract-storage snapshots plus state-domain history, restores them through
  the signed catalog path, deletes the restored hot StateDomainChange/StateTxRange
  rows, and checks archive reads through both `TronBackend` and JSON-RPC
  (`eth_getBalance`, `eth_getCode`, and `eth_getStorageAt`) for a historical
  block. This proves cold state-domain history and restored latest/code/storage
  snapshots are usable by archive RPC, not only by in-memory unit fixtures.
- TRON solidity/PBFT account routes are covered on the same restored backend:
  the test pins solid/PBFT bounds to the historical block, removes restored hot
  history rows, then verifies `/walletsolidity` and `/walletpbft`
  `getaccount`/`getaccountresource`/`getreward` succeed through cold
  state-domain history. The restored API matrix now also includes a second
  account deleted at the snapshot head: its latest-domain row is absent, hot
  history rows are removed, and backend, TRON HTTP, and JSON-RPC archive reads
  still reconstruct the block-1 account from cold StateDomainChange history
  while head reads return the deleted/zero state. The same restore soak now
  includes a recreated contract account whose `AccountKVGeneration` changes at
  the snapshot head: old-generation storage rows remain physically present in
  restored latest files, but backend and JSON-RPC archive reads prove block-1
  storage is recoverable while head/block-2 reads do not leak untouched old
  slots into the recreated contract. Contract metadata uses the same
  account-as-of generation path, so delete/recreate history no longer depends
  on a live `KVGeneration` row: backend `GetContractAt` and `/wallet*`
  `getcontract` now prove the restored block-1 contract metadata differs from
  the head contract metadata for the same address. SystemDelegation
  latest-domain rows are also restored in that matrix: V2 delegation buckets
  plus the delegation index are read back through backend calls and the `/wallet*`
  `getdelegatedresourcev2`/`getdelegatedresourceaccountindexv2` HTTP routes.
  The same restored backend now also starts the real TRON HTTP and JSON-RPC
  server lifecycles on port `0`, discovers their bound addresses, and sends
  archive API requests through those listeners instead of only through
  in-memory `httptest` handlers. gRPC `WalletSolidity` account and reward
  methods now also dispatch through the solid-block `GetAccountAt`/`GetRewardAt`
  backend paths instead of live-head reads, with tests pinning that route to
  the shared archive/as-of state session boundary. Delegation V2 solidity/PBFT
  HTTP routes and gRPC `WalletSolidity` delegation methods now use
  `GetDelegatedResourceV2At`/`GetDelegatedResourceAccountIndexV2At` at the
  solid/PBFT bound instead of live-head SystemDelegation rows. Backend archive
  coverage writes V2 delegation buckets and delegation indexes through temporal
  SystemDelegation history and verifies block-1/block-2 as-of reads diverge.
  HTTP solidity/PBFT `getaccountbyid` now dispatches through
  `GetAccountByIdAt`, and gRPC `WalletSolidity.GetAccountById` supports the
  `account_id` path through the same solid-block archive session. Backend
  coverage now also exercises temporal `SystemAccountIndex` history by resolving
  the same account ID to different accounts at block 1 and block 2. TRON HTTP
  and gRPC account/account-id handlers now preserve empty account responses only
  for explicit account misses; cold state-history or account-id reconstruction
  errors surface as HTTP 500 / gRPC Internal instead of being disguised as
  absent accounts. The `PersistentHistoryReader` live/head flat-latest path now
  applies the same rule to corrupt account envelopes and account protobufs,
  returning archive data errors instead of silently returning a missing account.
  TRON HTTP and gRPC contract handlers follow the same
  error-preserving rule, keeping explicit contract misses as empty/NotFound
  responses while surfacing latest/cold contract metadata read failures as
  HTTP 500 / gRPC Internal; the underlying contract history reader now also
  treats corrupt contract metadata protobuf as an archive data error. TRON HTTP
  `getaccountnet` now applies the same
  split: explicit account misses still return `{}`, while cold account-net or
  dynamic-property reconstruction failures return HTTP 500. TRON HTTP
  `getproposalbyid` now preserves `{}` only for explicit proposal misses and
  surfaces cold proposal-history reconstruction failures as HTTP 500. The
  underlying proposal history reader now also treats corrupt proposal JSON and
  malformed proposal-index bytes as archive data errors instead of silently
  returning missing proposal data. TRC10 asset history reads likewise surface
  corrupt rooted asset metadata as archive data errors instead of silently
  returning missing asset data. HTTP
  solidity/PBFT `getaccountnet` now dispatches through `GetAccountNetAt`, whose
  backend implementation reconstructs account bandwidth usage from the shared
  archive session and reuses the same dynamic-property history boundary as
  `getaccountresource`. HTTP solidity/PBFT market queries
  (`getmarketorderbyid`, `getmarketordersfromaccount`,
  `getmarketpricebypair`) and gRPC `WalletSolidity` market methods now dispatch
  through `SystemMarket` history at the solid/PBFT bound instead of live-head
  market rows. Backend coverage writes market order/account-order/price-list
  rows through temporal `SystemMarket` history, verifies block-1/block-2
  as-of reads diverge, and the state history reader now surfaces corrupt market
  order/account-order/price-list protobuf payloads as archive data errors. HTTP
  solidity/PBFT `listexchanges` and gRPC
  `WalletSolidity.ListExchanges` now dispatch through `SystemExchange` history
  at the bound; the backend reads `latest_exchange_num` and
  `allow_same_token_name` from the same historical dynamic-property snapshot so
  pre-fork reads enumerate V1 exchanges and post-fork reads enumerate V2
  exchanges, matching java-tron's final-store selection. The state history
  reader now surfaces corrupt V1/V2 exchange protobuf payloads instead of
  silently dropping them from point/list archive reads. HTTP solidity/PBFT
  TRC10 asset metadata routes (`getassetissuebyid`, `getassetissuebyname`,
  `getassetissuelist`, `getpaginatedassetissuelist`) and gRPC
  `WalletSolidity` asset methods now dispatch through `SystemAsset` history at
  the bound; the backend reads historical `token_id_num` and
  `allow_same_token_name` so pre-fork list/name reads use legacy records while
  post-fork reads use V2 records. HTTP solidity/PBFT `getchainparameters` and
  `getnextmaintenancetime` now also read historical `SystemDynamicProperty`
  rows at the solid/PBFT bound instead of live-head dynamic properties. gRPC
  `WalletSolidity` Stake 2.0 resource availability queries
  (`GetCanDelegatedMaxSize`, `GetAvailableUnfreezeCount`,
  `GetCanWithdrawUnfreezeAmount`) now use solid-bound archive account and
  `SystemDelegation` history instead of live-head state.
  Witness/vote history reads also surface corrupt witness-capsule and pending
  vote protobuf payloads as archive data errors instead of treating them as
  missing witness/vote records.
  gRPC `WalletSolidity.GetBurnTrx`, `GetBandwidthPrices`, and
  `GetEnergyPrices` now likewise read solid-bound `SystemDynamicProperty`
  history, including string-typed price-history rows, instead of live-head
  dynamic properties.
- Backend-level cold state-domain snapshot coverage now also records
  `GetAccountResourceAt` and `GetRewardAt` answers before hot history pruning,
  deletes the hot StateDomainChange/StateTxRange rows for the covered blocks,
  and then rechecks account resource usage, DP-derived limits, and witness
  reward/allowance through the cold history manager.
- The remaining production chain `rawdb.ReadBlockKV` call sites have been
  eliminated outside rawdb itself. TVM `BLOCKHASH`/legacy `CHAINID` now resolve
  through freezer-aware strict `BlockHashByNumber`/`ReadBlockHashByNumber`
  paths when available, and the pruning finish-stage guard uses freezer-aware
  canonical hash lookups. The remaining no-error block-hash helper calls are
  limited by source audit to the `blockbuffer` compatibility method and the
  actuator legacy genesis-hash fallback. A rawdb
  source audit test now fails on new production `rawdb.ReadBlockKV` calls, and
  the remaining `ReadBlockHashByNumber` allowlist is function-scoped rather
  than file-scoped, so a new direct call in the same source file is still
  rejected unless it becomes an explicit audited boundary. The focused
  `scripts/dev/audit_hot_only_reads.sh` entrypoint runs these hot-only and
  cold-boundary source audits without requiring developers to remember the
  rawdb package test regex. The same audit suite
  separately pins the raw freezer copy helpers, including the strict raw block
  and transaction-info readers, to explicit audited boundaries: live freezer
  append through `cmd/gtron/freezer_adapter.go`, plus the blockbuffer's strict
  buffered-block hash fallback before it consults its cold reader. Actuator
  historical compatibility gates now resolve chain identity through
  `Context.EffectiveGenesisHash`, and
  the same audit suite prevents actuator code from reintroducing scattered
  direct `rawdb.ReadBlockHashByNumber(ctx.DB, ...)` calls that would depend on
  hot genesis block rows after freezer/prune. The audit suite also pins all
  remaining production `ReadBlockHashByNumber` calls to explicit freezer/cold
  index boundaries (`blockbuffer`, TVM `BLOCKHASH`, pruner stage verification,
  snapshot builder canonical-hash readers, freezer adapter, and db diagnostics),
  and the cold-boundary audit now covers the strict block-hash reader too, so
  new block-number hash lookups cannot silently bypass cold chain coverage.
  The same source-audit file now also rejects new production
  `rawdb.NewChainDB(..., rawdb.NoopAncient{})` constructors outside audited
  isolated replay/diagnostic boundaries, preventing new call sites from
  creating hot-only chain readers that skip ancient freezer and cold index
  sidecars by construction. The same cold-boundary audit now covers the
  integrity-preserving strict readers for block numbers, transaction indexes,
  transaction infos, state roots, section blooms, balance traces, and account
  traces, so new production code cannot call those variants on a hot-only KV
  store and accidentally bypass registered cold sidecars.
- `gtron db storage-alerts --json` now packages freezer, stage, and snapshot
  health into one machine-readable report for soak/production monitors. The
  JSON form preserves the same critical exit semantics as the text command and
  includes freezer/stage/snapshot detail arrays plus hidden-freezer and retired
  snapshot counters. It also reads the persisted `history-prune-mode-v1` row,
  reports `modeAlert*` plus `pruneMode*` fields, treats persisted-mode storage
  read errors as critical instead of an unknown mode, and fails when stage
  progress contradicts mode semantics, such as `archive` datadirs with
  hot-prune, lookup-prune, or tail-prune progress, or non-`minimal` datadirs
  with freezer-tail prune progress. The Nile sampler and storage benchmark
  harness request the JSON form while retaining a legacy text parser fallback.
- `gtron db storage-alerts --prometheus` now exposes the same aggregate storage
  alert state as Prometheus text metrics for external monitor scrape jobs:
  overall/component status values, component issue counts, per-component
  issue-kind counts, hidden freezer bytes, retired snapshot counters, and the
  persisted prune mode.
- `scripts/dev/storage_benchmark.sh` now writes a
  `<mode>-<role>-storage-alerts.prom` artifact for every storage-alert gate and
  records its path as `storageAlertPrometheus` in the JSONL result row. Critical
  storage states still emit the Prometheus metrics before the harness exits
  non-zero.
- `scripts/dev/nile_sync_sample.sh --offline-db-check` now writes the same
  storage-alert Prometheus metric payload next to the JSONL output by default,
  records `offlineDbCheckPrometheus*` fields, and supports an explicit
  `--storage-alert-prometheus-file` path for long-running Nile scrape jobs.
- The storage benchmark and Nile acceptance checkers now require readable
  Prometheus artifacts to expose both `gtron_storage_alert_status` and the
  normalized `gtron_storage_alert_issue` metric family, so production soak
  gates catch older binaries or scrape payloads that cannot route freezer,
  stage, mode, and snapshot issue kinds. When JSONL alert details include a
  structured `kind`/`severity`, the same checkers now require a matching
  `gtron_storage_alert_issue{component,kind,severity}` series in the captured
  artifact. Nile sync acceptance can also require the latest selected sample to
  carry stage-stall diagnostics and successful historical JSON-RPC archive-read
  evidence through `stageStalled*`/`stageStalls` and `archiveApi*` fields. The
  storage benchmark acceptance checker can now require the same historical
  transaction and receipt archive proof with `archiveApiTx*` fields and
  `--require-archive-tx-evidence`.

Needed:

- Add longer-running node/server bootstrap soak tests that start the whole node
  stack after `gtron snapshot restore` and exercise a broader `/wallet*`,
  JSON-RPC, and archive-read matrix, especially multi-account contract,
  broader delegation flows, and broader account delete/recreate fixtures beyond
  the current real HTTP/JSON-RPC lifecycle smoke plus
  balance/code/storage/account/resource/deleted-account/recreated-contract/
  SystemDelegation-latest checks.
- Keep auditing newly introduced direct hot-only `rawdb.Read*KV` and
  raw block-hash fallback call sites before enabling more aggressive
  chain-data prune defaults.
- `scripts/dev/snapshot_manifest_profile.py` now profiles active snapshot
  manifest bytes by payload versus lookup sidecar family and can gate runs with
  `--max-sidecar-share-milli` or `--max-family-sidecar-share-milli`; use that
  evidence to evaluate compact/merged cold index formats for block hash by
  number, tx lookup, per-tx info, and state-root lookup only if sidecar
  profiles show they dominate disk or lookup latency.
- Keep only recent chain data and wallet-hot indexes in Pebble under full/snap
  modes.
- Wire the storage benchmark and Nile sampler Prometheus artifacts into the
  external monitor/alert routing used for long Nile/mainnet soaks.
  Catalog/freezer sidecar mismatch is now caught by signed/local manifest
  verification as well as tail-prune/stage-status coverage gates.

### P1: Operator Mode Semantics

go-tron's `archive/full/snap` does not yet match Erigon's mode matrix.

Recommended TRON mapping:

| Mode | State history | Block history | Use case |
| --- | --- | --- | --- |
| `archive` | all | all | RPC providers and historical state APIs |
| `full` | recent window | freezer/snapshotted old blocks | default full node |
| `blocks` | recent state only | all blocks | indexers needing transaction/block history |
| `minimal` | minimal recent state | minimal recent blocks | validator/SR constrained disk mode, only if TRON APIs can tolerate it |
| `snap` | recent hot state plus cold local state files | freezer/snapshotted old blocks | go-tron-specific mode for local immutable state snapshots |

Status:

- `--prune.mode` is now accepted as the Erigon-style CLI entry point.
- `--gcmode` remains a deprecated alias.
- The runtime pruning policy now preserves distinct internal mode labels for
  `full`, `blocks`, `minimal`, `snap`, and `archive`. `blocks` and `minimal`
  share `full`'s finite state-history retention window for hot
  `StateDomainChange`/`StateTxRange` pruning, and the Worker/Checker paths now
  enforce and audit that retention without collapsing their mode labels.
  Archive/as-of state queries also apply the same local history-window gate for
  `full`, `blocks`, and `minimal`, so requests below the retained floor fail
  with `ErrArchiveHistoryPruned` instead of reconstructing from incomplete or
  mode-inconsistent history.
- `blocks` preserves complete local chain-freezer history while still allowing
  state/history and hot lookup pruning. `minimal` is the only mode that
  registers a chain-freezer tail-prune lifecycle. It applies virtual-tail
  hiding and physical freezer shard reclamation after verified freezer/index
  stage progress, event-log cold coverage, and cold chain-freezer segment
  coverage are visible; if accessor sidecars exist, that coverage includes
  byte-for-byte offset validation against the freezer segment. Cold
  chain-freezer segment reads are available as a safety fallback, with
  `chain-freezer-accessor` sidecars covering non-scan block-number point reads
  for newly built snapshots. Regression coverage now locks this mode gate and
  runs a restart drill: after physical freezer file deletion and local freezer
  reopen, old blocks remain unavailable locally but readable through the cold
  snapshot fallback, while retained recent blocks still read from the local
  freezer. Successful tail-prune passes now also write
  `StageSnapshotChainFreezerTailPrune`, whose block value is `tail-1`, and log
  the same `prunedThroughBlock` boundary plus the count of physically reclaimed
  data files.
- `snap` keeps go-tron's cold-snapshot-gated hot pruning. State-domain hot
  history pruning now builds its coverage map only from registered history
  segments whose on-disk checker succeeds; if a manifest advertises a covered
  range but the history file is missing or corrupt, pruning aborts before
  deleting the hot `StateDomainChange` rows.
- `archive` keeps every temporal state row and auto-enables history capture.
- The selected mode is persisted in rawdb as `history-prune-mode-v1`; startup
  rejects incompatible mode changes for an existing datadir and also rejects
  mode/stage contradictions before services start, e.g. `archive` datadirs
  with hot-prune/lookup-prune/tail-prune progress or non-`minimal` datadirs
  with freezer-tail prune progress.
- Chain lookup pruning is already wired for `full`, `blocks`, `minimal`, and
  `snap` once verified chain-freezer/index snapshot coverage exists; `archive`
  keeps hot lookup rows.
- `scripts/dev/storage_benchmark.sh` is now the repeatable measurement harness
  for producer-time, follower sync catch-up, and datadir size split by hot
  Pebble, ancient freezer, and state snapshots across prune modes. Its default
  producer matrix covers `full`, `blocks`, `minimal`, and `archive`. Its
  `--signed-cold-prune` drill builds cold chain-freezer coverage, signs the
  catalog, prunes hot chain lookup rows through the verified signer for
  `blocks`/`minimal`, and restarts only `minimal` so the tail-prune lifecycle
  can report the post-prune boundary. The matching runbook is
  `docs/dev/erigon-storage-benchmark.md`, with the first smoke sample recorded in
  `docs/dev/erigon-storage-benchmark-results-2026-06-10.md`. The harness can
  now also enable history capture and run `snapshot build-derived-indexes`
  after producer shutdown, recording the derived-index cold coverage boundary,
  active segment count, and build time in the same JSONL row. When the signed
  cold-prune drill runs with derived indexes enabled, it also signs the updated
  manifest and runs the verified balance-trace and section-bloom hot-row prune
  commands, recording reclaimed block trace, account trace, and bloom rows. The
  signed cold-prune drill also runs `gtron snapshot prune-retired` and records
  retired snapshot segment count, deleted/missing/skipped files, and reclaimed
  bytes through `retiredPrune*` JSON fields. Each sample now also runs
  `gtron db storage-alerts` before the JSONL row is emitted and records
  `freezerAlertStatus`, `freezerAlertIssues`,
  `freezerAlertHiddenBytes`, `freezerAlertDetails`, `stageVerifyStatus`,
  `stageVerifyIssues`, `stageVerifyDetails`, `modeAlertStatus`,
  `modeAlertIssues`, `modeAlertDetails`, `pruneMode`, `pruneModePersisted`,
  `snapshotAlertStatus`, `snapshotAlertIssues`, `snapshotAlertDetails`, and the
  `snapshotRetired*` counters, so critical freezer, stage, mode, prune, and
  cold-coverage regressions
  are emitted as `status=storage-alerts-critical` rows before the harness exits
  non-zero. Warning rows keep the exact alert detail in JSONL without failing
  the run.
- `scripts/dev/storage_benchmark_acceptance.py` now gates collected JSONL runs
  before production sign-off: it enforces required prune-mode coverage, clean
  storage-alert statuses, readable storage-alert Prometheus artifacts,
  minimal-mode signed cold lookup prune plus tail-prune evidence, and
  project-specific numeric `--min`/`--max` thresholds. Its optional
  `--require-prune-mode-semantics` gate also checks the Erigon-style mode
  matrix directly: persisted prune mode must match the sampled mode, `archive`
  rows must not show hot/cold prune progress, and `blocks`/`full`/`snap` rows
  must not show freezer-tail pruning.

Remaining:

- Promote `blocks`/`minimal` block-retention behavior from tested primitives
  and dev harness coverage to production-complete status. The freezer and
  chain-freezer table set now have the tested virtual-tail primitive, physical
  shard reclamation primitive, planner, runtime apply path, and cold
  chain-freezer segment fallback reads with block-number accessors. Minimal
  tail pruning now also requires continuous cold snapshot coverage across the
  entire pruned range, and the benchmark drill can compare `blocks` lookup
  pruning without freezer-tail truncation against `minimal` tail pruning. The
  remaining gap is collected long-running soak/space samples and concrete
  project threshold values before old block file reclamation is considered
  production-complete under `minimal`.

### P2: Derived Domains And RPC Indexes

Erigon also reduces RPC cost by moving logs, traces, receipts, and lookup
indexes into domain/index families. go-tron should register TRON equivalents:

- transaction info / retstore cold files and accessors
- section bloom / event log indexes
- account trace and balance trace history
- PBFT/finality read models where deterministic or explicitly runtime
- event/log and account-oriented cold accessors beyond the chain-index sidecar

Keep derived/runtime data out of the internal full-state root.

### P2: Parallel Execution And Prefetch

Erigon's `ExecV3` can run with a parallel executor and SharedDomains. TRON block
execution has stronger java-tron ordering constraints, so this must be staged:

- First, prefetch accounts, code, storage, witness/system KV, and contract
  metadata from upcoming txs.
- Then parallelize signature decoding, static validation, receipt/log indexing,
  and cold reads.
- Only parallelize state mutation when a conflict detector proves the tx sets are
  independent and the final journal order is byte-identical to serial java-tron
  execution.

Status:

- `core/state/prefetcher.go` now provides the first race-safe prefetch driver:
  worker goroutines warm raw latest-domain account, account-KV, contract code,
  and contract storage reads through `ethdb.KeyValueReader`, with bounded
  non-blocking enqueue and hit/miss/drop/error stats. It deliberately avoids
  mutating `StateDB` object caches because those maps do not yet have a
  concurrent access model.
- `actuator.PrefetchKeysFor(tx)` now extracts deterministic envelope-derived
  hints for account latest rows, contract metadata and trigger-contract code
  rows, metadata-derived contract-origin account rows, system delegation rows,
  witness/proposal/brokerage rows, pending vote rows, TRC10 `SystemAsset`
  metadata/index rows, and envelope-addressable `SystemMarket`/`SystemExchange`
  rows. It covers transfer, TRC10 transfer/issue/participate, TVM
  create/trigger, contract settings, vote witness, witness operations,
  governance proposals, Stake 1.0/2.0, shielded transparent endpoints, market
  sell/cancel including metadata-derived maker order rows behind compatible
  price levels, exchange token operations including metadata-derived TRC10
  asset rows for both exchange token legs, owner-only actuators, and
  account-create families.
  The detailed audit lives in `docs/dev/state-prefetch-keys.md`.
- `core/state_processor.go::ProcessBlock` now has opt-in lookahead wiring:
  `BlockChain.applyBlock` enables the prefetcher only when
  `params.ChainConfig.StatePrefetchEnabled` is true, while the public
  `ProcessBlock` helpers retain the previous prefetch-off behaviour. CLI and
  TOML controls are available through `--state.prefetch.enabled`,
  `--state.prefetch.workers`, `--state.prefetch.lookahead`, and
  `[state.prefetch]`.
- `core/state_processor_prefetch_bench_test.go` now provides focused
  `BenchmarkProcessBlock_HeavyTRX_HeavyState` and
  `BenchmarkProcessBlock_HeavyTRX_ColdState` variants for prefetch off and
  worker counts 2/4/8. The operator runbook is
  `docs/dev/state-prefetcher.md`.
- `scripts/dev/state_prefetch_benchmark.sh` now provides the repeatable sweep
  harness for these benchmarks, recording commit/environment metadata, raw Go
  benchmark output, and optional `benchstat` summaries.

Remaining:

- Collect full benchmark sweep samples across representative machines and add
  Nile/mainnet replay evidence before enabling by default.

### P2: General ETL Layer

Snapshot builders stream today, but large backfills and derived indexes still
need a general sorted-ingestion layer.

Status:

- `core/rawdb/etl` now provides a generic temp-run collector for unordered
  `Put`/`Delete` operations. It spills sorted runs once the memory buffer fills,
  k-way merges runs by key, collapses duplicate keys with latest-input-wins
  semantics, and loads the final key-ordered stream through
  `ethdb.KeyValueWriter` with optional `ethdb.Batcher` support.
- Unit coverage pins sorted output order, duplicate-key collapse, delete
  tombstones, multi-run spills, batch flushing, and temporary-directory cleanup.
- `snapshots.Manager.RestoreLatest` now restores latest-domain snapshot
  segments through one collector and loads the resulting rawdb writes in
  physical key order. Regression coverage forces manifest segment order to
  differ from physical key order and asserts the final restore stream is sorted.
- `snapshots.Manager.RestoreStateDomainHistory` now restores StateDomainChange
  hot rows, inverse indexes, and StateTxRange rows through one collector and
  loads the resulting rawdb writes in physical key order. Regression coverage
  proves the previous direct row/index/tx-range order would be unsorted and the
  final restore stream is sorted.
- `snapshots.RestoreChainFreezerIndexes` now restores historical block-hash,
  transaction-hash, and per-transaction-info hot lookup rows through one
  collector and loads the resulting rawdb writes in physical key order.
  Regression coverage proves the previous direct block/tx/info order would be
  unsorted and the final restore stream is sorted.
- `snapshots.BuildChainIndexSegmentFromChainFreezerSegment` now builds
  chain-freezer `chain-index` sidecars through the collector and streams the
  sorted block-hash and transaction-hash tables into the existing binary
  sidecar format. `BuildChainIndexSegmentFromChainFreezerSegmentWithOptions`
  and `Aggregator.BuildChainFreezerWithOptions` expose the same ETL scratch
  controls for large freezer snapshot builds.
- `rawdb.DerivedIndexCollector` now provides a typed collector-backed bulk-load
  surface for replay-derived RPC indexes: transaction lookup/info rows,
  account trace, balance trace, and section bloom rows. Future backfill commands
  can add rows in block execution order and still load the final rawdb stream in
  physical key order.
- `rawdb.RebuildTransactionDerivedIndexesFromBlocks` now rebuilds transaction
  reverse lookup, per-block `TransactionRet`, and per-tx `TransactionInfo` rows
  from retained blocks plus hot or ancient per-block info rows through
  `DerivedIndexCollector`. Blocks without retained `TransactionRet` can still
  rebuild tx-hash reverse lookup rows from the canonical block body, but
  present per-block info rows must match the canonical block transaction count,
  block number, and tx hash order before the rebuild republishes per-block or
  per-tx info rows.
- `gtron db rebuild-tx-indexes` exposes that transaction lookup/info rebuild to
  operators. It opens hot Pebble plus read-only ancient freezer rows, supports
  explicit or head-derived block ranges, and routes the rebuild through sorted
  ETL scratch-space options.
- Section-bloom key encoding now matches java-tron's
  `Long.toHexString(section*1_000_000 + bitIndex)` layout, and
  `rawdb.RebuildSectionBloomsFromTransactionInfos` rebuilds compressed
  java-compatible section bitsets from retained block `TransactionInfo.log`
  payloads through `DerivedIndexCollector`. Partial-range rebuilds preserve
  existing block bits in the same section by reading and ORing existing rows.
  Existing hot/cold section-bloom rows are now read through a strict accessor
  in this rebuild path, so hot KV read errors and cold sidecar errors abort the
  rebuild instead of being treated as missing rows that could clear unrelated
  block bits.
  Cold section-bloom snapshot sidecars are accepted only when their block range
  covers complete java-tron bloom sections (`from % 2048 == 0` and
  `(to + 1) % 2048 == 0`), so manifests cannot advertise a partial section as
  safe cold coverage before hot rows are pruned.
  The rebuild now rejects tx-bearing blocks whose `TransactionRet` coverage is
  missing, differs from the block's transaction list length, contains nil
  entries, points at a different block number, or carries a tx id that does not
  match the canonical block transaction at the same index, preventing operators
  from publishing incomplete or mismatched log prefilter rows as a successful
  rebuild.
- `gtron db rebuild-section-blooms` exposes that section-bloom rebuild to
  operators. It opens hot Pebble plus read-only ancient freezer rows, supports
  explicit or head-derived block ranges, and routes the rebuilt `sb-` rows
  through sorted ETL scratch-space options.
- `TronBackend.GetLogs` now consumes section-bloom rows as an optional
  address/topic prefilter before reading block bodies and `TransactionRet`
  payloads. Missing or malformed bloom rows are treated as unknown and fall
  back to the old block scan, preserving correctness for older datadirs until
  the rebuild command has populated the index.
- Wallet HTTP/gRPC `getaccountbalance` and `getblockbalancetrace` now expose
  the existing account-trace and block-balance-trace rawdb rows. Account
  balance lookup follows java-tron's `getPrevBalance` ordering: if the request
  block has no exact account trace, the response reports the newest trace block
  at or before the requested height; if no prior row exists, it returns the
  requested block identifier and zero balance.
- History-enabled canonical block replay now records java-tron-compatible
  `BlockBalanceTrace` transaction operations from actual StateDB balance
  mutations and writes final `AccountTrace` rows for every account whose
  balance changed or was deleted in the block. The recorder follows
  `StateDB.Snapshot`/`RevertToSnapshot`, so reverted TVM/internal-call balance
  changes do not leak into the trace. Synchronous and async commit paths persist
  the trace rows in the block metadata batch, and the rawdb writer now surfaces
  marshal/Put errors instead of silently dropping archive data.
- `rawdb.ChainDB` now has an optional cold account/balance trace reader. The
  existing `ReadBlockBalanceTrace`, `HasBlockBalanceTrace`,
  `ReadAccountTrace`, and `ReadAccountTraceAtOrBefore` accessors prefer hot
  rawdb rows and fall through to that reader on misses, choosing the newest
  account trace across hot and cold sources for java-tron `getPrevBalance`
  semantics. A registered `balance-trace` snapshot segment now freezes hot
  `BlockBalanceTrace` protobuf rows plus fixed-width account trace index rows,
  `snapshots.Manager` implements the cold reader, and runtime startup attaches
  it to `ChainDB` alongside the chain-index sidecar. Strict block-balance-trace
  reads are now exported and the backend API uses them, so malformed or
  mismatched cold trace rows surface as archive data errors instead of being
  collapsed into a false "trace not found" result. When the canonical block is
  readable, strict hot/cold `BlockBalanceTrace` reads also bind any embedded
  block identifier hash to the canonical block hash before archive APIs expose
  the payload; non-empty embedded hashes must be full 32-byte block hashes even
  when the canonical block has already been pruned from local hot storage.
  Balance-trace block identifiers now resolve their canonical block through the
  same strict ChainDB block accessor, and account-trace responses use that
  strict path when translating an older trace row back to its block identifier,
  so corrupt hot/freezer block bodies surface as data errors instead of
  misleading "block not found" or false response identifiers.
- DB maintenance commands that rebuild, audit, or replay derived rows now open
  the same snapshot-aware `ChainDB` view as runtime startup. Balance-trace
  replay backfill uses that archive view when checking existing target rows, so
  a pruned hot trace that is still present in a verified cold sidecar is counted
  as existing instead of being duplicated back into Pebble.
- The standalone `cmd/balance-trace` and `cmd/reward-trace` diagnostics now
  open the same `state-snapshots` manager, wrap the chain freezer with snapshot
  fallback, and route transaction-info, account-trace, and block-balance-trace
  reads through the cold-sidecar-aware `ChainDB` boundary. `reward-trace` also
  attaches the same chain-index, section-bloom, and event-log sidecars so its
  block timestamp and cycle scans can cross hot-pruned block/index ranges. The
  diagnostics use strict block-number, state-root, and block-body reads, and
  the balance diagnostic also uses the strict transaction-info and trace
  readers for printed rows, so malformed hot rows or corrupt/mismatched cold
  sidecars fail the diagnostic instead of being reported as missing receipt,
  missing balance data, or a false genesis-root fallback. This keeps historical
  balance and reward investigations usable after chain lookup and balance trace
  pruning.
- Exact cold `BlockBalanceTrace` lookups skip balance-trace segment files whose
  block range cannot contain the requested block before opening them. This
  keeps unrelated missing or retired newer trace sidecars from blocking older
  archive reads, while in-range missing segments still surface as data errors.
- `gtron snapshot build-balance-traces --snapshot.from-block --snapshot.to-block`
  now audits canonical block coverage before building, rejects missing or
  mismatched `BlockBalanceTrace` rows, builds those registered cold trace
  segments from local rawdb trace rows, validates/records the same snapshot
  chain identity used by freezer snapshots, and leaves Wallet/API callers on
  the existing rawdb accessors.
- `gtron snapshot prune-balance-traces` now deletes hot account/balance trace
  rows only after verifying the signed snapshot catalog and registered
  `balance-trace` segments. The prune preflights every hot row in the covered
  range against the cold segment and aborts before deletion if any row is
  missing or differs, so archive reads can move to cold storage without silent
  trace loss.
- Production snapshot/prune lifecycle now runs balance-trace hot-row pruning
  with a persisted `SnapshotBalanceTracePrune` stage, so covered
  `BlockBalanceTrace` and account-trace rows are reclaimed across restarts
  without rescanning already processed segments. Snap-mode history passes now
  also publish matching `balance-trace` sidecars when the same block range has
  complete hash-matching hot `BlockBalanceTrace` coverage plus exact-height hot
  `AccountTrace` rows for every touched account; incomplete legacy ranges are
  skipped rather than publishing false cold coverage, and mismatched block
  traces fail the pass before any hot trace rows are pruned.
- `rawdb.ChainDB` now also has an optional cold section-bloom reader. Registered
  `section-bloom` snapshot segments freeze java-tron-compatible `sb-` rows by
  source block range, `snapshots.Manager` serves those rows after hot misses,
  and runtime startup attaches the manager so `TronBackend.GetLogs` can keep
  using address/topic bloom prefilters after hot rows are reclaimed.
- `gtron snapshot build-section-blooms --snapshot.from-block
  --snapshot.to-block` builds registered cold section-bloom segments from local
  hot `sb-` rows and records the same signed-snapshot chain identity as freezer
  and trace sidecars.
- `gtron snapshot build-derived-indexes --snapshot.from-block
  --snapshot.to-block` builds balance-trace and section-bloom sidecars together
  and integrates them into one manifest generation, while retaining the
  pre-freeze balance-trace coverage audit.
- Registered cold `event-log` snapshot segments can now be built from retained
  canonical blocks plus hot or ancient `TransactionRet` payloads.
  `snapshots.Manager` exposes address/topic-filtered event-log iteration over
  those immutable files, and v2 segments carry segment-local address plus
  positional-topic postings so filtered cold reads avoid scanning unrelated log
  payloads. A registered `event-log-index` sidecar now maps address/topic keys
  to candidate cold event-log segment starts, so filtered manager reads can
  skip unrelated immutable segment files before using the segment-local
  postings. The manager walks continuous multi-segment coverage in block order,
  clips each segment to the query range, and has regression coverage for
  address/topic filters spanning adjacent immutable segments plus index-backed
  skipping of unrelated segments. Manifest validation rejects orphaned,
  prefix-missing, suffix-missing, or gapped `event-log-index` sidecars unless
  the referenced block range is continuously covered by active `event-log`
  segments. Indexed coverage now also replays the active event-log segments
  and compares the rebuilt address/topic postings with the registered
  `event-log-index` sidecar, so format-valid but stale or incomplete sidecars
  cannot satisfy archive log coverage gates. Covered filtered archive reads
  also cross-check global index candidates against readable segment-local
  lookup postings, rejecting stale sidecars that would otherwise omit a
  matching event-log segment while still skipping missing non-candidate files.
  Non-covered manager iteration can still fall back to a full cold event-log
  segment scan when the immutable event-log segments continuously cover the
  request range, so stale global index sidecars degrade performance without
  producing a false empty result.
  `gtron snapshot build-event-logs`
  exposes the standalone operator build path, while `gtron snapshot
  build-derived-indexes` now emits event-log and event-log-index sidecars
  together with balance-trace and section-bloom segments. Production snap-mode
  history passes can now publish matching
  event-log and event-log-index sidecars in the same manifest generation as the
  state-history segment before hot prune runs, and both production and manual
  event-log builders now advance a hash-bound `SnapshotEventLogBuild` stage
  only when indexed cold log coverage exists as a continuous prefix from block
  1, so operators can audit it against the verified `Finish` boundary without
  trusting a height-only cold coverage row across forks. Snapshot
  restore/bootstrap also derives that stage from verified manifest `event-log`
  plus `event-log-index` coverage, combining adjacent indexed sidecars into one
  continuous block-1 prefix, so restored nodes can continue minimal-mode tail
  pruning without locally rebuilding log sidecars. Runtime startup registers the
  manager on
  `ChainDB`, and
  `TronBackend.GetLogs` now pushes address/topic filters into cold coverage
  checks so index-covered archive reads verify only candidate immutable segments
  before streaming cold logs. Filtered manager queries now compose adjacent
  `event-log-index` sidecars across the request range, so a query that spans
  multiple immutable index passes can still skip unrelated event-log segment
  files instead of falling back to a full cold scan. Cold coverage verification
  and event-log iteration can now run through one `ChainDB` boundary backed by
  a single snapshot-manager manifest view, so an archive log query cannot mix a
  passing coverage check from one immutable view with rows from a later
  uncovered view. That covered boundary also rejects nil, out-of-range, or
  non-monotonic cold event-log rows before API code can silently drop or
  reorder them after hot receipt rows have been pruned. Runtime event-log
  segment iteration and the `ChainDB` covered cold-log boundary now also
  recheck each payload address against the row/index metadata, so a corrupted
  cold payload cannot be returned under a stale address posting after hot
  receipt rows are gone. When the canonical block is readable, covered cold log
  rows must also carry the canonical block hash for their block number and a
  transaction hash/index that matches the canonical block body; when the
  canonical `TransactionRet` row is readable, the row's block-wide log index
  and payload must match the canonical receipt log too.
  Segment-local address/topic lookup candidates must still match the requested
  filter at query time too: stale postings now fail as cold-index corruption
  instead of degrading into a false empty archive-log result. Backend and
  JSON-RPC
  `eth_getLogs` regressions delete hot `TransactionRet` rows and unrelated cold
  segment files to prove filtered archive reads are served through the cold
  index path. The API falls back to the hot scan on coverage gaps and surfaces
  checker failures as archive data errors. `gtron snapshot
  event-log-index-stats` now gives operators a readonly profile of active
  event-log-index sidecars, including address/topic key counts, postings,
  average postings per key, single- versus multi-segment key counts, and
  worst-case candidate segment fanout.
- `gtron snapshot prune-retired` now reclaims physical snapshot files listed in
  the manifest's retired segment set after active segment preflight succeeds,
  without rewriting the signed manifest/catalog view.
- Production snapshot/prune lifecycle now runs retired snapshot file cleanup
  after hot-row prune hooks, so replaced immutable segment files do not
  accumulate indefinitely while the active signed snapshot view stays intact.
- `gtron snapshot prune-section-blooms` verifies the signed catalog and cold
  segment format, compares every hot `sb-` row in the covered section range
  byte-for-byte against the cold segment, and deletes the hot rows only after
  that preflight succeeds.
- Production snapshot/prune lifecycle now runs section-bloom hot-row pruning
  with a persisted `SnapshotSectionBloomPrune` stage, so snap/full/block/minimal
  modes can keep reclaiming covered `sb-` rows across restarts without rescanning
  already processed segments. Snap-mode history passes can also publish
  full-section `section-bloom` sidecars once the state-history cutoff fully
  covers a bloom section, ensuring the later section-bloom prune hook has
  verified cold coverage before deleting whole hot `sb-` rows.
- `rawdb.AuditBlockBalanceTraceCoverage` and
  `gtron db audit-balance-traces` now give operators a pre-freeze coverage
  check for archive trace sidecars: every canonical block in the requested
  range must exist and have a block-balance trace whose payload block
  identifier matches the canonical hash/number. Snapshot stage/cold coverage
  checks also require every block in the claimed balance-trace range to have a
  cold block-trace row, so a sparse sidecar cannot satisfy
  `SnapshotBalanceTracePrune` health checks or advance the balance-trace prune
  stage.
- `core.BackfillBalanceTracesByReplay` and
  `gtron db backfill-balance-traces` now provide a safe historical
  `BlockBalanceTrace`/`AccountTrace` backfill path for old datadirs: the
  command initializes an isolated replay database from the same genesis,
  enables history capture there, replays canonical blocks from the source
  chain, and copies only the generated trace rows back to the operator DB
  through the sorted ETL collector. `--db.replay.dir` makes that replay database
  persistent and resumable from its canonical head; one-shot runs can still use
  `--db.replay.tempdir`. Existing differing trace rows are rejected unless the
  operator explicitly passes `--db.balance-trace.overwrite`. Replay output and
  target hot/cold `BlockBalanceTrace` and exact `AccountTrace` rows are now
  checked through strict rawdb trace readers, so malformed, block-mismatched,
  hot rows that fail to read, or unreadable cold trace payloads fail the
  backfill instead of being treated as missing rows and republished into Pebble.
  Account-trace cold lookups that return a row above the requested block are
  also surfaced as sidecar data errors on strict/at-or-before reads. Source
  canonical blocks and snapshot-seeded replay-window state roots are now read
  through strict block/state-root accessors as well, so corrupt freezer or
  chain-index state cannot seed a replay database with a silently incomplete
  recent execution window.
- `gtron db backfill-balance-traces` can also seed an empty/genesis-only replay
  database from a verified signed snapshot before replay starts. Passing
  `--snapshot.dir` plus the trusted catalog key flags restores latest
  state/history through the same manifest verifier used by `snapshot restore`,
  installs the canonical snapshot boundary against local chain data, writes the
  boundary state root/head, and copies the recent block/TAPOS execution window.
  This gives post-boundary archive trace backfills a checkpointed start point
  instead of forcing a fresh replay directory to execute from genesis; blocks at
  or before the snapshot boundary still need retained traces or a replay DB that
  already contains that prefix.
- `rawdb.RebuildAccountTracesFromBlockBalanceTraces` and
  `gtron db rebuild-account-traces` now repair account-trace rows from retained
  java-tron-compatible `BlockBalanceTrace` operation diffs through sorted ETL.
  Partial ranges can use existing prior account-trace rows as their baseline;
  full rebuilds should start at genesis so cumulative balance diffs are
  self-contained.
- `BenchmarkSnapshotRestoreETL` now compares direct unordered restore writes
  against sorted collector loads for latest-domain, state-domain history, and
  chain-freezer lookup restore, and now includes chain-index sidecar build
  direct-memory vs sorted-ETL sub-benchmarks. The first smoke result is
  recorded in `docs/dev/etl-collector-benchmark-results-2026-06-10.md`.
- `BenchmarkDerivedIndexCollector` compares direct block-order writes against
  collector-backed sorted loads for transaction lookup/info, account trace,
  balance trace, and section bloom rows. It also compares direct block-order
  transaction lookup/info rebuild with the collector-backed rebuild path. The
  first smoke result is recorded in
  `docs/dev/etl-derived-index-benchmark-results-2026-06-10.md`.
- Snapshot restore APIs now expose `RestoreETLOptions` so callers can control
  the collector temp directory, buffer limit, and batch size for large
  bootstrap installs.
- `TronBackend` archive/as-of account, balance, code, storage, reward, and
  account-resource reads now share one archive state session. The session opens
  the head-aligned persistent history reader, holds the chain mutex until the
  query is done, and applies the shared block-range/history/prune-window gate
  before any API-specific reconstruction starts. Regression coverage proves the
  lock is held for successful sessions and released when the gate rejects a
  query.
- Source-audit coverage now also enumerates every public block-bound
  `TronBackend` archive/as-of API (`*At` plus `GetStorageAtBlock`) and requires
  each one to pass through `archiveStateAt` or an audited archive-aware helper.
  This keeps new solidity/PBFT/JSON-RPC historical APIs from accidentally
  serving live-head state while the broader archive surface grows.
- TRON HTTP solidity/PBFT source-audit coverage now checks the shared
  `boundFn` handlers directly: state/query handlers must keep paired live and
  `At` backend calls, while block/transaction boundary gates remain explicit
  audited exceptions. The matching wrapper audit also requires each
  `/walletsolidity` and `/walletpbft` wrapper to forward exactly once to its
  expected shared handler with the solid or PBFT bound.
- WalletSolidity gRPC now has the same source-audit boundary: every direct
  `SolidityServer` backend call is enumerated, state/query methods must use the
  solid-bound `At` backend APIs with `s.solidNum()`, and block/transaction
  lookups stay listed as explicit boundary-gate exceptions.
- JSON-RPC source-audit coverage now checks both the reflection `EthAPI` and the
  legacy dispatch handlers: state/execution methods with historical block tags
  must keep paired live and archive backend calls (`GetBalance`/`GetBalanceAt`,
  `Call`/`CallAt`, `EstimateGas`/`EstimateGasAt`, etc.), while explicit
  constants such as TRON's nonce-less `eth_getTransactionCount` stay listed as
  audited exceptions.
- JSON-RPC `debug_traceCall` and `debug_traceTransaction` now use the same
  archive execution-state setup as `eth_call`: historical traces can run from
  cold state-domain/code snapshots after hot block state roots, latest rows, and
  code rows are pruned, and transaction tracing uses strict tx/block readers so
  corrupt archive rows surface instead of looking like misses.
- Event-log archive reads now include backend coverage for block-hash single
  block filters after hot transaction-info rows are removed, proving the cold
  event-log segment path is not limited to from/to range filters. A stronger
  regression also removes the hot block body, block-hash lookup, and
  transaction-info rows, then serves the same `blockHash` log query through
  chain-freezer ancient rows plus the cold `chain-index` and `event-log`
  segments.
- Event-log backend reads now re-check cold sidecar rows against the requested
  block range, optional block hash, address filter, and topic filter before
  rendering JSON-RPC logs. Hot and cold `GetLogs` address matching now share the
  same TRON-address normalization, so 21-byte TVM log addresses filter
  consistently whether the data comes from hot `TransactionRet` rows or cold
  event-log segments.
- Event-log segment builds now use the shared sorted ETL collector. Source
  block scans spill event-log rows to scratch space keyed by
  block/transaction/log position, and the final segment writer streams sorted
  payload/index rows into the immutable segment while retaining only lookup
  postings in memory. Global event-log-index sidecars now feed
  `(address/topic key, segment start)` postings through the same ETL collector
  before serializing lookup maps, deduplicating repeated segment hits by sorted
  key instead of depending on scan-order map appends. Aggregator event-log
  builds can pass the same ETL temp, buffer, and batch knobs used by other
  snapshot restore/build paths, and production cold-builder passes now forward
  `snapshots.Config.ETL` to event-log, event-log-index, section-bloom, and
  balance-trace sidecar builders. The storage benchmark records the same
  event-log-index key/posting/fanout counters after derived-index builds,
  including average postings per key and single- versus multi-segment key
  counts, and the acceptance checker can require that evidence through
  `--require-event-log-index-evidence`, giving larger soaks a concrete
  selectivity signal before revisiting recsplit-style accessors.
- Event-log segment verification now also proves the segment-local address and
  positional-topic lookup maps are exact, not merely internally well-formed:
  every payload/index row must be reachable from the corresponding lookup key,
  and stale or missing lookup keys fail the registered checker before a cold
  filtered `eth_getLogs` path can return a false empty result.
- Manifest verification now also cross-checks active global `event-log-index`
  sidecars against the active `event-log` segments, so signed fetch, bootstrap,
  restore, and local verify gates reject stale address/topic postings before
  runtime archive log queries depend on them.
- Section-bloom segment builds now use the shared sorted ETL collector too:
  hot `sb-` rows are collected by `(section, bitIndex)` into scratch space and
  streamed into the immutable segment in lookup order. The derived-index
  aggregator passes its ETL options into section-bloom builds, so large bloom
  snapshot jobs no longer need to materialize every bloom row in a Go slice
  before writing.
- Balance-trace segment builds now feed account trace rows through the shared
  sorted ETL collector keyed by `(owner, reversedBlock)` before writing the
  immutable account index. Block trace protobuf payloads still stream directly
  by block number, so large payloads are not duplicated in scratch files while
  the archive account-balance lookup index no longer depends on raw iterator
  ordering.
- Balance-trace segment verification now proves every account touched by a
  `BlockBalanceTrace` operation has an exact-height account-index row in the
  same immutable segment, and rejects malformed operation addresses before
  signed manifest verification or archive balance APIs can trust the sidecar.
- Event-log cold segment builds now reject tx-bearing blocks whose
  `TransactionRet` coverage is missing, has a different transaction count,
  contains nil transaction-info entries, points at a different block number, or
  carries tx ids that do not match the canonical block transaction order. This
  prevents incomplete or mismatched hot source data from being published as
  immutable log coverage and making archive `eth_getLogs` return false
  negatives or wrong tx hashes after pruning.
- Snapshot restore API soak now also sends `eth_getTransactionByHash` through
  both the in-process JSON-RPC API and a real restarted JSON-RPC listener after
  hot block bodies, block-hash lookups, tx lookups, and per-tx info rows are
  absent. This pins the archive transaction lookup path to cold chain-freezer
  rows plus the `chain-index` sidecar, matching the existing receipt coverage.
- gRPC `WalletSolidity.GetTransactionCountByBlockNum` now shares the same solid
  boundary as the other WalletSolidity block reads: requests above the solidified
  block return `NotFound` before the backend can read a live-head block.
- gRPC `WalletSolidity.GetBlock(BlockReq)` now resolves `latest`, `earliest`,
  block numbers, and java-tron BlockIds through the same solid boundary. BlockIds
  expose their encoded height before any backend hash lookup, so requests above
  the solidified block cannot read hot/cold block payloads accidentally; the
  `detail=false` form returns transaction IDs without full transaction bodies.
- gRPC `Wallet.GetBlock(BlockReq)` now implements the same `BlockReq` parser for
  head, genesis, number, and java-tron BlockID reads, so regular Wallet callers
  also reach the ChainDB hot/cold block lookup path instead of the generated
  unimplemented stub; `detail=false` mirrors the Solidity response shape.
- HTTP `/walletsolidity` and `/walletpbft` block-by-id/range reads are now
  bound-aware too. `getblockbyid` extracts the java-tron BlockId height before
  any backend hash lookup, and `getblockbylimitnext` rejects ranges that cross
  the solid/PBFT boundary before calling the range reader.
- HTTP `/wallet/getblockbylimitnext` and gRPC
  `Wallet.GetBlockByLimitNext{,2}` now reject negative, empty, and reversed
  ranges before calling `GetBlocksByRange`, preventing invalid signed inputs
  from widening into large unsigned hot/cold block scans.
- Block-hash lookup now has a strict rawdb accessor too:
  `ReadBlockNumberStrict` surfaces malformed hot `bh-<hash>` rows and cold
  chain-index lookup errors. `TronBackend.GetBlockByHash` and JSON-RPC log
  filtering by `blockHash` use that strict path, so archive index corruption is
  reported as a data error instead of being disguised as an empty/not-found
  result. Once the hash lookup resolves a block number, the backend now reads
  the body through `ReadBlockStrict`; corrupt or missing freezer/hot block
  bodies behind a valid index surface as archive data errors before
  `eth_getLogs(blockHash=...)` can return a false empty result. Backend
  block-by-number and range reads now use the same strict body accessor, and
  JSON-RPC block-by-number/hash handlers only translate real not-found results
  to `null`; corrupt indexed block bodies now surface as JSON-RPC errors. TRON
  HTTP and gRPC block-by-number/hash handlers now make the same distinction,
  preserving empty/not-found responses for explicit misses while surfacing cold
  index or block-body corruption as internal server errors.
- HTTP `/walletsolidity`/`/walletpbft` and gRPC `WalletSolidity` transaction
  by id / transaction-info by id reads now resolve the transaction's block
  through the hot/cold tx lookup index before exposing payloads. Transactions
  above the solid/PBFT boundary return the API's normal empty/not-found response
  without reading the full transaction, block body, or receipt; the strict
  payload readers still verify block-body/index consistency after the bound
  admits the lookup. The strict transaction-info reader now also surfaces cold
  chain-index block-local-position lookup errors and corrupt readable block
  bodies instead of silently falling back to a per-block receipt scan.
- Transaction-info read paths now reject mismatched payloads before exposing
  them to APIs: hot `ti-<txid>` rows must either carry no embedded id or match
  the lookup key, cold tx-info fallbacks must either carry no embedded block
  number or match the tx index's block, and backend block-number receipt queries
  validate any retained `TransactionRet` list against the canonical block
  transaction count, block number, and tx hash order. The hot `TransactionInfo`
  writer and sorted `DerivedIndexCollector` now apply the same per-tx id/key
  check before new `ti-` rows can be written. Backend transaction-by-id/hash
  reads use the strict tx lookup index plus strict block-body access, so
  `eth_getTransactionByHash` can return transactions after receipt rows are
  pruned and a corrupt freezer/hot block behind a valid transaction index
  returns a data error instead of a false not-found result. Block-number TransactionInfo
  queries use the same strict block-body read before validating retained
  `TransactionRet` rows. JSON-RPC `eth_getTransactionReceipt` now preserves
  those transaction/block lookup errors after the receipt row has been found
  instead of translating cold archive corruption into a `null` receipt. TRON
  HTTP and gRPC transaction-by-id / transaction-info-by-id handlers now keep
  java-tron-compatible empty/not-found responses only for explicit misses; cold
  lookup, block-body, or receipt corruption from the backend is surfaced as an
  internal server error instead of being disguised as a missing transaction.
- The hot `eth_getLogs` fallback scan now uses the strict per-block
  `TransactionRet` reader too. For non-genesis blocks, missing per-block
  tx-info rows on tx-bearing blocks now fail the query instead of producing a
  false empty archive log response when cold event-log coverage is incomplete.
  Present rows with mismatched block numbers, counts, nil entries, or tx ids
  fail the query instead of producing a false empty or wrong log response. The
  fallback range scan also reads block bodies through `ReadBlockStrict`, so a
  corrupt freezer/hot body in the scanned range surfaces as data corruption
  instead of being skipped.
- Per-block `TransactionRet` reads now apply the same block-number guard at the
  rawdb accessor boundary for hot `tib-<block>` rows and ancient `tx_infos`
  rows: an embedded `TransactionRet.block_number` or
  `TransactionInfo.block_number` is accepted only when it is missing/zero or
  matches the requested block. This keeps fallback log/receipt scans from
  consuming mismatched hot or cold transaction-info payloads. Snapshot and
  rebuild publishers use the strict variant so malformed source rows fail the
  cold coverage build with a concrete corruption error instead of being treated
  as an ordinary coverage miss. The strict variant also surfaces ancient
  `tx_infos` read errors instead of falling back to a hot `tib-` duplicate or
  disguising freezer corruption as missing coverage. A rawdb source audit now
  prevents production snapshot builders from regressing to the non-strict
  per-block tx-info reader.
- Cold balance-trace reads now reject mismatched cold sidecar payloads before
  APIs or rebuild paths consume them: block trace reads reject a returned
  `BlockBalanceTrace.block_identifier.number` that disagrees with the requested
  block, while account-trace `AtOrBefore` reads reject cold lookups that point
  past the requested block. Older block-trace payloads that omit the block
  identifier remain readable.
- Offline restart-sync rewind/replay now reads canonical blocks and incremental
  unwind target state roots through strict freezer/cold-sidecar-aware
  accessors. Corrupt ancient block rows, malformed state-root rows, or cold
  hash lookup errors abort `RestartSyncFromHeight` with the original storage
  error instead of collapsing into a misleading missing-block or zero-root
  diagnosis.
- Startup head loading now uses the same strict chain boundary: genesis block
  reads preserve ancient/freezer errors, persisted head hash lookups surface
  cold chain-index failures, and the resolved head block must exist and match
  the stored head hash before `BlockChain` construction can continue. This
  prevents archive/minimal nodes from silently falling back to genesis when
  cold head metadata is corrupt or unreadable. The earlier
  `SetupGenesisBlockWithAncient` idempotency check now uses the strict genesis
  block reader too, so a corrupt frozen genesis row aborts startup instead of
  being treated as a missing genesis that can be rewritten into the hot DB.
- Fork rewind LCA loading is strict as well: `switchFork` resolves the LCA
  block number, block body, and state root through freezer/cold-sidecar-aware
  strict accessors before reloading consensus caches or re-applying the new
  branch. Cold lookup/state-root errors now abort the fork switch with storage
  context instead of replaying from a zero-root cache baseline.
- Archive execution root selection now has a strict state-root path for
  error-returning callers. `historyReaderAt` and `archiveExecutionRoot`
  surface corrupt cold state-root rows, cold hash lookup failures, or malformed
  block rows directly, while the legacy `StateRootAtBlock` zero-on-error
  contract remains available for compatibility-only callers.
- Producer block assembly and `openCurrentState` now share the same strict
  known-block state-root resolver. Parent/current cold state-root failures
  abort with storage context instead of falling through to a zero or stale
  `account_state_root` baseline.
- Live head-state `TronBackend` APIs that return errors now open the head
  state or resolve the head state root through that strict current-state path
  too: account/contract reads, constant-call simulation, account resource/net,
  JSON-RPC live balance/code/storage, debug trace calls,
  witness/proposal/exchange lists, TRC10 asset and market live queries,
  account-id/proposal-id lookups, delegation, freeze-v2 availability, reward
  reads, and broadcast-time transaction validation no longer route through
  `HeadStateRoot()`'s legacy zero-on-error fallback. Cold head state-root
  lookup failures surface as backend errors instead of misleading not-found,
  empty-resource, or zero-value results.
- `eth_getLogs` treats section-bloom read failures as optimization misses and
  falls back to canonical block/TransactionInfo scanning for the bounded query
  range. Strict section-bloom errors still surface through rebuild, prune, and
  verification paths where the index itself is the artifact being audited.
- The runbook is `docs/dev/etl-collector.md`.

Remaining:

- Finish the remaining derived RPC index/cold data surface, especially broader
  event/log cold accessors and any rebuild paths that still bypass the
  collector where benchmarks show lower Pebble write amplification. The
  account/balance trace read APIs are wired, history-enabled execution now
  populates new trace rows, account traces can be repaired from retained
  block-balance traces, rawdb readers can fall through to registered cold trace
  and section-bloom segments after verified hot pruning, progress-aware
  lifecycle pruning now covers chain lookups, section blooms, and balance
  traces, balance-trace snapshot builds now reject incomplete source ranges, and
  isolated replay backfill has replay-DB resume, collector-backed trace writes,
  signed-snapshot checkpoint starts, and snapshot-aware balance-trace
  diagnostics. Production archive completeness still
  needs larger-datadir soak, recsplit-style event-log address/topic profiling
  beyond the current sorted sidecar, and broader API coverage on top of the
  shared backend archive/as-of state session.
- Collect longer Pebble-backed benchmark samples for large snapshot restore and
  backfill workloads, then tune collector buffer/batch defaults.

### P3: Accessor Format Evaluation

go-tron's `.bt` latest accessor is correct for ordered prefix iteration.
Erigon's recsplit/hash accessors are still useful for point-heavy indexes.

Status:

- Raw chain freezer tables now expose a diagnostic `Stats` snapshot with
  freezer-wide head/tail plus per-table physical tail, hidden tail, prunable
  flags, shard IDs, visible size, and hidden size. `gtron db freezer-status`
  opens the chain freezer read-only and prints that state for benchmark and
  soak sampling after minimal-mode tail pruning. Freezer repair events now also
  update `ancient/repair/*` metrics and the opt-in debug metrics endpoint can
  filter those rows for alert exporters.

Adopt only where profiles justify it:

- tx hash to block lookup
- event/log topic and address indexes
- history accessor point lookup if `.kv` binary search becomes a bottleneck
- existence filters for cold CodeDomain or commitment snapshots

## Recommended Implementation Order

1. Lock the compatibility gate: add archive/root fixture coverage before more
   storage churn.
2. Normalize operator modes and document exact retention semantics.
3. Finish chain freezer integrity checks and any remaining accessor audits.
4. Build remote snapshot bootstrap for latest/history/commitment/freezer files.
5. Move sync to a fuller staged loop with resumable downloader/import/execution
   progress.
6. Add derived history/index domains for receipts, logs, traces, tx lookup, and
   balance/account traces.
7. Extend ETL collector usage to large backfill and derived-index builds, then
   collect Pebble-backed restore/backfill samples.
8. Collect storage/sync samples with `scripts/dev/storage_benchmark.sh`, then
   benchmark Pebble vs MDBX only after the storage interfaces above are stable.
9. Evaluate recsplit/existence filters for point-heavy cold accessors.
10. Explore parallel execution only with serial-equivalence tests and java-tron
    replay fixtures.

## Definition Of Done

The Erigon import goal is complete when:

- A fresh go-tron node can bootstrap from verified snapshots and only sync the
  recent tail from peers.
- Hot Pebble stores only recent mutable state, recent history, runtime metadata,
  and explicitly wallet-hot indexes.
- Archive mode answers historical account, storage, code, resource, event, and
  transaction-info reads without scanning unrelated block ranges.
- Full/snap/minimal modes prune hot history only after cold coverage is visible,
  readable, and verified by the registered segment checkers.
- Chain freezer or equivalent immutable block files cover old blocks and major
  block/transaction indexes.
- Sync import is stage-resumable and range-batched.
- java-tron wire, P2P, block validation, actuator, VM, DPoS, fork, and
  maintenance compatibility tests remain green.
