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
| Cold history | Binary `history/state-domain-change-*.seg` plus `.idx` and `.kv`. | Strong functionally, different file names from Erigon. |
| Latest files | Binary `.seg` plus `.lidx` and `.bt`. | Strong for point lookup and prefix iteration. Not recsplit, intentionally. |
| Commitment domain | Staged hex-patricia branch rows, checkpoints, cold branch restore, java root adapter. | Strong. Internal root is decoupled from java-tron header root. |
| Code retention | Content-addressed CodeDomain latest snapshots selected by account-envelope history. | Strong, with a deliberate no-temporal-code policy. |
| Cold/hot lifecycle | `SnapshotLifecycle` runs builder, compactor, and pruner in order. | Moderate to strong. Local lifecycle and operator remote fetch/restore exist; automated remote publish/handoff is still missing. |
| Pruning modes | `archive`, `full`, `snap`, `blocks`, and `minimal` are accepted through `--prune.mode`; `--gcmode` remains a deprecated alias. | Partial. The CLI vocabulary now matches Erigon; `minimal` now has freezer virtual-tail enforcement plus physical shard reclamation gated by cold coverage, while `blocks` still needs distinct block-retention behavior. |
| Chain freezer | `core/freezer` plus `core/rawdb/freezer`, `ChainDB` fall-through, and cold `chain-index` sidecars. | Moderate. Old block bodies/tx infos/state roots can be served from freezer, and verified sidecars cover block/tx lookup rows after hot prune. |
| Staged execution | Hash-bound `Headers/Bodies/Execution/Commitment/Finish`, `InsertBlocks`, `canonicalRangeExecutor`, reusable `CommitScope`. | Partial. Range-shaped and stage-tracked, but not a full Erigon staged-sync loop. |
| Parallel execution | Async commitment can overlap fold with next block in bulk sync. | Partial. No Erigon-style parallel transaction executor. |
| Snapshot bootstrapping | Local snapshot build/restore plus signed remote fetch exist. | Moderate. Preverified HTTP(S) catalog/manifest/segment download, local reset/resync, and bootstrap restore are covered; production hosting/defaults remain. |
| Derived history domains | Some blooms/traces/receipts are still rawdb or planned. | Weak to partial. Erigon has receipts/log/traces indexes as registered domains or indexes. |
| ETL sorted ingestion | Streaming snapshot builders, batches, and `core/rawdb/etl` collector support exist. | Partial. The generic temp-run collector is available; restore/backfill/index callers still need migration and benchmark evidence. |

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
  across fetch, verify, bootstrap, restore, and chain-lookup prune commands.
  The file supports comments, comma-separated entries, and overlap windows for
  Ed25519 key rotation.
- `docs/dev/snapshot-bootstrap.md` records the operator runbook for signed
  remote bootstrap: trusted key file format, key rotation steps, preflight
  verify, fetch+restore, one-step bootstrap, and safety notes around
  `--snapshot.reset` and fork config hashes.
- `gtron snapshot fetch` downloads a signed HTTP(S) snapshot catalog, verifies
  the catalog signature before trusting manifest paths, checks the catalog
  manifest checksum, downloads active segments, and then re-runs strict
  registered-segment/checksum verification on the local snapshot directory.
  `--snapshot.reset` deletes the target local snapshot directory first so an
  operator can discard stale local views and resync from the latest remote
  catalog.
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
  `chain-index` sidecar for historical rows.
- Raw freezer accessors now share the same cold path where appropriate:
  `ReadBlockRaw`, `ReadTransactionInfosRaw`, `ReadBlockHashByNumber`, and
  `ReadBlockStateRootRaw` can read through `ChainDB` instead of assuming the
  copied rows are still present in hot KV.

Needed:

- Mainnet/testnet publication policy: official trusted catalog keys, rotation,
  and release workflow.
- Production HTTP catalog/segment hosting and operator defaults; BitTorrent or
  WebSeed can come later.
- Restore pipeline handoff from snapshot/freezer boundary into downloader,
  `SyncService.HandleBlock`, and local two-node P2P tail sync is now covered by
  regression tests. Remaining production work is longer-running soak/metrics
  around real hosted snapshots and heterogeneous peers.
- Operator publication still needs the official latest remote snapshot URL and
  trusted key release artifacts. The local CLI and runbook now cover trusted key
  files, key rotation, fetch, verify, bootstrap, restore, and reset primitives.

This is the largest sync-speed win because it avoids replaying all historical
blocks from genesis.

### P1: Full Staged Sync Loop

`InsertBlocks` is range-shaped, but go-tron still executes and commits block by
block under the canonical chain path.

Status:

- `SyncService` now persists lightweight downloader/body/import watermarks:
  `SyncInventory` records the highest peer inventory target observed,
  hash-bound `SyncBodies` records the highest block body accepted into the
  transient downloader staging table, and hash-bound `SyncImport` records the
  latest block successfully imported by the live sync loop. New sync sessions
  restore `SyncInventory` when it is ahead of the current head, preserving
  restart diagnostics/remaining-block estimates without advancing canonical
  chain state. These are intentionally outside `CanonicalExecutionStages()` so
  peer-advertised or downloaded progress cannot masquerade as executed
  canonical chain progress.
- Received sync block bodies are now written to a `sync-staged-block-v1-`
  staging table before they enter the in-memory drain buffer. A restarted sync
  session reloads contiguous staged bodies from `head+1`; successful
  `InsertBlocks` deletes the matching staging rows, while active sync reset
  clears unimported rows so peer failover does not inherit a stale path.
- Regression coverage checks both normal multi-peer sync and snapshot-freezer
  boundary handoff: inventory target progress survives the CHAIN_INVENTORY path,
  downloaded bodies are staged and restored across session startup, and imported
  block number/hash progress is written after `InsertBlocks` succeeds.

Needed:

- Extend the downloader/body/import watermarks into a fuller staged loop for
  execution, commitment, finish, snapshot build, prune, and freezer.
- Let sync drain fetched ranges by stage instead of coupling download and
  execution to one service loop.
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
- Verified chain-freezer snapshot restore writes the same `ChainFreezer` stage
  to the restored datadir after the segment is installed or verified as already
  present, so remote bootstrap and live freezer passes expose one cold-chain
  coverage watermark.
- `PruneHotChainLookups` can now remove historical hot `bh-`, `tx-`, `ti-`,
  and `bsr-` lookup rows for a freezer range after verifying the matching
  `chain-index` sidecar against the chain-freezer segment. Tests prove cold
  reads continue to work through freezer plus sidecar after these hot lookup
  rows are deleted.
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
  is absent.
- Freezer startup repair is covered for table cardinality mismatch: writable
  opens truncate all freezer tables to the common low head, while readonly opens
  reject mismatched heads instead of silently serving a partial ancient view.
- The raw freezer now has a prunable-table virtual tail API: `TruncateTail`
  persists a hidden ancient tail and makes old rows unreadable without changing
  the append head. The production chain-freezer table set marks `bodies`,
  `tx_infos`, and `state_roots` as prunable and has regression coverage proving
  the table set can advance and persist a hidden tail.
- `PlanChainFreezerTailPrune` now provides the boundary calculator for
  minimal-mode block retention. It combines `ChainFreezer` and
  `SnapshotChainLookupPrune` stage progress, converts inclusive coverage block
  `N` into freezer tail `N+1`, and caps the target by the ancient append head
  plus the recent-block retention window. The apply path verifies cold
  chain-freezer coverage before calling runtime `TruncateTail`; tests pin
  missing-stage no-ops, lookup-stage caps, retention-window caps,
  ancient-head caps, short-chain behavior, DB-backed stage reads, successful
  tail truncation, and no-op behavior when cold coverage is missing.
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
  sidecar. Tests prove the accessor build/check/verify path and that Manager
  can serve a row even when full segment scanning would reject trailing bytes.
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
  state-domain history.
- Backend-level cold state-domain snapshot coverage now also records
  `GetAccountResourceAt` and `GetRewardAt` answers before hot history pruning,
  deletes the hot StateDomainChange/StateTxRange rows for the covered blocks,
  and then rechecks account resource usage, DP-derived limits, and witness
  reward/allowance through the cold history manager.
- The remaining production chain `rawdb.ReadBlockKV` call sites have been
  audited. TVM `BLOCKHASH`/`CHAINID` now have cold fallbacks, and the pruning
  finish-stage guard prefers `CanonicalBlockHash`; the `cmd/gtron` adapter
  resolves that through `BlockChain.GetBlockByNumber`, which can read the chain
  freezer. The direct KV read in the pruner is only a fallback for minimal test
  sources and does not currently block aggressive hot chain lookup pruning.

Needed:

- Add longer-running node/server bootstrap soak tests that start the full API
  servers after `gtron snapshot restore` and exercise a broader `/wallet*`,
  JSON-RPC, and archive-read matrix, especially multi-account contract,
  delegation, and account delete/recreate fixtures beyond the current
  balance/code/storage/account/resource checks.
- Keep auditing newly introduced direct hot-only `rawdb.Read*KV` call sites
  before enabling more aggressive chain-data prune defaults.
- Evaluate compact/merged cold index formats for block hash by number, tx
  lookup, per-tx info, and state-root lookup if sidecar profiles show they
  dominate disk or lookup latency.
- Keep only recent chain data and wallet-hot indexes in Pebble under full/snap
  modes.
- Add higher-level operator alerts around freezer repair events and
  catalog/freezer sidecar mismatch.

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
  still share `full`'s finite state-history retention window until block
  retention enforcement lands, but lifecycle logs/checks no longer collapse
  them to `full`.
- `blocks` preserves complete local chain-freezer history while still allowing
  state/history and hot lookup pruning. `minimal` is the only mode that
  registers a chain-freezer tail-prune lifecycle. It applies virtual-tail
  hiding and physical freezer shard reclamation after verified freezer/index
  stage progress and cold chain-freezer segment coverage are visible. Cold
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
- `snap` keeps go-tron's cold-snapshot-gated hot pruning.
- `archive` keeps every temporal state row and auto-enables history capture.
- The selected mode is persisted in rawdb as `history-prune-mode-v1`; startup
  rejects incompatible mode changes for an existing datadir.
- Chain lookup pruning is already wired for `full`, `blocks`, `minimal`, and
  `snap` once verified chain-freezer/index snapshot coverage exists; `archive`
  keeps hot lookup rows.
- `scripts/dev/storage_benchmark.sh` is now the repeatable measurement harness
  for producer-time, follower sync catch-up, and datadir size split by hot
  Pebble, ancient freezer, and state snapshots across prune modes. Its
  `--signed-cold-prune` drill builds cold chain-freezer coverage, signs the
  catalog, prunes hot chain lookup rows through the verified signer, and
  restarts `minimal` once so the tail-prune lifecycle can report the post-prune
  boundary. The matching runbook is `docs/dev/erigon-storage-benchmark.md`,
  with the first smoke sample recorded in
  `docs/dev/erigon-storage-benchmark-results-2026-06-10.md`.

Remaining:

- Promote `blocks`/`minimal` block-retention behavior from tested primitives
  and dev harness coverage to production-complete status. The freezer and
  chain-freezer table set now have the tested virtual-tail primitive, physical
  shard reclamation primitive, planner, runtime apply path, and cold
  chain-freezer segment fallback reads with block-number accessors, but
  production still needs collected long-running soak/space samples and
  acceptance thresholds before old block file reclamation is considered
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
  worker goroutines warm raw latest-domain account, account-KV, and contract
  storage reads through `ethdb.KeyValueReader`, with bounded non-blocking
  enqueue and hit/miss/drop/error stats. It deliberately avoids mutating
  `StateDB` object caches because those maps do not yet have a concurrent
  access model.
- `actuator.PrefetchKeysFor(tx)` now extracts the first deterministic
  envelope-derived hints for account latest rows, contract metadata rows, and
  system delegation rows. It covers transfer, TVM create/trigger, contract
  settings, vote witness, Stake 1.0/2.0, shielded transparent endpoints,
  owner-only actuators, account-create, and participate-asset-issue families.
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
- The runbook is `docs/dev/etl-collector.md`.

Remaining:

- Migrate high-volume snapshot restore, history backfill, chain-index build,
  and derived RPC index build paths onto the collector where benchmarks show
  lower Pebble write amplification.
- Add path-specific benchmark samples comparing direct unordered writes versus
  collector loads.

### P3: Accessor Format Evaluation

go-tron's `.bt` latest accessor is correct for ordered prefix iteration.
Erigon's recsplit/hash accessors are still useful for point-heavy indexes.

Adopt only where profiles justify it:

- tx hash to block lookup
- event/log topic and address indexes
- history accessor point lookup if `.kv` binary search becomes a bottleneck
- per-table physical-tail observability and repair-event metrics for pruned
  freezer tails
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
7. Add ETL collector support for large sorted index builds and snapshot restore.
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
- Full/snap/minimal modes prune hot history only after cold coverage is visible
  and verified.
- Chain freezer or equivalent immutable block files cover old blocks and major
  block/transaction indexes.
- Sync import is stage-resumable and range-batched.
- java-tron wire, P2P, block validation, actuator, VM, DPoS, fork, and
  maintenance compatibility tests remain green.
