# Erigon-Style Storage Benchmark

This runbook records the repeatable harness for measuring go-tron's progress
toward Erigon-style hot/cold storage, pruning modes, and snapshot-assisted sync.

The harness is intentionally not a CI pass/fail test. It emits JSONL rows that
can be graphed over multiple samples and machines.

## Producer Profile

Run one dev witness per prune mode and measure time to a target height plus
datadir size split by hot Pebble, ancient freezer, and state snapshots:

```bash
scripts/dev/storage_benchmark.sh \
  --profile producer \
  --modes full,blocks,minimal,snap,archive \
  --target-blocks 120 \
  --freezer-margin 3 \
  --freezer-interval 1s \
  --build-cold-freezer \
  --build-derived-indexes \
  --keep
```

The output path is printed at startup. Each JSON row contains:

- `elapsedSeconds`
- `height`
- `datadirBytes`
- `chaindataBytes`
- `ancientBytes`
- `snapshotBytes`
- `ancientFiles`
- `snapshotFiles`
- `derivedIndexBytes`
- `derivedIndexFiles`
- `snapshotManifestProfileStatus`
- `snapshotProfileSegments`
- `snapshotProfileTotalBytes`
- `snapshotPayloadBytes`
- `snapshotSidecarBytes`
- `snapshotSidecarShareMilli`
- `snapshotLatestSidecarBytes`
- `snapshotLatestSidecarShareMilli`
- `snapshotStateHistorySidecarBytes`
- `snapshotStateHistorySidecarShareMilli`
- `snapshotChainFreezerSidecarBytes`
- `snapshotChainFreezerSidecarShareMilli`
- `snapshotEventLogSidecarBytes`
- `snapshotEventLogSidecarShareMilli`
- `snapshotBalanceTraceSidecarBytes`
- `snapshotBalanceTraceSidecarShareMilli`
- `snapshotSectionBloomSidecarBytes`
- `snapshotSectionBloomSidecarShareMilli`
- `coldFreezerToBlock`
- `derivedIndexToBlock`
- `derivedIndexSegments`
- `derivedIndexBuildSeconds`
- `eventLogIndexSegments`
- `eventLogIndexFromBlock`
- `eventLogIndexToBlock`
- `eventLogIndexAddressKeys`
- `eventLogIndexAddressPostings`
- `eventLogIndexAddressAvgPostingsMilli`
- `eventLogIndexAddressMaxPostings`
- `eventLogIndexAddressSingletonKeys`
- `eventLogIndexAddressMultiPostingKeys`
- `eventLogIndexTopicKeys`
- `eventLogIndexTopicPostings`
- `eventLogIndexTopicAvgPostingsMilli`
- `eventLogIndexTopicMaxPostings`
- `eventLogIndexTopicSingletonKeys`
- `eventLogIndexTopicMultiPostingKeys`
- `balanceTracePruneToBlock`
- `balanceTraceBlockRowsPruned`
- `balanceTraceAccountRowsPruned`
- `sectionBloomPruneToSection`
- `sectionBloomRowsPruned`
- `signedColdPrune`
- `chainLookupPruneToBlock`
- `chainLookupBlockIndexes`
- `chainLookupTxIndexes`
- `retiredPruneRan`
- `retiredPruneSegments`
- `retiredPruneDeleted`
- `retiredPruneMissing`
- `retiredPruneSkippedActive`
- `retiredPruneBytesDeleted`
- `tailPrunedThroughBlock`
- `tailPrunedFiles`
- `historyWindow`
- `freezerAlertStatus`
- `freezerAlertIssues`
- `freezerAlertHiddenBytes`
- `freezerAlertDetails`
- `stageVerifyStatus`
- `stageVerifyIssues`
- `stageVerifyDetails`
- `stageAlertPipelineComplete`
- `stageAlertPipelinePending`
- `stageAlertPipelineIssues`
- `stageAlertPipelineNext`
- `stageAlertPipelineNextStatus`
- `stageAlertPipelineNextTarget`
- `stageAlertPipelineNextUpstream`
- `stageAlertPipelineNextCurrent`
- `stageAlertPipelineTasks`
- `snapshotAlertStatus`
- `snapshotAlertIssues`
- `snapshotAlertDetails`
- `snapshotRetiredSegments`
- `snapshotRetiredFiles`
- `snapshotRetiredMissing`
- `snapshotRetiredSkippedActive`
- `snapshotRetiredBytes`
- `archiveApiStatus`
- `archiveApiChecks`
- `archiveApiFailures`
- `archiveApiBlock`
- `archiveApiDepthBlocks`
- `archiveApiCallProbe`
- `archiveApiTraceTransactionProbe`
- `archiveApiTraceBlockProbe`
- `archiveApiMethods`
- `archiveApiTxProbe`
- `archiveApiTxHash`
- `archiveApiTxMethods`
- `storageBenchmarkPrometheus`
- `storageAlertPrometheus`

`--build-cold-freezer` runs `gtron snapshot build-freezer` after stopping each
producer so cold chain-freezer snapshot bytes are included in the size split.
The freezer build writes or preserves the manifest chain identity, so the output
can be signed in a follow-up drill. It does not sign catalogs or mark
lookup-prune progress by itself; it is a measurement step, not a production
prune workflow.

`--build-derived-indexes` starts the producer with `--history.enabled`, then
runs `gtron snapshot build-derived-indexes` after stopping it. The build uses
the same post-freezer-margin boundary as the cold freezer build and publishes
balance-trace, section-bloom, event-log, and event-log-index sidecars into the
snapshot manifest. Use this option to measure the ETL-backed derived-index
builders and to generate indexed log coverage before minimal-mode prune drills.
After the build, the harness runs `gtron snapshot event-log-index-stats` and
records address/topic key counts, postings, average postings per key in milli
units, worst-case postings per key, and single- versus multi-segment key
counts for the event-log-index sidecars. These counters are the first profiling
signal for whether the sorted sidecar is selective enough or needs a
recsplit-style accessor.
The Nile sampler records the same `eventLogIndex*` counters when
`scripts/dev/nile_sync_sample.sh` runs with `--event-log-index-stats`, so
production soak evidence can be checked against the same lookup-selectivity
rules as benchmark artifacts.

Cold state-domain history segments are block-compressed by default when the
producer lifecycle emits them. For A/B storage measurements or an emergency
rollback to raw segment emission, start the producer with
`--snapshot.compress-history=false` or set
`GTRON_SNAPSHOT_COMPRESS_HISTORY=false`; existing compressed and raw segments
remain readable either way.

## Signed Cold Prune Drill

Run the producer profile with `--signed-cold-prune` to exercise the minimum
operator prune workflow in one sample:

```bash
scripts/dev/storage_benchmark.sh \
  --profile producer \
  --modes blocks,minimal \
  --target-blocks 120 \
  --freezer-margin 3 \
  --freezer-interval 1s \
  --signed-cold-prune \
  --archive-api-probe \
  --history-window 8 \
  --keep
```

This builds the cold chain-freezer segment, signs `snapshot-catalog.json`, runs
`gtron snapshot prune-chain-lookups` with the catalog signer as a trusted key
for non-`archive` modes, and then runs `gtron snapshot prune-retired` for each
selected mode. `full`, `snap`, and `blocks` should report lookup-prune plus
retired snapshot-file cleanup with no freezer-tail prune. `minimal` then
restarts once so the tail-prune lifecycle can run. Use a small
`--history-window` for short dev samples; production and soak runs should use
the intended retention window.

With `--archive-api-probe`, the runner also calls the JSON-RPC archive read
surface (`eth_getBlockByNumber`, `eth_getBlockByHash`,
`eth_getBlockTransactionCountByNumber`, `eth_getBlockTransactionCountByHash`,
`eth_getUncleCountByBlockNumber`, `eth_getUncleCountByBlockHash`,
`eth_getUncleByBlockNumberAndIndex`, `eth_getUncleByBlockHashAndIndex`,
`eth_getBlockReceipts`, `eth_getBalance`, `eth_getCode`, `eth_getStorageAt`,
and `eth_getLogs`) at `height-1` by default and emits `archiveApi*` fields.
When the probed historical block contains a transaction, the probe also adds
`eth_getTransactionByHash`, `eth_getTransactionReceipt`,
`eth_getTransactionByBlockNumberAndIndex`, and
`eth_getTransactionByBlockHashAndIndex` plus `archiveApiTx*` fields.
For transaction-bearing blocks, `eth_getBlockReceipts` must include the same
selected transaction hash; an empty block-receipts result is counted as an
archive API failure. If those block receipts include receipt logs,
`eth_getLogs` over the same historical block must also return the corresponding
log entries. Once the unfiltered log proof succeeds, the runner also records an
`eth_getLogsFiltered` archive method label for a second `eth_getLogs` call with
a receipt-derived address/topic filter.
Pass `--archive-api-block`, `--archive-api-address`, or `--archive-api-storage-slot`
when a run needs to target a known historical contract/account. Pass
`--archive-api-call-data` as well to include `eth_call`, `debug_traceCall`, and
`eth_estimateGas` against that address. Pass `--archive-api-trace-block` when
the run should also prove
`debug_traceBlockByNumber` and `debug_traceBlockByHash` on the selected
historical block.

When the same run also passes `--build-derived-indexes`, the signed drill also
runs `gtron snapshot prune-balance-traces` and
`gtron snapshot prune-section-blooms` after catalog publication. Those commands
verify the signed catalog and compare hot rows against the cold sidecars before
deleting anything, so the JSON row can report trace/bloom hot-row reclamation
alongside chain lookup pruning.

`prune-retired` verifies the active manifest before deleting retired snapshot
segment files. The JSON row reports `retiredPruneRan=true` plus the retired
segment count, deleted/missing file count, active-file skips, and reclaimed
bytes through the `retiredPrune*` fields.

After the drill, `gtron db freezer-status --datadir <dir>` prints the local
freezer head/tail plus per-table physical tail, hidden tail, shard IDs, and
visible/hidden sizes. The header also includes `repairApplied`,
`repairTargetHead`, `repairTargetTail`, and `repairRecordedAt`; if any
`Freezer repair table` rows appear, preserve them with the benchmark artifacts
because the freezer had to truncate table bounds on open before serving the
status view. The same repair record is persisted in the freezer directory as
`repair.json`, so a later readonly status sample can still surface the last
automatic repair. When the debug server is enabled, `/debug/metrics?prefix=ancient/repair/`
also exposes `ancient/repair/applied`, `tables`, `target/head`, `target/tail`,
`recorded`, and `events` for alert sampling. Capture this alongside the JSONL
row when validating minimal-mode physical shard reclamation.

For long-running sync/freezer drills, also scrape
`/debug/metrics?prefix=chain/freezer/`. The runner exports the visible frozen
range (`frozen/min`, `frozen/max`, `frozen/has`), cumulative progress
(`blocks`, `passes`), latest pass wall-clock fields (`lastpass/time`,
`lastpass/duration` in nanoseconds), and the sampled hot block-row footprint
(`pebble/size`). These are the primary live signals for confirming that a
fresh follower is draining historical block rows into ancient files instead of
leaking them in Pebble.

`scripts/dev/storage_benchmark.sh` runs
`gtron db storage-alerts --json --datadir <dir>` before emitting each JSONL row.
When
persisted freezer state is unsafe for prune/archive assumptions, including a
recorded repair, a missing or impossible `ChainFreezer` stage, inconsistent
per-table bounds, or a virtual tail past the append head, the harness writes a
`status=storage-alerts-critical` JSONL row and then exits non-zero. It uses the
same fail-after-row behavior if canonical/sync/snapshot/prune stage rows are
hash-mismatched, out of order, or claim cold coverage that the local manifest
cannot prove. It also fails when the persisted prune mode contradicts stage
progress, such as an `archive` datadir with hot-prune/lookup-prune/tail-prune
stages or a non-`minimal` datadir with freezer-tail prune progress; the same
mode/stage contradiction is rejected during node startup before services are
registered. It also warns when retired snapshot files still occupy disk after
compaction or replacement. The JSONL row includes
`storageAlertStatus`, `freezerAlertStatus`, `freezerAlertIssues`,
`freezerAlertHiddenBytes`, `freezerAlertDetails`, `stageVerifyStatus`, `stageVerifyIssues`,
`stageVerifyDetails`, `stageAlertPipelineComplete`,
`stageAlertPipelinePending`, `stageAlertPipelineIssues`,
`stageAlertPipelineNext`, `stageAlertPipelineNextStatus`,
`stageAlertPipelineNextTarget`, `stageAlertPipelineNextUpstream`,
`stageAlertPipelineNextCurrent`, `stageAlertPipelineTasks`,
`modeAlertStatus`, `modeAlertIssues`, `modeAlertDetails`, `pruneMode`,
`pruneModePersisted`,
`snapshotAlertStatus`, `snapshotAlertIssues`, `snapshotAlertDetails`, and the
`snapshotRetired*` counters; warning rows capture hidden freezer bytes and
retired snapshot bytes that still await physical pruning. Each row also records
`storageAlertPrometheus`, the Prometheus text artifact produced from the same
storage-alert gate for archive/soak monitor ingestion.
The row also records `storageBenchmarkPrometheus`, a benchmark-owned
Prometheus artifact with the row's height, elapsed seconds, hot/cold/snapshot
bytes, cold-archive bytes, derived-index bytes, matching bytes-per-block
gauges for the same storage families, snapshot sidecar share, and archive API
probe checks/block/depth/failures plus per-method success gauges. The
`gtron_storage_benchmark_archive_api_depth_blocks` gauge matches
`archiveApiDepthBlocks`, the sampled `height - archiveApiBlock` distance. The
storage-alert and benchmark artifacts are written next to the JSONL output so
they remain readable even when the harness removes its temporary workdir.
The acceptance checker binds `gtron_storage_alert_status`,
`gtron_storage_alert_issue`, `gtron_storage_stage_pipeline_*`,
`gtron_storage_signed_cold_prune`, and
`gtron_storage_prune_boundary_block{field=...}` samples to the row's `datadir`
label when present, so an aggregated metrics file cannot satisfy one datadir's
storage row with another datadir's stage, alert, or prune-boundary metrics.
For external monitors that scrape command output instead of JSONL harness rows,
run `gtron db storage-alerts --prometheus --datadir <dir>`. The Prometheus text
output exposes overall/component status gauges (`0=ok`, `1=warning`,
`2=critical`), component issue counts, normalized issue-kind counts by
component/severity/kind, hidden freezer bytes, retired snapshot counters, and
the persisted prune mode without putting high-cardinality issue details into
metric labels. It also exposes `gtron_storage_stage_pipeline_*` gauges so
external monitors can separate pending storage-maintenance stage work from
critical storage-integrity failures, including the next stage edge's
stage/status/upstream labels and target/current block cursors.

## Sync Profile

Run one dev witness and one fresh follower per mode. The row measures follower
catch-up time and follower datadir size:

```bash
scripts/dev/storage_benchmark.sh \
  --profile sync \
  --modes full,blocks,minimal,snap \
  --target-blocks 120 \
  --sync-max-diff 2 \
  --keep
```

The producer always uses `full` mode in this profile. The follower uses the mode
under test, which isolates mode impact on sync/import and local storage.

For a real Nile node that is already running, use
`scripts/dev/nile_sync_sample.sh` instead of this dev-network launcher. It
appends JSONL samples with HTTP height/block ID, peer count, elapsed sync time,
total and per-interval block rates, and hot/cold/snapshot disk split plus
per-interval byte deltas without opening the live Pebble store. The Nile sampler
also breaks out `derivedIndexBytes`/`derivedIndexFiles` for chain-index,
balance-trace, section-bloom, and event-log/index sidecars, and reports
Pebble hot-store SST/WAL ratios plus interval SST/WAL bytes per block. It also
profiles `state-snapshots/manifest.json` when present and emits the same
`snapshotManifestProfileStatus`, `snapshotPayloadBytes`,
`snapshotSidecarBytes`, `snapshotSidecarShareMilli`, and per-family
`snapshot*Sidecar*` counters as the benchmark harness, so Nile samples can gate
lookup-sidecar overhead with the same thresholds used by dev profiles. It also
reports `intervalDiskGrowthPrimaryDetailed*` across Pebble file buckets,
ancient tables, snapshot subdirectories, replay, and datadir-other bytes so
archive/full/minimal sync runs can be compared by the actual interval growth
source while keeping `snapshotBytes` as the full snapshot directory size. The
`interval*ToHotGrowthRatio` fields compare positive cold/archive/index growth
against positive Pebble hot-store growth for the same sample interval. Run it with
`--sync-log-file` to fold the latest `Imported chain segment` stage planner,
execution-plan, slow-phase, state-mutation, and peer fields into the JSONL row.
Each row also includes `soakHealthStatus`, `soakHealthIssues`, and
`soakPrimaryBottleneck*` fields so long Nile runs can be filtered directly for
HTTP/peer failures, stage regressions, pipeline violations, storage alerts, and
the current sync-stage bottleneck.
Set `--prometheus-output <file>` on the Nile sampler when a scrape job should
consume the current sync sample directly. The sampler writes
`gtron_nile_sync_*` gauges for height, target/stage lag, throughput,
full-staged-sync status/ready/coverage/bottleneck, hot/cold/index bytes,
per-stage full-staged-sync block/present/verified evidence when available,
adjacent sync-stage lag, cold-builder head lag, interval stage throughput,
bytes-per-block efficiency, snapshot sidecar share, archive probe
checks/block/failures, and sample/soak health status, and records
`samplePrometheus*` fields in the JSONL row. Gate that artifact with
`nile_sync_acceptance.py
--require-sample-prometheus-artifact` so the scrape payload cannot drift from
the accepted JSONL sample. Use
`scripts/dev/prometheus_artifact_export.py results.jsonl --output gtron.prom`
to atomically combine the latest JSONL-referenced `samplePrometheus`,
`offlineDbCheckPrometheus`, `storageBenchmarkPrometheus`, and
`storageAlertPrometheus` artifacts into one node_exporter textfile-collector
payload.
Run it with `--offline-db-check` only after the node is stopped to add
`storage-alerts` stage/freezer/snapshot diagnostics, including
`stageVerifyDetails` entries such as `SyncBodies`/`SyncBodiesReady`
staged-body mismatches.
Use `--archive-api-probe` on the Nile sampler when the same production JSONL
must satisfy archive-read acceptance gates. It emits `archiveApiStatus`,
`archiveApiChecks`, `archiveApiFailures`, `archiveApiBlock`,
`archiveApiDepthBlocks`, and `archiveApiMethods` from historical JSON-RPC reads,
including TRON-empty uncle-count and uncle-by-index probes for Ethereum client
compatibility.
When the probed block has a transaction, it also emits `archiveApiTxProbe`,
`archiveApiTxHash`, and `archiveApiTxMethods`; add `--archive-api-call-data`
plus `--archive-api-method eth_call`, `--archive-api-method debug_traceCall`,
and `--archive-api-method eth_estimateGas` only when the sample targets a known
historical contract. The probe counts only shape-valid JSON-RPC results as
successful:
block reads must return an object, account/code/storage/call reads must return
hex strings, logs must return a list, and transaction/receipt reads must return
objects rather than `null`. The block result must also carry the requested
historical block number, and transaction/receipt results must carry the
transaction hash selected from that block.

## Interpreting Results

- `full`: should keep recent hot state/history and use local freezer for old
  chain data.
- `blocks`: should preserve complete local chain-freezer history while allowing
  state/history and hot lookup pruning once cold coverage exists.
- `minimal`: should be evaluated with `--signed-cold-prune` after verified
  cold chain-freezer, chain-index, and indexed event-log coverage exists; the
  drill reports lookup-prune coverage and the restart-time tail-prune boundary.
- `snap`: should prune hot history only after immutable state-domain snapshot
  coverage exists, making it the evidence path for snapshot-assisted state
  compaction.
- `archive`: should retain all temporal state rows and is expected to consume
  more hot storage.

For production acceptance, collect at least:

1. Short dev samples from this harness for every changed storage slice.
2. Long-running private-chain soak samples with the same JSON schema.
3. Mainnet/Nile replay or fixture samples for archive reads after hot prune.

After collecting a candidate run, gate the JSONL with the acceptance checker:

```bash
scripts/dev/storage_benchmark_acceptance.py results.jsonl \
  --role producer \
  --require-modes full,blocks,minimal,snap,archive \
  --require-prometheus-artifacts \
  --require-benchmark-prometheus-artifacts \
  --require-prune-mode-semantics \
  --require-archive-api-evidence \
  --require-archive-api-mode minimal \
  --min-archive-api-depth-blocks 100 \
  --require-archive-tx-evidence \
  --require-archive-tx-mode minimal \
  --require-event-log-index-evidence \
  --require-event-log-index-mode minimal \
  --require-snapshot-profile-mode minimal \
  --require-retired-prune-mode minimal \
  --require-minimal-tail-prune \
  --require-size-reduction minimal:full:chaindataBytes=0.40 \
  --max-snapshot-point-sidecar-share-milli 1000 \
  --max-snapshot-point-snapshot-share-milli 200 \
  --max minimal.producer.snapshotSidecarShareMilli=350 \
  --min minimal.producer.tailPrunedThroughBlock=100000
```

For a long enough minimal-mode soak that should cross freezer shard boundaries,
add `--require-minimal-physical-tail-prune` so the run must also report
`tailPrunedFiles > 0`. Keep that gate off for short smoke samples where the
virtual tail can advance without deleting a physical shard file.

The checker verifies required mode coverage, rejects non-clean storage-alert
statuses by default, checks that each latest selected sample has a readable
Prometheus artifact with `gtron_storage_alert_status` and
`gtron_storage_alert_issue` for the row's `datadir`, verifies
`storageAlertStatus` against `gtron_storage_alert_status`
(`0=ok`, `1=warning`, `2=critical`) when the field is present, and when the row
carries a stage-pipeline cursor it also verifies the Prometheus
`gtron_storage_stage_pipeline_*` values, next-target/current cursors, and
stage/status/upstream labels match that same row. When the row carries
`signedColdPrune` or prune-boundary fields, it also checks that the Prometheus
`gtron_storage_signed_cold_prune` and
`gtron_storage_prune_boundary_block{field=...}` samples match. It confirms
minimal-mode signed cold lookup pruning plus tail-prune evidence, rejects
mode-semantics regressions such as `archive` rows with prune progress or
`blocks` rows with freezer-tail pruning, requires every selected row under
`--require-prune-mode-semantics` to carry a persisted `pruneMode` matching the
sampled `mode`, requires signed cold prune rows in every non-`archive` mode to
carry a valid chain-lookup prune boundary covered by `coldFreezerToBlock`, and
rejects `minimal` tail-prune boundaries that exceed the matching lookup-prune,
cold-freezer, or derived-index coverage boundary. It also rejects physical
freezer-tail file deletion outside `minimal`, and positive `tailPrunedFiles`
in `minimal` must be paired with a valid `tailPrunedThroughBlock`. With
`--require-minimal-physical-tail-prune`, it also requires the latest minimal
row to prove physical freezer-tail file deletion through `tailPrunedFiles`.
Use `--require-retired-prune-mode minimal` to require the latest minimal row to
prove `snapshot prune-retired` ran cleanly: `retiredPruneRan` must be true,
retired-prune missing/skipped-active counts must be zero, and the post-check
`snapshotRetired*` bytes/files counters must be zero, so immutable snapshot
rotation does not leave reclaimable files behind.
With `--require-archive-api-evidence`, at least one latest selected row must
prove historical archive API reads by reporting `archiveApiStatus=ok`,
`archiveApiChecks>0`, `archiveApiFailures=0`, a historical `archiveApiBlock`
below the sampled `height`, and `archiveApiMethods` covering the default
method set (`eth_getBlockByNumber`, `eth_getBlockByHash`,
`eth_getBlockTransactionCountByNumber`, `eth_getBlockTransactionCountByHash`,
`eth_getUncleCountByBlockNumber`, `eth_getUncleCountByBlockHash`,
`eth_getUncleByBlockNumberAndIndex`, `eth_getUncleByBlockHashAndIndex`,
`eth_getBlockReceipts`, `eth_getBalance`, `eth_getCode`, `eth_getStorageAt`,
and `eth_getLogs`). If
`archiveApiDepthBlocks` is present, the checker requires it to equal
`height - archiveApiBlock`; `archiveApiChecks`, `archiveApiFailures`,
`archiveApiBlock`, `archiveApiDepthBlocks`, and `height` must be integer block
or count evidence. Add
`--min-archive-api-depth-blocks BLOCKS` when the row must prove the archive
probe reached at least that far below the sampled head. Add
`--require-archive-api-mode minimal` so the latest pruned minimal row must prove its own archive reads
instead of letting an unpruned `archive` row satisfy the run. Repeat
`--require-archive-api-mode` or use `--require-archive-api-modes` when a run
must prove mode-local archive reads for more modes. Add
`--require-archive-tx-evidence` and `--require-archive-tx-mode minimal` when the
selected probe block is known to contain a transaction; this requires
same-row archive API evidence, `archiveApiTxProbe=true`, a `0x`-prefixed
32-byte `archiveApiTxHash`, and successful `eth_getTransactionByHash`,
`eth_getTransactionReceipt`, `eth_getTransactionByBlockNumberAndIndex`, and
`eth_getTransactionByBlockHashAndIndex` probes. When
`eth_getBlockReceipts` returns receipt logs, the same-row `eth_getLogs` probe
must include those logs before the archive API evidence is accepted; the
follow-up `eth_getLogsFiltered` label proves the same log can be found through
address/topic filters. Add `--require-archive-filtered-log-evidence` when the
benchmark targets a block with receipt logs and the acceptance gate must prove
that filtered event-log lookup path. Add
`--archive-api-method eth_call`, `--archive-api-method debug_traceCall`, and
`--archive-api-method eth_estimateGas` when the samples also pass
`--archive-api-call-data` against a known historical contract. Add
`--require-archive-trace-block` for runs collected with
`--archive-api-trace-block`; it requires successful `debug_traceBlockByNumber`
and `debug_traceBlockByHash` probes. If the row also
reports `chainLookupPruneToBlock` or `tailPrunedThroughBlock`, the archive API
block must be at or below the corresponding prune boundary so the row proves
post-prune archive reads rather than a latest-state fallback. With
`--require-event-log-index-evidence`, at least one latest derived-index row
must report active `eventLogIndexSegments`, a non-inverted
`eventLogIndexFromBlock`/`eventLogIndexToBlock` range whose end matches
`derivedIndexToBlock`, plus internally consistent address/topic key, posting,
average-fanout, max-fanout, singleton, and multi-posting counters. Rows that
also report `tailPrunedThroughBlock` must prove that the event-log index range
covers the tail-prune boundary. Add `--require-event-log-index-mode minimal` so
the latest pruned minimal row must prove its own event-log index coverage
instead of letting another mode's sidecar satisfy the run. Repeat
`--require-event-log-index-mode` or use `--require-event-log-index-modes` for
additional mode-local log-index proofs. Add
`--require-event-log-index-non-empty` when the sample is expected to include
logs and should prove non-empty address-index fanout.
The benchmark harness and Nile sampler automatically run
`scripts/dev/snapshot_manifest_profile.py <snapshot-dir> --json --verify-files`
when a manifest exists and records the active payload/sidecar split in the
`snapshot*Sidecar*` fields only after each segment file exists and matches the
manifest size. Add `--require-snapshot-profile-mode minimal` (or the plural
mode list) to the storage benchmark checker, or
`--require-snapshot-profile-evidence` to `nile_sync_acceptance.py`, so the
latest selected row must carry a valid manifest profile with
`snapshotManifestProfileStatus=ok`, `snapshotProfileVerifyFiles=true`,
`snapshotProfileVerifiedSegments == snapshotProfileSegments`, consistent
payload+sidecar totals, and a recomputable `snapshotSidecarShareMilli`. Then
use ordinary `--max` thresholds such as
`--max minimal.producer.snapshotSidecarShareMilli=350` for benchmark rows,
`--max snapshotSidecarShareMilli=350` for Nile rows, or family-specific fields
such as `snapshotEventLogSidecarShareMilli` to gate sidecar overhead in long
runs. Run the profiler directly on saved artifacts when a standalone gate is
useful; it reports `sidecarShareMilli` overall and per family (`latest`,
`state-history`, `chain-freezer`, `event-log`, `balance-trace`,
`section-bloom`, and `other`) and can fail a run with
`--max-sidecar-share-milli` or `--max-family-sidecar-share-milli`. Its JSON
also includes `pointIndexCandidates`, with direct segment, byte, payload,
sidecar, candidate-local sidecar-share, and snapshot-share counters for the
P3 recsplit/existence-filter candidates: `txHashLookup`, `eventLogIndex`,
`stateHistoryAccessor`, `latestBTree`, `chainFreezerAccessor`, `codeDomain`,
and `commitmentSnapshot`; `--max-point-sidecar-share-milli` and
`--max-point-snapshot-share-milli` can fail saved artifacts when any candidate
is too sidecar-heavy or consumes too much of the snapshot. The acceptance script
can apply the same gate to JSONL rows with
`--max-snapshot-point-sidecar-share-milli` and
`--max-snapshot-point-snapshot-share-milli`. The benchmark JSONL row exposes the
same values as
`snapshotPoint*{Segments,Bytes,PayloadBytes,SidecarBytes,SidecarShareMilli,SnapshotShareMilli}`.
Keep the compact/merged index-format decision evidence-driven: only consider
replacing sorted `chain-index`, `event-log-index`, accessor, or btree sidecars
after the profile shows they dominate disk or lookup latency in long samples.
Use `--require-size-reduction MODE:BASE_MODE:FIELD=RATIO` on comparable
multi-mode runs to require the latest selected `MODE` row to reduce a byte
counter by at least `RATIO` versus `BASE_MODE`; for example,
`minimal:full:chaindataBytes=0.40` requires minimal mode to use at least 40%
less hot chaindata than full mode in the same selected role. Keep ratio gates
off short smoke samples where fixed overhead dominates the measured bytes.
It also applies any project-specific numeric `--min`/`--max` thresholds.

Recorded samples:

- [2026-06-10 smoke sample](erigon-storage-benchmark-results-2026-06-10.md)
