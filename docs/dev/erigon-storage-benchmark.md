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
  --modes full,blocks,minimal,archive \
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
- `coldFreezerToBlock`
- `derivedIndexToBlock`
- `derivedIndexSegments`
- `derivedIndexBuildSeconds`
- `eventLogIndexSegments`
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
  --history-window 8 \
  --keep
```

This builds the cold chain-freezer segment, signs `snapshot-catalog.json`, runs
`gtron snapshot prune-chain-lookups` with the catalog signer as a trusted key,
and then runs `gtron snapshot prune-retired` for each selected mode. `blocks`
stops there and should report lookup-prune plus retired snapshot-file cleanup
with no freezer-tail prune. `minimal` then restarts once so the tail-prune
lifecycle can run. Use a small `--history-window` for short dev samples;
production and soak runs should use the intended retention window.

When the same run also passes `--build-derived-indexes`, the signed drill also
runs `gtron snapshot prune-balance-traces` and
`gtron snapshot prune-section-blooms` after catalog publication. Those commands
verify the signed catalog and compare hot rows against the cold sidecars before
deleting anything, so the JSON row can report trace/bloom hot-row reclamation
alongside chain lookup pruning.

`prune-retired` verifies the active manifest before deleting retired snapshot
segment files. The JSON row reports the retired segment count, deleted/missing
file count, active-file skips, and reclaimed bytes through the `retiredPrune*`
fields.

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
`freezerAlertStatus`, `freezerAlertIssues`, `freezerAlertHiddenBytes`,
`freezerAlertDetails`, `stageVerifyStatus`, `stageVerifyIssues`,
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
  --modes full,blocks,minimal \
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
Run it with `--offline-db-check` only after the node is stopped to add
`storage-alerts` stage/freezer/snapshot diagnostics, including
`stageVerifyDetails` entries such as `SyncBodies`/`SyncBodiesReady`
staged-body mismatches.

## Interpreting Results

- `full`: should keep recent hot state/history and use local freezer for old
  chain data.
- `blocks`: should preserve complete local chain-freezer history while allowing
  state/history and hot lookup pruning once cold coverage exists.
- `minimal`: should be evaluated with `--signed-cold-prune` after verified
  cold chain-freezer, chain-index, and indexed event-log coverage exists; the
  drill reports lookup-prune coverage and the restart-time tail-prune boundary.
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
  --require-modes full,blocks,minimal,archive \
  --require-prometheus-artifacts \
  --require-prune-mode-semantics \
  --require-minimal-tail-prune \
  --min minimal.producer.tailPrunedThroughBlock=100000
```

The checker verifies required mode coverage, rejects non-clean storage-alert
statuses by default, checks that each latest selected sample has a readable
Prometheus artifact with `gtron_storage_alert_status` and
`gtron_storage_alert_issue`, and when the row carries a stage-pipeline cursor it
also verifies the Prometheus `gtron_storage_stage_pipeline_*` values and next
target match that same row. It confirms minimal-mode signed cold lookup pruning
plus tail-prune evidence, rejects mode-semantics regressions such as
`archive` rows with prune progress or `blocks` rows with freezer-tail pruning,
and applies any project-specific numeric `--min`/`--max` thresholds.

Recorded samples:

- [2026-06-10 smoke sample](erigon-storage-benchmark-results-2026-06-10.md)
