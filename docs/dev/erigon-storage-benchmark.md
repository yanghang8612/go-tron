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
- `balanceTracePruneToBlock`
- `balanceTraceBlockRowsPruned`
- `balanceTraceAccountRowsPruned`
- `sectionBloomPruneToSection`
- `sectionBloomRowsPruned`
- `signedColdPrune`
- `chainLookupPruneToBlock`
- `chainLookupBlockIndexes`
- `chainLookupTxIndexes`
- `tailPrunedThroughBlock`
- `tailPrunedFiles`
- `historyWindow`

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

This builds the cold chain-freezer segment, signs `snapshot-catalog.json`, and
runs `gtron snapshot prune-chain-lookups` with the catalog signer as a trusted
key for each selected mode. `blocks` stops there and should report lookup-prune
coverage with no freezer-tail prune. `minimal` then restarts once so the
tail-prune lifecycle can run. Use a small `--history-window` for short dev
samples; production and soak runs should use the intended retention window.

When the same run also passes `--build-derived-indexes`, the signed drill also
runs `gtron snapshot prune-balance-traces` and
`gtron snapshot prune-section-blooms` after catalog publication. Those commands
verify the signed catalog and compare hot rows against the cold sidecars before
deleting anything, so the JSON row can report trace/bloom hot-row reclamation
alongside chain lookup pruning.

After the drill, `gtron db freezer-status --datadir <dir>` prints the local
freezer head/tail plus per-table physical tail, hidden tail, shard IDs, and
visible/hidden sizes. The header also includes `repairApplied`,
`repairTargetHead`, `repairTargetTail`, and `repairRecordedAt`; if any
`Freezer repair table` rows appear, preserve them with the benchmark artifacts
because the freezer had to truncate table bounds on open before serving the
status view. The same repair record is persisted in the freezer directory as
`repair.json`, so a later readonly status sample can still surface the last
automatic repair. Capture this alongside the JSONL row when validating
minimal-mode physical shard reclamation.

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

Recorded samples:

- [2026-06-10 smoke sample](erigon-storage-benchmark-results-2026-06-10.md)
