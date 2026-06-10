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
  --modes full,minimal,archive \
  --target-blocks 120 \
  --freezer-margin 3 \
  --freezer-interval 1s \
  --build-cold-freezer \
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

`--build-cold-freezer` runs `gtron snapshot build-freezer` after stopping each
producer so cold chain-freezer snapshot bytes are included in the size split.
The freezer build writes or preserves the manifest chain identity, so the output
can be signed in a follow-up drill. It does not sign catalogs or mark
lookup-prune progress by itself; it is a measurement step, not a production
prune workflow.

## Sync Profile

Run one dev witness and one fresh follower per mode. The row measures follower
catch-up time and follower datadir size:

```bash
scripts/dev/storage_benchmark.sh \
  --profile sync \
  --modes full,minimal \
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
- `minimal`: should be evaluated with a separate production prune drill after
  verified cold coverage exists; the harness measures baseline size and can
  include cold freezer snapshot bytes.
- `archive`: should retain all temporal state rows and is expected to consume
  more hot storage.

For production acceptance, collect at least:

1. Short dev samples from this harness for every changed storage slice.
2. Long-running private-chain soak samples with the same JSON schema.
3. Mainnet/Nile replay or fixture samples for archive reads after hot prune.

Recorded samples:

- [2026-06-10 smoke sample](erigon-storage-benchmark-results-2026-06-10.md)
