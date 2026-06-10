# Erigon-Style Storage Benchmark Results: 2026-06-10

This file records the first smoke sample from `scripts/dev/storage_benchmark.sh`.
It is a harness validation sample, not a production acceptance result.

## Environment

- Time: 2026-06-10T02:08:43Z
- Branch: `claude/async-commit-buffer-model`
- HEAD: `3a0c86f`
- Worktree: dirty
- Profile: `producer`
- Target block: 8
- Command:

```bash
GTRON="$tmp/gtron" scripts/dev/storage_benchmark.sh \
  --profile producer \
  --modes full,minimal,archive \
  --target-blocks 8 \
  --timeout 120 \
  --workdir "$tmp" \
  --output "$tmp/results.jsonl" \
  --keep
```

## Results

| mode | elapsedSeconds | height | datadirBytes | chaindataBytes | ancientBytes | snapshotBytes | ancientFiles | snapshotFiles |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| full | 23 | 8 | 98,304 | 57,344 | 36,864 | 0 | 10 | 0 |
| minimal | 20 | 8 | 98,304 | 57,344 | 36,864 | 0 | 10 | 0 |
| archive | 22 | 8 | 114,688 | 73,728 | 36,864 | 0 | 10 | 0 |

## Notes

- The run proves the benchmark harness can build the current `gtron`, run the
  dev producer profile, and emit comparable JSONL rows.
- `minimal` and `full` match in this short sample because no signed cold
  chain-freezer catalog was installed and no verified lookup-prune progress was
  available. Minimal freezer tail reclamation intentionally stays gated behind
  cold coverage.
- `archive` is larger even at block 8 because archive mode enables temporal
  state capture and retains the hot history rows.
- This sample is too short for acceptance thresholds. Production acceptance
  still needs long-running samples with signed cold coverage plus post-prune
  restart checks.

## Signed Cold Prune Smoke

This sample validates the automated `minimal` signed cold prune drill added to
the benchmark harness.

## Signed Cold Prune Environment

- Time: 2026-06-10T02:46:14Z
- Branch: `master`
- HEAD: `7e6c0c8`
- Worktree: dirty
- Profile: `producer`
- Target block: 10
- Command:

```bash
GTRON="$tmp/gtron" scripts/dev/storage_benchmark.sh \
  --profile producer \
  --modes minimal \
  --target-blocks 10 \
  --timeout 150 \
  --freezer-margin 3 \
  --freezer-interval 1s \
  --signed-cold-prune \
  --history-window 2 \
  --workdir "$tmp/bench" \
  --output "$tmp/results.jsonl" \
  --keep \
  --no-build
```

## Signed Cold Prune Results

| mode | height | coldFreezerToBlock | signedColdPrune | chainLookupPruneToBlock | chainLookupBlockIndexes | chainLookupTxIndexes | tailPrunedThroughBlock | tailPrunedFiles | historyWindow |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| minimal | 10 | 6 | 1 | 6 | 7 | 1 | 6 | 0 | 2 |

## Signed Cold Prune Notes

- The drill completed the freezer build, catalog signing, trusted lookup prune,
  and post-prune `minimal` restart path.
- `tailPrunedFiles=0` is expected for this short sample: the logical freezer
  tail advanced through block 6, but the tiny local freezer did not reclaim a
  physical shard file.
- This is still a harness validation sample. Production acceptance needs
  long-running private-chain and replay samples with realistic retention
  windows and space thresholds.
