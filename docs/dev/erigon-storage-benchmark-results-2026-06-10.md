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
