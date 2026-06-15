# Nile 7×24h soak

A long-running gtron Nile (testnet) sync used to demonstrate G1's
natural-language exit ("持续同步 7×24h 无 state root 分叉"). Independent of
the M0″ Phase 2 fixture replay, which is the formal G1 准入 check.

The soak runs against Nile rather than mainnet — `run-gtron.sh` passes
`--testnet` + Nile `--seednode`s, and `check-divergence.sh` compares against
`nile.trongrid.io`. Nile's shorter history and faster cold sync make it a
practical continuous-soak target; mainnet G1 validation still goes through
M0″ Phase 2.

## Layout

```
/Users/asuka/gtron-soak/
├── datadir/                                # gtron Pebble store
├── logs/
│   ├── gtron.err.log                       # gtron stderr (sync milestones, errors)
│   ├── gtron.out.log                       # gtron stdout (banner)
│   ├── monitor.{out,err}.log               # check-divergence.sh telemetry
│   └── soak-monitor.log                    # one line every 5 min: ts h=<height> peers=<n> gtron=<bid> oracle=<bid> {MATCH|DIVERGE|UNKNOWN|gtron-down}
└── scripts/
    ├── run-gtron.sh                        # gtron wrapper used by LaunchAgent
    └── check-divergence.sh                 # per-tick comparison vs nile.trongrid.io
```

LaunchAgents (~/Library/LaunchAgents):

| plist | StartInterval | KeepAlive |
| --- | --- | --- |
| `com.tronprotocol.gtron-soak.plist` | – | true |
| `com.tronprotocol.gtron-soak-monitor.plist` | 300s | – |

## Operations

```bash
# Start
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tronprotocol.gtron-soak.plist
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tronprotocol.gtron-soak-monitor.plist

# Stop
launchctl bootout gui/$(id -u)/com.tronprotocol.gtron-soak-monitor
launchctl bootout gui/$(id -u)/com.tronprotocol.gtron-soak

# Restart cleanly (NEW datadir; do this rarely — repeat restarts can trip seed
# rate-limits. 30+ min cooldown afterward).
launchctl bootout gui/$(id -u)/com.tronprotocol.gtron-soak
rm -rf /Users/asuka/gtron-soak/datadir/* /Users/asuka/gtron-soak/logs/gtron.*.log
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.tronprotocol.gtron-soak.plist

# Status
launchctl print gui/$(id -u)/com.tronprotocol.gtron-soak | grep -E "state|pid|last exit"
tail -F /Users/asuka/gtron-soak/logs/gtron.err.log
tail -F /Users/asuka/gtron-soak/logs/soak-monitor.log
curl -s http://127.0.0.1:8090/wallet/getnowblock | jq -r '.block_header.raw_data.number'
```

## Structured Sync Samples

Use `scripts/dev/nile_sync_sample.sh` to append one JSONL row with the current
HTTP height, peer count, sync elapsed time, and datadir size split:

```bash
scripts/dev/nile_sync_sample.sh \
  --datadir /Users/asuka/gtron-soak/datadir \
  --http http://127.0.0.1:8090 \
  --mode full \
  --start-unix "$(cat /Users/asuka/gtron-soak/sync-start.unix)" \
  --sync-log-file /Users/asuka/gtron-soak/logs/gtron.err.log \
  --pid-file /Users/asuka/gtron-soak/gtron.pid \
  --debug-metrics-url 'http://127.0.0.1:6060/debug/metrics?prefix=chain/freezer/' \
  --output /Users/asuka/gtron-soak/logs/sync-samples.jsonl
```

Run it from cron/systemd/LaunchAgent every few minutes during catch-up and the
7d soak. Each row includes `height`, `nodeInfoCurrentBlock`,
`nodeInfoHeightDelta`, `blockId`, `peers`, `sampleStatus`, `elapsedSeconds`,
`blocksPerSecond`, `blocksPerMinute`, `syncTargetHeight`,
`syncTargetLagBlocks`, `syncEtaSeconds`, `intervalSeconds`, `intervalBlocks`,
`intervalBlocksPerSecond`, `intervalSyncEtaSeconds`, `datadirBytes`,
`chaindataBytes`, `ancientBytes`, `snapshotBytes`, `coldArchiveBytes`,
`derivedIndexBytes`,
`derivedIndexFiles`, Pebble hot-store buckets such as `chaindataSSTBytes`,
`chaindataWALBytes`, `chaindataLogBytes`, `chaindataManifestBytes`,
`chaindataOptionsBytes`, snapshot buckets such as `snapshotLatestBytes`,
`snapshotHistoryBytes`, `snapshotCommitmentBytes`,
`snapshotRetiredDirectoryBytes`, and `snapshotOtherBytes`, plus matching
`*Files` / `*BytesDelta` fields, total
per-block byte rates, per-interval byte deltas/rates,
per-interval bytes-per-new-block fields such as
`intervalDatadirBytesPerBlock`, `intervalChaindataBytesPerBlock`,
`intervalChaindataSSTBytesPerBlock`, `intervalChaindataWALBytesPerBlock`,
`intervalColdArchiveBytesPerBlock`, `intervalDerivedIndexBytesPerBlock`,
`intervalReplayBytesPerBlock`, and `intervalDatadirOtherBytesPerBlock`,
plus interval growth attribution fields:
`intervalPositiveDiskGrowthBytes`, `intervalDiskGrowthPrimary`,
`intervalDiskGrowthPrimaryBytes`, `intervalDiskGrowthPrimaryShare`,
`intervalDetailedPositiveDiskGrowthBytes`,
`intervalDiskGrowthPrimaryDetailed`,
`intervalDiskGrowthPrimaryDetailedBytes`,
`intervalDiskGrowthPrimaryDetailedShare`,
`intervalChaindataGrowthShare`, `intervalAncientGrowthShare`,
`intervalSnapshotGrowthShare`, `intervalReplayGrowthShare`,
`intervalDatadirOtherGrowthShare`, `intervalColdArchiveGrowthShare`, and
`intervalDerivedIndexGrowthShare`, plus hot/cold interval ratios
`intervalColdToHotGrowthRatio`, `intervalAncientToHotGrowthRatio`,
`intervalSnapshotToHotGrowthRatio`, and
`intervalDerivedIndexToHotGrowthRatio`. The top-level primary growth bucket is
computed from non-overlapping top-level buckets (`chaindata`, `ancient`,
`snapshot`, `replay`, `other`). The detailed primary bucket is computed from
Pebble file buckets, ancient table buckets, snapshot subdirectories, replay,
and datadir-other bytes; use it to spot whether interval growth is driven by
SST/WAL, freezer tables, or derived snapshot indexes. Cold archive and derived
index shares are overlapping diagnostic views for archive/index growth; the
interval hot/cold ratios compare positive cold/index growth against positive
Pebble hot growth. Rows also include a compact operator summary:
`soakEfficiencyWindow`, `soakEfficiencyStatus`,
`soakEfficiencyBlocksPerSecond`, `soakEfficiencyEtaSeconds`,
`soakEfficiencyDatadirBytesPerBlock`, `soakEfficiencyHotBytesPerBlock`,
`soakEfficiencyColdArchiveBytesPerBlock`,
`soakEfficiencyDerivedIndexBytesPerBlock`, `soakEfficiencyDiskPrimary`,
`soakEfficiencyDiskPrimaryBytes`, `soakEfficiencyDiskPrimaryShare`,
`soakEfficiencyStageBottleneck`,
`soakEfficiencyStageBottleneckLagBlocks`, and
`soakEfficiencyStageBottleneckLagShare`. When a previous JSONL sample exists,
these `soakEfficiency*` fields summarize the latest interval; otherwise they
fall back to cumulative sync and current disk distribution. Rows also include
`coldToHotBytesRatio`,
`derivedIndexToHotBytesRatio`, `chaindataSSTToHotBytesRatio`,
`chaindataWALToHotBytesRatio`, `chaindataWALToSSTBytesRatio`,
`derivedIndexSnapshotBytesRatio`, `coldArchiveDatadirShare`,
`derivedIndexColdArchiveRatio`, `ancientFiles`, `snapshotFiles`, and
`coldArchiveFiles`, plus the repo commit used to produce the sample. When
`--pid-file` is provided, rows also include `processPid`, `processStatus`,
`processRssBytes`, `processCpuPercent`, `processUptimeSeconds`, and
`processOpenFiles` so catch-up throughput can be correlated with local resource
pressure. Derived index bytes are the chain-index/accessor, balance-trace,
section-bloom, and event-log/index sidecars inside `state-snapshots`;
`snapshotCommitmentBytes` tracks commitment root/checkpoint/branch snapshot
files separately from `snapshotOtherBytes`, while `snapshotBytes` remains the
whole snapshot directory size for backward comparisons.
If the launch wrapper does not already maintain a pid file, write
`echo "$$" > /Users/asuka/gtron-soak/gtron.pid` immediately before `exec gtron`;
the shell PID is retained by the gtron process after `exec`.
When `--debug-metrics-url` points at a gtron `/debug/metrics` endpoint, rows
also include `debugMetricsStatus`, `debugMetricsPrefix`, `debugMetricsCount`,
`debugMetricsNumericCount`, `debugMetricsNames`, the raw `debugMetrics` map,
and convenience freezer gauges such as `debugMetricChainFreezerBlocks`,
`debugMetricChainFreezerPasses`, `debugMetricChainFreezerLastPassDuration`,
and `debugMetricChainFreezerPebbleSize`. Use a narrowed URL such as
`http://127.0.0.1:6060/debug/metrics?prefix=chain/freezer/` during long soaks
so the JSONL row stays compact while still correlating internal gauges with
stage progress and disk growth.
`sampleStatus` is `ok` when HTTP calls work, peers are present, and
`getnodeinfo.currentBlock` agrees with `getnowblock`; otherwise it reports the
first visible sampling issue such as `no-peers`, `height-mismatch`, or an HTTP
endpoint error.

When `--sync-log-file` points at the gtron runtime log, the sampler parses the
latest `Imported chain segment` summary and emits `syncLog*` fields. These
include the segment throughput (`syncLogSegmentBlocks`, `syncLogSegmentTxs`,
`syncLogBlocksPerSecond`, `syncLogTxsPerSecond`, `syncLogSegmentHead`,
`syncLogSegmentRemain`), the observed stage planner result
(`syncLogStageComplete`, `syncLogStageCompleted`, `syncLogStageScheduled`,
`syncLogStageIncomplete`, `syncLogStageCompletionRatio`,
`syncLogStageTasksPerBlock`, `syncLogStageCompletedPerBlock`,
`syncLogStageNext*`, `syncLogStageBlockedStatus`), and the explicit planned
execution schedule (`syncLogExecPlanBlocks`, `syncLogExecPlanStages`,
`syncLogExecPlanBodyStages`, `syncLogExecPlanPostBodyStages`,
`syncLogExecPlanExecutionStages`, `syncLogExecPlanCommitmentStages`,
`syncLogExecPlanFinishStages`, `syncLogExecPlanFirst`, `syncLogExecPlanLast`).
Rows also include the phase-level cursor from the same import batch
(`syncLogPhaseCursorComplete`, `syncLogPhaseCursorCompletedPhases`,
`syncLogPhaseCursorScheduledPhases`, `syncLogPhaseCursorIncompletePhases`,
`syncLogPhaseCursorCompletionRatio`, `syncLogPhaseCursorCompletedTasks`,
`syncLogPhaseCursorScheduledTasks`,
`syncLogPhaseCursorTaskCompletionRatio`,
`syncLogPhaseCursorCurrent*`, `syncLogPhaseCursorNextBlock`, and
`syncLogPhaseCursorBlockedStatus`), so a blocked batch names whether the
current staged task is still in execution, commitment, or finish.
For the batch-phase stage planner, a fully completed N-block import normally
reports `syncLogStageScheduled = syncLogExecPlanStages = 4*N`; a blocked row
shows how many bodies/execution/commitment/finish tasks completed before the
next missing or mismatched task. The sampler also records
`syncLogAppliedPlanBlocks`, `syncLogAppliedPlanStages`,
`syncLogAppliedPlanBodyStages`, `syncLogAppliedPlanPostBodyStages`,
`syncLogAppliedPlanExecutionStages`, `syncLogAppliedPlanCommitmentStages`,
`syncLogAppliedPlanFinishStages`, `syncLogAppliedPlanFirst`, and
`syncLogAppliedPlanLast`, plus per-block ratios, so partial imports can be
distinguished from the larger attempted execution plan. These fields are
log-derived and complement the
`stage*` fields from `gtron db stage-status`: log fields explain what the last
import batch planned and observed, while DB fields show the persisted recovery
boundary. The same log parser also records startup recovery fields, including
`syncStartupPipelineOrderChecked`, `syncStartupPipelineOrderIssues`,
`syncStartupPipelineOrderFirstIssue`, `syncStartupPipelineOrderReadErrors`, and
`syncStartupPipelineOrderFirstReadErrorStage`, plus
current-head completion fields
(`syncStartupHeadCompletionChecked`,
`syncStartupHeadCompletionHasPrefix`,
`syncStartupHeadCompletionLastStage`,
`syncStartupHeadCompletionLastBlock`,
`syncStartupHeadCompletionFillStages`,
`syncStartupHeadCompletionWritten`, and
`syncStartupHeadCompletionComplete`), plus
`syncStartupPipelineOrderRepairChecked`,
`syncStartupPipelineOrderRepairDeleted`, and
`syncStartupPipelineOrderRepairUpdated`, plus the repaired restart cursor
(`syncStartupPipelineCursorChecked`, `syncStartupPipelineCursorComplete`,
`syncStartupPipelineCursorLastStage`, `syncStartupPipelineCursorLastBlock`,
`syncStartupPipelineCursorHasNext`, `syncStartupPipelineCursorNextStage`, and
`syncStartupPipelineCursorBlocked`), so restart samples show whether the node
repaired stale rows, completed missing downstream sync stages for the current
canonical head, revalidated full sync-stage ordering after staged-body restore,
which persisted progress row failed to decode first, and which sync stage the
next import pass should advance.

To include staged-sync progress without making the sampler open a live Pebble
datadir, pass a captured `gtron db stage-status` output file:

```bash
scripts/dev/nile_sync_sample.sh \
  --datadir /Users/asuka/gtron-soak/datadir \
  --http http://127.0.0.1:8090 \
  --stage-status-file /Users/asuka/gtron-soak/logs/stage-status.txt \
  --output /Users/asuka/gtron-soak/logs/sync-samples.jsonl
```

Rows then include the parsed stage map plus flat sync-stage fields:
`stageSyncInventory`, `stageSyncBodies`, `stageSyncBodiesReady`,
`stageSyncImport`, `stageSyncExecution`, `stageSyncCommitment`,
`stageSyncFinish`, `stageCanonicalFinish`, `stageChainFreezer`, and
`stageSnapshotEventLogBuild`. The sampler also derives
`stageSyncBodiesReadyGapBlocks`, `stageSyncImportExecutionLagBlocks`,
`stageSyncExecutionCommitmentLagBlocks`,
`stageSyncCommitmentFinishLagBlocks`, and `stageSyncFinishHeadLagBlocks`, plus
`stageSyncBottleneck`/`stageSyncBottleneckLagBlocks`,
`stageSyncPipelineLagBlocks`, and `stageSyncBottleneckLagShare` for the largest
current pipeline lag and its share of total staged-sync backlog. It also reports
`stageSyncPipelineMonotonic`, `stageSyncPipelineViolation`,
`stageSyncPipelineViolationCount`, `stageSyncPipelineMaxViolationBlocks`, and
`stageSyncPipelineViolations`, so a sample row can flag any downstream stage
that advanced ahead of its upstream stage. Rows also include an operator-facing
full staged sync summary: `fullStagedSyncStatus`,
`fullStagedSyncReady`, `fullStagedSyncCompleteAtHead`,
`fullStagedSyncPresentStageCount`, `fullStagedSyncVerifiedStageCount`,
`fullStagedSyncMissingStages`, `fullStagedSyncHashIssues`,
`fullStagedSyncCompleteBlock`, `fullStagedSyncHeadBlock`,
`fullStagedSyncHeadLagBlocks`, `fullStagedSyncCompletionRatio`,
`fullStagedSyncPipelineLagBlocks`, `fullStagedSyncBottleneck`, and
`fullStagedSyncBottleneckLagBlocks`,
`fullStagedSyncBottleneckLagShare`, `fullStagedSyncStageCoverageRatio`, and
`fullStagedSyncVerificationRatio`. `fullStagedSyncReady` means the six
sync stages (`SyncBodies`, `SyncBodiesReady`, `SyncImport`, `SyncExecution`,
`SyncCommitment`, `SyncFinish`) are present, hash/verification checks did not
flag them, and the pipeline is monotonic; `fullStagedSyncCompleteAtHead` is the
stricter condition where `SyncFinish` has caught up to the sampled HTTP head.
Across consecutive JSONL rows it
also reports `restartRecoveryStatus`, `heightRegressionBlocks`,
`stageProgressRegressionCount`, `stageProgressMaxRegressionBlocks`, and
`stageProgressRegressions`. These fields make restart/repair events visible
without manually diffing samples: `height-regression` means the HTTP head moved
backward, `stage-regression` means one or more persisted stage rows moved
backward or disappeared after repair, `pipeline-violation` means downstream
stage progress is ahead of its upstream stage, `stalled` means no height or
stage movement was observed while still lagging the target, and `progressing`
means the current interval advanced. The row also reports
`stageSyncFinishHeadEtaSeconds`,
`stageChainFreezerHeadLagBlocks`, `stageChainFreezerHeadEtaSeconds`,
`stageSnapshotEventLogBuildHeadLagBlocks`, and
`stageSnapshotEventLogBuildHeadEtaSeconds` when the previous sample is
available enough to derive a per-stage rate. When the output JSONL already has
a previous sample, rows also include interval stage throughput fields such as
`intervalStageSyncImportBlocks`,
`intervalStageSyncExecutionBlocks`,
`intervalStageSyncCommitmentBlocks`,
`intervalStageSyncFinishBlocks`, matching `*BlocksPerSecond` values, and
adjacent-stage ratios such as `intervalStageSyncExecutionToImportRatio` and
`intervalStageSyncFinishToCommitmentRatio`, so long-running samples show both
where backlog is accumulating and whether each downstream stage is keeping up.
Rows also carry stage-stall fields derived from the previous JSONL sample:
`stageLastProgressUnix`, `stageStalled`, `stageStalledCount`,
`stageStalledStage`, `stageStalledSeconds`, `stageStalledLagBlocks`, and
`stageStalls`. These flag an individual lagging stage even when the HTTP head
or another stage is still advancing. For example, `stageStalledStage =
stageSyncExecution` with positive `stageStalledLagBlocks` means `SyncImport`
moved ahead while `SyncExecution` did not; treat sustained `stage-stalled`
health warnings during catch-up as the next profiling target.

## Full Staged Sync Validation

"Full staged sync" means a cold Nile node can make progress through explicit,
restart-safe stages instead of treating sync as one opaque block-import loop.
In go-tron today the observable staged pipeline is:

1. `SyncInventory`: peer target height discovered from chain inventory.
2. `SyncBodies`: block bodies downloaded and persisted as staged rows.
3. `SyncBodiesReady`: contiguous staged bodies are ready to drain.
4. `SyncImport`: staged bodies were handed to block import.
5. `SyncExecution`: block execution completed.
6. `SyncCommitment`: state commitment/update work completed.
7. `SyncFinish`: the imported range is fully verified and safe for downstream
   freezer/prune/snapshot consumers.

For a production Nile run, capture these checks:

1. Start from an empty datadir and sample every few minutes with
   `nile_sync_sample.sh`.
2. Periodically write `gtron db stage-status --datadir <dir>` output to the
   `--stage-status-file` path used by the sampler.
3. Confirm `stageSyncPipelineMonotonic=true`, or manually check
   `stageSyncBodies >= stageSyncBodiesReady >= stageSyncImport >=
   stageSyncExecution >= stageSyncCommitment >= stageSyncFinish`.
4. Confirm `stageSyncBottleneck` moves as expected during catch-up; a persistent
   large `import-execution`, `execution-commitment`, `commitment-finish`, or
   `finish-head` lag is the signal to profile that stage.
5. Stop gtron mid-catch-up, restart without deleting the datadir, and confirm
   staged bodies are recovered or pruned to a contiguous prefix: the next sample
   should not show `SyncBodiesReady` ahead of a missing or mismatched body row.
6. After catch-up, run at least one stopped-node `--offline-db-check` sample and
   keep the JSONL row together with `gtron.err.log` and `stage-status.txt`.

If any stage row is missing, unbound, ahead of canonical head, or hash-mismatched,
the restart path should repair it by keeping only a contiguous hash-bound prefix.
Treat repeated repairs after a clean restart as a bug, not an expected steady
state.

When the node is stopped, add `--offline-db-check` to also run
`gtron db storage-alerts --datadir <dir>` and include freezer/stage/snapshot
alert fields in the row. Do not enable that flag against a live Pebble datadir
unless the DB can be opened by the diagnostic command.

## Shielded TRC20 Replay Recovery

If a Nile node was already synced past the shielded TRC20 activation window
with an older binary, rebuilding the binary alone does not rewrite contract
storage that was materialized by earlier blocks. Rewind and replay from the
block immediately before proposal #39:

```bash
# One-shot recovery flag. Remove it after the node has replayed past the
# failing shielded TRC20 block range.
--sync.restart-from 6360100
```

This replays the historical proposal at block 6,360,101 and every subsequent
shielded TRC20 mint/transfer with the current Sapling-enabled precompile
implementation.

## Linux Sync Service Profile

For a dedicated Nile sync host with roughly 60 GiB RAM, the current profiling
baseline uses an 8 GiB Pebble block cache, 256 MiB memtables, and relaxed L0
thresholds:

```ini
[Service]
Environment=GOMEMLIMIT=32GiB
ExecStart=/bin/bash -lc 'exec /data/gtron/go-tron/build/bin/gtron \
    --datadir       /data/gtron/nile/datadir \
    --testnet \
    --p2p.port      18888 \
    --http.port     8090 \
    --jsonrpc.port  8545 \
    --grpc.port     50051 \
    --pprof.port    6060 \
    --pprof.addr    127.0.0.1 \
    --maxpeers      30 \
    --db.cache      8192 \
    --db.handles    8192 \
    --db.memtable   256 \
    --db.l0.compact 8 \
    --db.l0.stop    64 \
    --seednode      44.236.192.97:18888 \
    --seednode      44.236.125.107:18888 \
    --seednode      44.232.119.174:18888 \
    --seednode      52.39.105.180:18888 \
    --seednode      54.70.52.47:18888'
MemoryHigh=32G
MemoryMax=40G
LimitNOFILE=65536
```

If the host only has around 27 GiB available to gtron, keep the tighter
`GOMEMLIMIT=20GiB`, `MemoryHigh=20G`, `MemoryMax=23G` settings instead.

## Sync expectations

Nile's head is far lower than mainnet's, so cold sync from genesis is short.
The 2026-05-15 restart reached h≈59k within the first hours, MATCH'ing
`nile.trongrid.io` block IDs at every monitor tick. Plan for a brief
catch-up + 7d steady-state.

While catching up, `soak-monitor.log` will show
`oracle=<nile.trongrid.io blockID at gtron-head> gtron=<our blockID>`. MATCH
means historical block hashes are byte-identical (the G1 invariant).
DIVERGE at any height is the alert: capture the height, archive the
block, and add a divergence-allowlist entry or open a parity bug.

`gtron-down` lines indicate the HTTP endpoint isn't responding — common
during the first 15-30s after launchd restart while gtron initializes,
otherwise investigate (probably crashed, check `gtron.err.log`).

## Disk

Nile's datadir is small — observed ~472 MB at h≈59k on 2026-05-15. Disk
pressure is not a concern for the Nile soak (mainnet would be a different
story: java-tron mainnet datadir is 1-2 TB). Still worth a periodic
`du -sh /Users/asuka/gtron-soak/datadir` glance.

## Known constraints

- Seed-side rate limit per source IP: ~3-4 sync attempts in one session
  trip a session-wide ban; 30+ minute cooldown of *no reconnect attempts* lifts
  it. The per-addr dial throttle in `p2p.Server` (commit `bb52bb7`) prevents
  the maintainCh thundering herd that previously caused this within minutes.
- Discovery service routing table is seeded from `params.NileBootstrapNodes`
  (`--testnet` selects the Nile list) + the explicit `--seednode` flags;
  dead seeds in either list don't break sync as long as one accepts
  TRON-Hello.
