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
  --jsonrpc http://127.0.0.1:8545 \
  --mode full \
  --start-unix "$(cat /Users/asuka/gtron-soak/sync-start.unix)" \
  --sync-log-file /Users/asuka/gtron-soak/logs/gtron.err.log \
  --pid-file /Users/asuka/gtron-soak/gtron.pid \
  --debug-metrics-url 'http://127.0.0.1:6060/debug/metrics?prefix=chain/freezer/' \
  --prometheus-output /Users/asuka/gtron-soak/logs/sync-sample.prom \
  --output /Users/asuka/gtron-soak/logs/sync-samples.jsonl
```

For archive-read evidence, add `--archive-api-probe`. The sampler probes
`eth_getBlockByNumber`, `eth_getBalance`, `eth_getCode`, `eth_getStorageAt`,
and `eth_getLogs` at `height-1` by default and emits `archiveApi*` fields. If
the probed block contains a transaction, it also probes
`eth_getTransactionByHash` and `eth_getTransactionReceipt` and emits
`archiveApiTx*` fields. Use `--archive-api-block`, `--archive-api-address`, and
`--archive-api-storage-slot` to pin the historical target; choose a block with
at least one transaction when the acceptance gate uses
`--require-archive-tx-evidence`. Add `--archive-api-call-data` when the target
is a known historical contract and the sample should also prove `eth_call`.
The sampler counts only shape-valid JSON-RPC results as successful: block reads
must return an object, account/code/storage/call reads must return hex strings,
logs must return a list, and transaction/receipt reads must return objects
rather than `null`. The block result must also carry the requested historical
block number, and transaction/receipt results must carry the transaction hash
selected from that block.

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
`snapshotRetiredDirectoryBytes`, and `snapshotOtherBytes`, snapshot manifest
profile counters such as `snapshotManifestProfileStatus`,
`snapshotPayloadBytes`, `snapshotSidecarBytes`, `snapshotSidecarShareMilli`,
and per-family `snapshot*Sidecar*` bytes/share fields, plus matching
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
`intervalDerivedIndexGrowthShare`. When `--prometheus-output` is set, each
sample also writes a low-cardinality Prometheus text artifact and records
`samplePrometheus` plus `samplePrometheusStatus` in the JSONL row. The artifact
exposes sync height, target/stage lag, full-staged-sync status/ready/coverage
and bottleneck gauges, throughput, hot/cold/index byte gauges, snapshot sidecar
share, archive probe failures, and sample/soak health status for external
scrape jobs.
Rows also include hot/cold interval ratios
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
pressure. Rows produced with `--archive-api-probe` also include
`archiveApiStatus`, `archiveApiChecks`, `archiveApiFailures`,
`archiveApiBlock`, `archiveApiMethods`, `archiveApiEndpoint`,
`archiveApiTxProbe`, `archiveApiTxHash`, and `archiveApiTxMethods` for
historical JSON-RPC read validation. Derived index bytes are the
chain-index/accessor, balance-trace,
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
`syncLogPhaseCursorCurrent*`, `syncLogPhaseCursorCurrentTaskCount`,
`syncLogPhaseCursorCurrentTaskRemaining`, `syncLogPhaseCursorNextBlock`,
`syncLogPhaseCursorNextPhase`, `syncLogPhaseCursorNextCanonical`,
`syncLogPhaseCursorNextSync`, and `syncLogPhaseCursorBlockedStatus`), so a
blocked batch names whether the current staged task is still in execution,
commitment, or finish and how many tasks remain in that phase.
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
boundary. `syncLogTxTop` records the top transaction contract types in the same
segment window, which separates contract-heavy `TriggerSmartContract` windows
from transfer-heavy windows when throughput changes. The same log parser also
records startup recovery fields, including
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
datadir, pass a captured `gtron db stage-status --json` output file. The
sampler still accepts the legacy text output for older binaries.

```bash
gtron db stage-status --json \
  --datadir /Users/asuka/gtron-soak/datadir \
  > /Users/asuka/gtron-soak/logs/stage-status.json

scripts/dev/nile_sync_sample.sh \
  --datadir /Users/asuka/gtron-soak/datadir \
  --http http://127.0.0.1:8090 \
  --stage-status-file /Users/asuka/gtron-soak/logs/stage-status.json \
  --output /Users/asuka/gtron-soak/logs/sync-samples.jsonl
```

Rows then include the parsed stage map plus flat sync-stage fields:
`stageSyncInventory`, `stageSyncBodies`, `stageSyncBodiesReady`,
`stageSyncImport`, `stageSyncExecution`, `stageSyncCommitment`,
`stageSyncFinish`, `stageCanonicalFinish`, `stageChainFreezer`, and
`stageSnapshotEventLogBuild`. When the captured JSON contains structured
`issueDetails`, the sampler records `stageIssueRows`, `stageIssueDetails`,
`stageOrderIssueRows`, `stageSyncOrderIssueRows`,
`stageStorageOrderIssueRows`, and `stageOrderIssueDetails`, so long soaks can
filter directly on the stage edge that broke instead of parsing the free-form
`issues` strings. Rows also include
`stagePipelineComplete`, `stagePipelinePending`, `stagePipelineIssues`,
`stagePipelineNext`, `stagePipelineNextStatus`,
`stagePipelineNextTarget`, `stagePipelineNextUpstream`,
`stagePipelineNextCurrent`, and `stagePipelineTasks` from the `stage-status`
`pipeline` object. These fields describe the next canonical or
post-`Finish` snapshot/prune/freezer stage edge that can be advanced; pending
tasks are backlog diagnostics, not health failures by themselves.
The sampler also derives
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
`fullStagedSyncUnverifiedStages`, `fullStagedSyncCompleteBlock`,
`fullStagedSyncHeadBlock`, `fullStagedSyncHeadLagBlocks`,
`fullStagedSyncCompletionRatio`, `fullStagedSyncPipelineLagBlocks`,
`fullStagedSyncBottleneck`, and
`fullStagedSyncBottleneckLagBlocks`,
`fullStagedSyncBottleneckLagShare`, `fullStagedSyncStageCoverageRatio`, and
`fullStagedSyncVerificationRatio`, plus
`fullStagedSyncStageDetails` for per-stage field/block/hash/verification
evidence. `fullStagedSyncReady` means the six
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
2. Periodically write `gtron db stage-status --json --datadir <dir>` output to
   the `--stage-status-file` path used by the sampler.
3. Confirm `stageSyncPipelineMonotonic=true`, or manually check
   `stageSyncBodies >= stageSyncBodiesReady >= stageSyncImport >=
   stageSyncExecution >= stageSyncCommitment >= stageSyncFinish`.
   Full staged sync requires each sync stage to be hash-verified:
   `SyncBodies`/`SyncBodiesReady` may be `staged` or `canonical`; import,
   execution, commitment, and finish must be `canonical`. If a required stage
   is present but lacks verification evidence, the sampler reports
   `fullStagedSyncStatus=unverified-stage` and does not mark it ready.
   The same row is also marked with the `full-staged-sync-unverified` critical
   health issue.
4. Confirm `stageSyncBottleneck` moves as expected during catch-up; a persistent
   large `import-execution`, `execution-commitment`, `commitment-finish`, or
   `finish-head` lag is the signal to profile that stage.
5. Stop gtron mid-catch-up, restart without deleting the datadir, and confirm
   staged bodies are recovered or pruned to a contiguous prefix: the next sample
   should not show `SyncBodies` or `SyncBodiesReady` ahead of a missing or
   mismatched body row.
   `gtron db stage-status --json --db.stage.verify --datadir <dir>` now fails
   this case by reopening the staged body rows referenced by `SyncBodies` and
   `SyncBodiesReady`.
6. After catch-up, run at least one stopped-node `--offline-db-check` sample and
   keep the JSONL row together with `gtron.err.log` and `stage-status.json`.

Gate the collected JSONL with the acceptance checker before treating a run as
full-staged-sync evidence:

```bash
scripts/dev/nile_sync_acceptance.py /Users/asuka/gtron-soak/logs/sync-samples.jsonl \
  --network nile \
  --mode full \
  --require-offline-db-check \
  --require-prune-mode-semantics \
  --require-stage-stall-evidence \
  --require-stage-detail-evidence \
  --require-startup-recovery-evidence \
  --require-archive-api-evidence \
  --require-archive-tx-evidence \
  --require-sample-prometheus-artifact \
  --min-height 100000 \
  --max-lag-blocks 5000 \
  --max-cold-stage-lag-blocks 5000 \
  --min-chain-freezer-blocks 1 \
  --min-chain-freezer-passes 1 \
  --min-sync-rate 1.0 \
  --max-datadir-bytes-per-block 500000 \
  --max-hot-bytes-per-block 120000 \
  --max-hot-growth-share 0.40 \
  --max-cold-archive-bytes-per-block 250000 \
  --max-derived-index-bytes-per-block 40000 \
  --require-snapshot-profile-evidence \
  --max snapshotSidecarShareMilli=350
```

By default the checker validates the latest selected row, requires a captured
stage-status file, accepts `catching-up` or `caught-up` staged-sync states, and
requires all six sync stages to be present and hash-verified in the
`fullStagedSync*` evidence fields. It also verifies that
`fullStagedSyncHeadLagBlocks` matches
`fullStagedSyncHeadBlock - fullStagedSyncCompleteBlock` and that
`fullStagedSyncHeadBlock` matches the sampled `height` when both are present.
It cross-checks the derived staged-sync metrics too:
`fullStagedSyncCompletionRatio` must match complete/head, pipeline lag must
cover the finish-head lag and match `stageSyncPipelineLagBlocks`, and the
reported bottleneck, bottleneck lag, and lag share must agree with the
corresponding `stageSync*` fields. When `fullStagedSyncStageDetails` is
present, the checker also cross-checks each stage detail against the aggregate
`fullStagedSync*` fields; add `--require-stage-detail-evidence` for production
soak gates so older sampler rows cannot pass without that per-stage evidence.
Use `--require-startup-recovery-evidence` when rows were collected with
`--sync-log-file`. It requires a healthy `Sync startup repair summary`: status
`ok`, at least one summary, completed sync-pipeline repair and current-head
completion, no blocked/interrupted repair, zero pipeline order/read errors, and
healthy optional order-repair/cursor checks when those subchecks ran.
Use `--require-sample-prometheus-artifact` when rows were collected with
`--prometheus-output`. The checker requires `samplePrometheusStatus=ok`, reads
the artifact, and verifies key gauges such as `gtron_nile_sync_height`,
`gtron_nile_sync_target_lag_blocks`,
`gtron_nile_sync_full_staged_sync_status`,
`gtron_nile_sync_full_staged_sync_ready`,
`gtron_nile_sync_full_staged_sync_head_lag_blocks`,
`gtron_nile_sync_full_staged_sync_stage_coverage_ratio`,
`gtron_nile_sync_full_staged_sync_verification_ratio`, the labelled
`gtron_nile_sync_full_staged_sync_bottleneck`, per-stage
`gtron_nile_sync_full_staged_sync_stage_{block,present,verified}` evidence
when `fullStagedSyncStageDetails` is present, hot/cold/index byte gauges,
`gtron_nile_sync_snapshot_sidecar_share_milli`, and archive probe failures
against the same JSONL row. If the row carries `datadir`, those gauges must
have the matching `datadir` label so an aggregate scrape file cannot satisfy
the wrong sample.
It also rejects HTTP/sample failures, critical soak health, stage regressions,
stage hash/staged-body/order issues, and non-monotonic sync-stage progress.
Use `--max-cold-stage-lag-blocks` to keep cold/archive builders close enough to
head for hot pruning and archive reads to remain useful. It requires
`stageChainFreezerHeadLagBlocks` and
`stageSnapshotEventLogBuildHeadLagBlocks` to be present, non-negative, and no
greater than the configured lag. This is separate from
`--max-lag-blocks`, which only checks full staged-sync completion.
Use `--min-chain-freezer-blocks` and `--min-chain-freezer-passes` with
`--debug-metrics-url` to prove the chain freezer has actually run, not only
that stage rows exist. These gates require `debugMetricsStatus=ok` and check
`debugMetricChainFreezerBlocks` / `debugMetricChainFreezerPasses`; keep the
thresholds low for smoke soaks and raise them for long archive/full runs.
Use `--min-sync-rate` to turn sync-speed evidence into a hard gate; it checks
the best available blocks-per-second field in this order:
`intervalBlocksPerSecond`, `intervalStageSyncFinishBlocksPerSecond`,
`soakEfficiencyBlocksPerSecond`, `syncLogBlocksPerSecond`, then
`blocksPerSecond`. Tune the threshold to the host/network baseline; keep it low
for early smoke runs and raise it for dedicated Nile soak hardware.
Use `--max-datadir-bytes-per-block` to cap total disk growth across hot Pebble,
ancients, snapshots, replay, and derived sidecars. It checks
`soakEfficiencyDatadirBytesPerBlock`, then `intervalDatadirBytesPerBlock`, then
`datadirBytesPerBlock`, so long-run samples prefer interval efficiency while
first samples can still use cumulative evidence when present. Keep this as the
outer storage budget, then use the hot/cold/index gates below to identify which
bucket is responsible when the total budget fails.
Use `--max-hot-bytes-per-block` to gate hot Pebble growth during catch-up. It
checks `soakEfficiencyHotBytesPerBlock`, then
`intervalChaindataBytesPerBlock`, then `chaindataBytesPerBlock`, so long-run
samples prefer interval efficiency while first samples can still use cumulative
evidence. Tune the byte threshold to the selected mode and expected transaction
mix; lower values are appropriate for `minimal`/`snap` runs after cold coverage
is active.
Use `--max-hot-growth-share` to require a real interval sample where hot Pebble
growth is no more than the configured fraction of positive disk growth. It
requires `soakEfficiencyWindow=interval`,
`intervalPositiveDiskGrowthBytes > 0`, and then checks
`intervalChaindataGrowthShare`. This is the hot/cold separation gate: the total
and per-bucket byte gates cap size, while this gate proves catch-up growth is
not still dominated by the hot store. Run it after at least two JSONL samples so
the interval evidence is meaningful.
Use `--max-cold-archive-bytes-per-block` to keep archive/snapshot sidecar
growth bounded while proving historical data remains available. It checks
`soakEfficiencyColdArchiveBytesPerBlock`, then
`intervalColdArchiveBytesPerBlock`, then `coldArchiveBytesPerBlock`, so long-run
archive/full samples prefer interval efficiency while first samples can still
use cumulative evidence. Tune this threshold per history mode and retained
archive window; archive mode can be higher than minimal/snap because it keeps
more historical sidecar data.
Use `--max-derived-index-bytes-per-block` to keep Erigon-style lookup/index
sidecars from growing faster than the sync/storage budget allows. It checks
`soakEfficiencyDerivedIndexBytesPerBlock`, then
`intervalDerivedIndexBytesPerBlock`, then `derivedIndexBytesPerBlock`, so long
soaks prefer interval efficiency while early samples can still pass with
cumulative evidence. Tune this separately from cold archive growth because
event-log, chain-lookup, bloom, and balance-trace indexes trade disk for query
and catch-up speed.
Use `--require-snapshot-profile-evidence` once the snapshot manifest exists to
prove the sampler profiled active payload bytes versus lookup sidecar bytes.
The gate requires `snapshotManifestProfileStatus=ok`, positive active segment
and total-byte counters, matching `snapshotPayloadBytes + snapshotSidecarBytes`
totals, a recomputable `snapshotSidecarShareMilli`, and sane per-family
`snapshot*SidecarShareMilli` fields. Pair it with `--max
snapshotSidecarShareMilli=...` or family gates such as `--max
snapshotEventLogSidecarShareMilli=...` to keep sidecar overhead within the
Nile/mainnet storage budget.
When `--allow-warning-health` is used, stage-stall warning rows can pass only
if `stageStalled*`, `stageStalls`, and `soakHealthIssues` describe the same
primary stalled stage. Add `--require-stage-stall-evidence` for production
soak gates so older sampler rows cannot pass without the `stageStalled*` and
`stageStalls` diagnostics that identify which full staged-sync edge stopped
moving. Add
`--require-caught-up` for final catch-up proof or `--all` to validate every
selected row in a candidate window. When `--require-offline-db-check` is used
and the JSONL row carries `stageAlertPipeline*` fields, the checker also
requires the referenced Prometheus artifact to include a matching
`gtron_storage_alert_status` sample and, when present, matching
`gtron_storage_stage_pipeline_*` values, next-target/current cursors, and
stage/status/upstream labels from the same offline DB check. If the row carries
prune-boundary evidence, the same artifact must also match
`gtron_storage_signed_cold_prune` and
`gtron_storage_prune_boundary_block{field=...}` for those JSONL fields. When
the row contains `datadir`, those Prometheus samples must carry the same
`datadir` label, so aggregated artifacts cannot satisfy a Nile row with metrics
from another node. When `--require-archive-api-evidence` is used, the latest
selected row must report `archiveApiStatus=ok`, zero archive probe failures, a
historical `archiveApiBlock` below `height`, and the default archive method set.
When `--require-prune-mode-semantics` is used, the latest selected row must
carry a persisted `pruneMode` matching the sampled `mode`; it also rejects
archive/non-minimal rows that report incompatible prune or tail-prune progress.
When storage-alerts exports prune boundary fields, the same gate also requires
`signedColdPrune` rows to carry `chainLookupPruneToBlock >= 0` and
`coldFreezerToBlock` covering that chain-lookup prune boundary; minimal tail
prune rows must keep `tailPrunedThroughBlock` covered by both boundaries.
Use `--require-archive-tx-evidence` for production archive proof after selecting
an `--archive-api-block` with at least one transaction; it requires the sampler
to report same-row archive API evidence, `archiveApiTxProbe=true`, a
`0x`-prefixed 32-byte `archiveApiTxHash`, and successful
`eth_getTransactionByHash` plus `eth_getTransactionReceipt` probes.
Add `--archive-api-method eth_call` to the acceptance command only for samples
that were collected with `--archive-api-call-data`.

If any stage row is missing, unbound, ahead of canonical head, or hash-mismatched,
the restart path should repair it by keeping only a contiguous hash-bound prefix.
Treat repeated repairs after a clean restart as a bug, not an expected steady
state.

When the node is stopped, add `--offline-db-check` to also run
`gtron db storage-alerts --json --datadir <dir>` and include
the overall `storageAlertStatus` plus freezer/stage/mode/snapshot alert fields
in the row. With `--require-offline-db-check`, acceptance requires those
overall and component statuses to be `ok` and their issue counts to be zero, so
storage-alert warnings cannot pass just because the command exited cleanly. The
row keeps both aggregate counts and detail arrays:
`freezerAlertDetails`, `stageVerifyDetails`, `modeAlertDetails`, and
`snapshotAlertDetails`. It also carries prune-boundary evidence from stage rows:
`signedColdPrune`, `coldFreezerToBlock`, `chainLookupPruneToBlock`,
`tailPrunedThroughBlock`, `balanceTracePruneToBlock`, and
`sectionBloomPruneToSection`. `modeAlertDetails` flags persisted prune-mode
conflicts such as `archive` datadirs with hot-prune or tail-prune progress. Do
not enable that flag against a live Pebble datadir unless the DB can be opened
by the diagnostic command. Captured `stage-status` files also populate
`stageStagedBodyIssueRows` and `stageStagedBodyIssueDetails` when downloader
body progress rows fail staged-body verification, including `stagedBlock` and
`stagedHash` when the referenced staged row can be decoded. Structured
stage-status issue details additionally raise `stage-status-issue`, and
structured order details raise `stage-order-issue` in `soakHealthIssues`.
For monitor scrape jobs outside the JSONL sampler, use
`gtron db storage-alerts --prometheus --datadir <dir>` while the node is stopped
or the database can otherwise be opened exclusively. The text metrics expose
overall/component status values (`0=ok`, `1=warning`, `2=critical`), issue
counts, normalized issue-kind counts by component/severity/kind, hidden freezer
bytes, retired snapshot counters, the persisted prune mode,
`gtron_storage_signed_cold_prune`,
`gtron_storage_prune_boundary_block{field=...}`, and
`gtron_storage_stage_pipeline_*` gauges for the same canonical/post-`Finish`
pipeline cursor exposed by `stage-status`. Offline JSON rows carry the same
cursor as `stageAlertPipeline*` fields, so the sampler can distinguish storage
maintenance backlog from actual storage-alert failures. When
`--offline-db-check` is used with `--output`, the sampler also writes
`${output}.storage-alerts.prom` by default and records
`offlineDbCheckPrometheus` plus `offlineDbCheckPrometheusStatus` in the JSONL
row. Use `--storage-alert-prometheus-file <file>` to place that artifact at a
stable path for external scrape jobs.
To route the latest JSONL-referenced artifacts into a node_exporter textfile
collector or another file-based scrape job, export them atomically into one
combined file:

```bash
scripts/dev/prometheus_artifact_export.py \
  /Users/asuka/gtron-soak/logs/sync-samples.jsonl \
  --output /Users/asuka/gtron-soak/logs/gtron-nile.prom \
  --require-field samplePrometheus
```

The exporter reads the latest selected row, resolves `samplePrometheus`,
`offlineDbCheckPrometheus`, and `storageAlertPrometheus` relative to the JSONL
file, then writes one combined Prometheus text artifact. Add `--all-rows` only
for debugging; production scrape jobs should usually export the latest row per
JSONL file.
The acceptance checker compares `storageAlertStatus` with
`gtron_storage_alert_status` (`0=ok`, `1=warning`, `2=critical`) when both are
present, so a scrape artifact with the right labels but stale status value is
rejected.

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
