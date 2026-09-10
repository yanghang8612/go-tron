# Busy history resource admission

## Evidence and objective

The September 10 metadata release preserved correctness and removed repeated
manifest decode work, but its stable online sample still accumulated history
debt. Continuous importer progress kept `SyncBuildReady` false. The state cold
builder waited below the 524,288-block busy watermark even while the independent
runtime resource probe reported readiness. Above that watermark, existing
bounded busy batches resumed. The observed long recovery used a dynamic 20%
budget selected after compaction-debt growth; it was not an extra fixed 20% cap.

Allow throughput-mode state history to make bounded progress between the soft
and busy watermarks when fresh storage and runtime observations permit it.
Keep importer readiness and resource readiness distinct.

## Design

- Add optional `Config.BusyHistoryBuildReady`, wired to the existing runtime
  CPU/cgroup/memory probe. A nil callback preserves existing admission.
- Require deep sync, debt strictly above the soft watermark and at or below the
  busy watermark, throughput mode, the shared heavy-work gate, a load probe,
  and fresh healthy engine/device observations satisfying the existing CPU
  burst storage predicate. All conditions must hold for the new opportunity.
- Use the existing bounded forced-busy scheduling/accounting path. Keep
  `HistoryAdmissionReady` and `HistoryAccelerated` false; separately report
  `HistoryBusyResourceReady` and actual resource-admitted attempts/builds.
- Preserve disk reservation, hard engine limits, lease exclusion/cooldown,
  density and configured batch caps, complete-block publication, dynamic duty,
  absolute retry deadlines and full outer-maintenance completion accounting.
  Existing above-busy liveness and balanced-mode behavior remain in effect.
- Do not change archive formats, queries, pruning coverage, canonical checks,
  cache size, or hot-state read algorithms.

## Validation and rollout

Use deterministic canonical input to compare archive bytes and query results
between idle and resource-admitted runs. Exercise watermark equalities, unknown
or stale probes, resource pressure, hard limits, lease denial, retry deadlines,
failed or pending maintenance, and complete-block density limits. Run relevant
race tests, the Go suite, and uncapped incremental lint.

Develop and push directly on `master` as requested. Build a pinned Git archive
on the deployment host using Go 1.25.5, CGO and Sapling, retaining the current
binary as rollback. Verify process identity, guards and native tests before
activation. Collect process-consistent baseline and post-deployment samples,
reporting failed requests explicitly and separating head throughput, cold
publication throughput, transaction density and outstanding debt.
