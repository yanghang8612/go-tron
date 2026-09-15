# History backlog admission at completed sync-session boundaries

2026-09-15. Implementation specification; enabling production throttling and
choosing production watermarks remain an explicit release decision. Reader
memoization is an independent optimization and must not be presented as a
replacement for capacity control.

## Evidence and objective

The completed 13-point 2026-09-15 baseline contains 9,184 imported blocks in
359.995 Wallet seconds (25.511 blocks/s). Cold coverage advanced 518 blocks in
360.094 metrics seconds (1.439 blocks/s). Eligible lag grew from 970,517 to
979,301 (+8,784, or 24.394 blocks/s). Head minus cold coverage is a different
quantity: 1,036,249 to 1,044,927. These samples use one unchanged process.

Separate overnight endpoints, 2026-09-14 10:40:29 UTC to 2026-09-15 01:58:22
UTC, show +539,040 imports, +197,585 cold blocks and +341,459 eligible backlog.
These two endpoints do not establish intermediate pressure or a constant rate.
Evidence is under `build/benchmarks/20260915-cold-backlog/`.

Prevent an explicitly configured downloader from admitting unbounded new
history debt. Preserve normal canonical execution, archival history, source
authentication, cold publication and prune permissions. This necessarily can
reduce network catch-up speed; no automatic production enablement or arbitrary
fixed import rate belongs to the implementation.

## Scope and interface

`net.HistoryBacklogAdmissionConfig` contains `Enabled`, `HighBlocks`,
`LowBlocks`, and `Probe func(context.Context) (HistoryBacklogSample, error)`.
The sample contains `ObservedAt`, `HeadBlock`, `EligibleBlock`, and
`PublishedBlock`. `SyncService.ConfigureHistoryBacklogAdmission` validates and
installs configuration before `Start`. Disabled is the default and does not
invoke the probe or start another worker. Enabled requires a probe and
`0 < LowBlocks < HighBlocks`. Changes while running are rejected.

The command layer owns explicit flags beside history configuration:
`--history.backlog-admission`, `--history.backlog-high-blocks`, and
`--history.backlog-low-blocks`. They have no environment-variable aliases, so
an inherited deployment environment cannot silently enable the new gate.
Watermarks have no silently selected enabled defaults.
Reject enablement without the operational cold history builder (snap/archive
history enabled); validate flags before startup. Register beside the existing
history catch-up flags and wire after the SyncService is constructed.

Also require `LowBlocks >= maxBusyDeferredColdHistoryBlocks(
maxDeferredColdHistoryBlocks(historyWindow))`. With the current 65,536-block
window this floor is 524,288 blocks. Above it, existing forced cold builds do
not depend on a supply-idle importer or a soft/busy resource opportunity. No
SyncBuildReady, pressure, lease, or cold defer behavior is changed. Without
this floor, import could be held while cold work legitimately defers forever.

Core supplies `SampleHistoryBacklog(ctx, historyWindow)`. It uses a nonblocking
canonical-lock attempt and verified durable stage rows:

* `Eligible = min(verified durable Finish, max(solidified - window, 0))`.
* `Published = verified durable SnapshotBuild`.
* `Lag = max(Eligible - Published, 0)`.

SnapshotBuild is written after successful immutable cold publication. Reading
it is a scheduling observation, **not** independent proof of current manifest
contents or permission to delete data. The sampler must not load the full
manifest on every probe, repair missing stages, flush, or take a large read
snapshot. Missing SnapshotBuild conservatively means Published=0; it cannot
invent cold coverage. Missing Finish is accepted only for an explicitly
verified fresh genesis. Mismatched/corrupt rows are errors. The normal lifecycle reconciles
publication/stage crash gaps. Parent core/cmd implementation owns canonical
hash verification and the conversion into the net sample.

The admission threshold uses eligible lag only. `HeadBlock - PublishedBlock`
is separately reported as head gap. Neither network distance nor downloader
buffer size substitutes for history debt. This policy limits cold-publication
backlog; physical disk growth additionally depends on pruning, derived indexes,
compaction, other data and bounded in-flight work. It is not a hard byte quota.

## Admission and hysteresis

Check at the top of `drainBufferedBlocksOnce`, before any
`BeginSyncInsertSession`, selection/decoding, or signature lookahead. Once
admitted, keep the existing session flow and 4,096-block rotation. Every return
still performs its existing `Finish`, resume-stage publication and ownership
release. Never wait within `ExecuteImportBatch`, between Insert calls on a live
executor, or while holding chainmu, insertSessionGate, ss.mu, drainMu, a history
view, or a maintenance lease.

With a valid new sample, enter hold at `Lag >= HighBlocks`. A held admission
releases only at `Lag <= LowBlocks`; the middle interval preserves its state.
Unknown, zero timestamp, stale/future, inconsistent heights, callback failure,
or cancellation cannot become a fabricated zero-lag admission. Enabled gates
fail closed with a separately visible reason/error. A sample received after
reset/Stop is discarded using an admission generation.

A probe error preserves the previous high-water latch rather than inventing
one. Once a valid sample returns, a previously unlatched service uses the high
threshold normally; a service already latched at high still needs the low mark.

While held, incoming drain triggers return promptly without acquiring a new
session or creating a sleeping goroutine. A single owned poller rechecks at a
fixed one-second cadence, skipping any still-active drain after a reset.
Concurrent checks coalesce; repeated inbound blocks
must not multiply the probe rate. A released poll schedules the existing
coalesced drain path; that next session checks a fresh sample again. All state
and callback ownership are serialized without calling the probe under the
admission mutex. One context deadline per probe requests cancellation after a
second; it is cooperative cancellation, **not** a hard bound on Pebble's current
Get. Do not wrap an uncancellable call in a disposable goroutine.

The stop signal prevents new admission and quiesces the session before joining
the poller, which may itself own a drain. Quiescing cancels the current probe;
the worker is canceled and joined before Stop returns. Start/Stop and duplicate Stop are safe;
configuration is immutable while running. A held service remains actively
syncing and is not a sticky import-error pause. Cold history, freezer, derived
history-index and prune workers retain their normal wakes, retries and budgets.

## Watchdog, reset, reorg and bounded overshoot

`StallRecoveryBlocked` includes an intentional active backlog hold. This keeps
the watchdog from treating a deliberate stationary head as failed peers or
recovering it around the admission. The gate does not change LastInsertTime,
fabricate progress, set IsPaused or report syncing as false. Once released,
normal watchdog recovery is available again.

Session deactivation/reset invalidates any in-flight probe and wake generation,
while preserving an already observed high-water latch until a fresh sample
reaches the low watermark. A peer change cannot bypass the hysteresis band.
It does not overwrite permanent stage rows or globally disable admission. A
new session must make its own fresh canonical observation. Probe results from
an old peer/session must never release a new hold. Stop, stop-height and sticky
execution errors take precedence. Existing reorg execution/rewind remains
unchanged; after a boundary, re-sample the current canonical stage hashes.
Canonical mismatch fails closed, and decreasing heights are not interpreted
as unsigned wraparound. Core validates real reorg/proof behavior; net tests
also replay regressed/replaced samples and reset races.

This minimal version does not interrupt a session in mid-chunk. Crossing the
high watermark may therefore admit the remaining existing session: at most
the 4,096 rotation threshold plus one configured import chunk, with the
existing asynchronous commitment/in-flight frontier difference separately
accounted. Configuration does not change either bound. An already published
eligible frontier can advance in a solidification jump; the high mark is an
admission trigger, not a promise that a sampled gauge can never exceed it.

## Public status and operational policy

Expose `SyncStatus.HistoryBacklog` with enabled/holding, high/low marks, reason,
head/eligible/published/lag/head-gap, observed/checked/since timestamps, checks,
probe errors and hold transitions. Unknown samples retain the last valid
heights only as explicitly dated information and expose their failure reason;
they never masquerade as a fresh valid sample. Log hold/release/reason changes,
not one unchanged message per poll. API and metrics wiring belongs to root.
Eight `sync/history_backlog/` gauges expose enabled, holding, lag, high, low,
head_gap, probe_errors and checks; disabled mode has an explicit zero schema.
Gauge reads do not invoke probes. Heights saturate only in signed metrics;
the actual admission comparison and status preserve full uint64 values.

Select high/low values using current eligible lag, durable cold service rate,
actual native hot/free bytes, safety reserve, restart/solidification slack and
the maximum admitted-session burst. The high-low band must exceed expected
probe and in-flight jitter and provide a useful recovery interval. Do not
infer bytes per pending block from CDC logical bytes or a single du interval.
The release review must state the concrete values, predicted backlog envelope
and catch-up tradeoff. Enabled unknown/failed probes remain held and observable;
there is no time-based fail-open escape that silently invalidates the cap.

## Validation and minimal files

Net changes are limited to `net/sync.go`, new
`net/sync_history_backlog.go`, and focused tests. Core adds a focused sampler
and its tests; cmd adds a focused flag/wiring helper plus startup/status
integration in main. No formats, schemas, consensus or reader logic changes.

Tests cover default-off zero-probe behavior; invalid config; exact high/low
boundaries and hysteresis; uint64 boundaries; repeated/unknown/stale/future
samples; probe errors/cancellation; no callback under admission or sync locks;
single polling worker under concurrent drain triggers; old-generation results
after reset; Start/Stop/restart and Stop during a cooperative blocked probe;
real staged-buffer preservation with an active service; no InsertSession while
held; correct resumed canonical batch/async Finish behavior; watchdog
suppression and restoration; stop-height/sticky pause priority; and regressed
canonical samples. Core tests verify durable vs buffered stages, lock busy,
missing/corrupt/hash-mismatched rows and actual canonical/reorg handling.
Run focused tests and race before broader package checks, using Go 1.25.5 and
coordinated CPU windows. Public status must make an intentional hold distinct
from network starvation or canonical failure.
