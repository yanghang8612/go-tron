# Busy history resource admission — 2026-09-10

This follow-up develops directly on `master`, as requested. The previously
validated metadata release and its report were fast-forwarded to master at
`5c7baf255dcccddb9caea664854c2b4eacad90e0` and pushed normally to GitHub.

## Scope

The state archive scheduler gains an optional throughput-mode admission between
its soft and busy debt watermarks. It requires the existing fresh CPU/cgroup/
memory readiness probe, fresh healthy storage, and the existing shared heavy
work gate. Importer readiness remains false, and these batches retain bounded
work and complete recovery accounting. No archive bytes, canonical checks,
history retention policy, ports or cache budget are intentionally changed.

New cumulative gauges `state/snapshot/cold/history/admission/busy_resource/`
`attempts` and `builds` distinguish actual work admitted by resource headroom
from work forced by the larger busy watermark. A resource decision alone is
not an acquired lease or a completed build.

## Baseline and validation

The old deployment was rechecked at 08:30 UTC: PID 22527, start ticks 4476169123,
binary `/data/gtron/releases/20260910-history-metadata/gtron`, SHA-256
`134ea2b22c83a0f0ea5d2d77e1784ead27c556a0fa9f97e2c2c9880c9a19ba87`.
The mainnet space guard passed and listeners matched the stable port layout.
Server checkout HEAD remained `19eda11f44424f673a40b06051edcbf629b50846`;
the running binary's source is `500b61273cbf0d175624cb1ff114b39ba409a37a`.
These are separate identities because release builds use isolated Git archives.

Raw captures and local analyses are under
`build/benchmarks/20260910-history-admission/`. Failed metric requests are
retained and excluded from numerical deltas; exact process-start integers
prevent comparisons across restarts. Wallet throughput has its own request
window and transaction-density measurements.

The 08:27:38–08:32:41 UTC baseline contains 19 valid metrics responses out of
21 (requests 004 and 014 timed out) and 21 successful Wallet responses, all on
process identity `1789027197720109146`. Over 303.186 seconds, head advanced
5,028 blocks (16.584/s) and published/pruned state advanced 5,473 (18.052/s),
so head-to-state distance decreased by 445 blocks. The separate 299.987-second
Wallet window measured 1,364.094 transactions/s and 81.451 transactions/block.
Process CPU averaged 3.100 cores. Eight forced-busy builds completed; importer
admission remained busy, five passes deferred for sync and four for recovery.
The old version does not expose the new resource counters; they are missing,
not zero. Unlike the previous metadata report's earlier sample, this window
already shows slight net debt reduction, so it must not be described as
continuously growing backlog.

The fixed historical balance canary passed before deployment at 08:34:55 UTC
(12 read-only requests, blocks 2043–2045). Its scope is one account's scalar
balance and fixed block hashes, not a whole-database audit.

Go 1.25.5 full-suite validation passed: 54 tested packages, 13 packages without
tests (`CGO_ENABLED=0`, two workers; core 157.460 s, snapshots 63.588 s).
Uncapped incremental lint against `5c7baf25` reported 0 new issues. Targeted
race checks passed for the new admission and existing runtime-resource probes.
Three identical-input real-Pebble cases verified matching segment references,
checksums, file bytes, history/event queries and contiguous coverage, including
complete-block rounding above a transaction target. Restoring only the old
unconditional busy deferral through a local Go overlay made the two new
positive admission cases fail as expected. Independent review found no
blocking issue. Local evidence is in
`build/benchmarks/20260910-busy-history-resource-admission/validation/`.

## Pinned deployment

Source commit: `0ab5b6d14c5a647fe381377d51a1c521d3a53879`.
Deployment-script commit on master: `882da7ed75536afde57a3345bb4d6aa7be024f97`.
Script SHA-256: `68dc992400c4690a491bb606dc0e2b37756a6ee8cff10b3c4124545c5979df27`.
Release directory: `/data/gtron/releases/20260910-history-admission`.
The script separately pins the server checkout, running source, candidate and
fetched master tip, and verifies all six changed non-document source files
against committed hashes. Existing holds, disk guards, unrelated units and
service settings are retained; activation changes only the executable path.

Native Go 1.25.5 Linux/amd64, CGO and Sapling validation completed at 08:41:09
UTC, including all snapshots tests and the native Sapling availability probe.
The built binary SHA-256 is
`ba071c728499e7ca02733deca765fc717290b1b76374157f7a6d92d1a3125deb`.
Activation succeeded at 08:42:08 UTC (16:42:08 China time): PID 32233,
start ticks 4476422233, exact metrics process identity
`1789029728814002906`. The activation health check observed head advance from
22,714,112 to 22,714,120. A separate startup sample at 08:43 UTC already
recorded three successful resource-admitted builds below the old busy
watermark; startup is excluded from the stable comparison.

## Stable post-deployment result

The 08:44:24–08:54:23 UTC metrics window spans 599.715 seconds: 35 valid
responses from 41 requests, with 006/014/025/026/029/032 transport timeouts
retained and excluded. All 41 Wallet requests succeeded. Every usable sample
has the expected new process identity. This window follows about two minutes
of startup warmup; counters are differenced only within each process.

| Whole-window measurement | Before | After |
|---|---:|---:|
| Head blocks/s | 16.584 | 34.743 |
| Wallet transactions/s | 1,364.094 | 4,036.172 |
| Transactions/block | 81.451 | 116.152 |
| State published blocks/s | 18.052 | 48.553 |
| State pruned blocks/s | 18.052 | 45.560 |
| Mean process CPU cores | 3.100 | 3.378 |
| Head-to-published distance change | -445 | -8,282 |

The new opportunity completed 16 attempts and 16 publications, all while
importer admission stayed busy; accelerated builds remained zero. These are
subsets of the 16 forced-busy attempts/builds, not additional work to sum.
Twenty-five resource deferrals and sixteen rate-limit deferrals demonstrate
that the shared gate and recovery still apply. Body and transaction-index
frontiers each advanced 65,536 blocks, ending together at 22,151,168.

The whole-window improvement is not sustained net catchup: observed state
publication and resource-build counters were unchanged from 08:47:08 to
08:53:23, approximately 375 seconds. Storage budget fell to level 1 after
compaction-debt growth, then remained in recovery/hysteresis while runtime
CPU/memory readiness stayed true. At 08:53:37 the healthy storage tier was
observed again; six further resource-admitted builds published 9,798 blocks.
The later-half window nevertheless has head 38.004 blocks/s versus state
34.411 blocks/s, increasing the distance by 1,023 blocks. This release removes
some importer-only idle periods; it does not establish continuous backlog
elimination. The storage budget's recovery observation cadence is the next
specific scheduling question to test.

At the final sample, head was 22,737,034 and state published/pruned was
22,177,672: distance 559,362 blocks, or 493,826 beyond the normal 65,536 window.
Body/index distance was 585,866, or 520,330 beyond that window. These debts
overlap and must not be added. Wallet still reported 63,382,408 blocks to the
network target, 28 peers / 8 sync peers, 2,478 buffered blocks and no fetch
backpressure. Thus substantial full-chain synchronization and archive work
remain. Publication/pruning metrics are not an atomic snapshot: the first
after response observed pruning 1,795 blocks ahead of its publication gauge;
the final gauges agree. This explains the different full-window publication
and pruning rates and is not by itself evidence of invalid pruning.

Observed cold-builder, strict-code and code-cache hash-rejection counters
stayed at zero. Shadow sender errors increased by 5; VM-shadow errors by 217
(199 readiness, 15 unsupported apply, 3 result). These nested counters are
reported separately and are not claims of canonical-chain failures or a zero
error workload. VM workloads also changed: contained import-window median
raw energy per VM transaction fell from 100,264 to 25,954, while VM share rose
from 80.39% to 88.25%. Different blocks, VM intensity, cache warmth and archive
phases prevent attributing the head/TPS rate changes to this patch.

At 08:55:16 UTC, process identity and binary checksum still matched; the disk
guard passed, gtron and its disk-guard timer were active, and Nile/automatic
deployment units remained inactive. Existing server checkout modifications
were unchanged. The fixed historical balance canary passed again at 08:56:02
UTC with the same block hashes and balances (12 read-only requests).

At 09:00:06 UTC, three blocks imported after activation (22,714,200,
22,714,600 and 22,718,000; respectively 111/112/112 transactions) matched
TRONGrid mainnet on block ID, parent hash and ordered transaction IDs. The
gateway Java node returned an empty object for the first requested historical
block, so it could not serve as this comparison's reference; its observed tip
was 86,119,531. The final full Wallet response also timed out and was retained
but excluded. A smaller `eth_getBlockByNumber(false)` response completed that
comparison successfully. Evidence is in
`canonical-after-trongrid/summary.json` beneath this round's local directory.
This samples three canonical blocks, not every block or all state values.

Full comparison and per-point evidence are retained locally in
`build/benchmarks/20260910-history-admission/comparison.{md,json}` and
`after-analysis.json`. No sustained-throughput claim should be extrapolated
from this finite, bursty sample.

## Next cache investigation

Depth-7 flush-only promotions are not sufficient evidence to change cache
policy: a promotion changes eviction ordering without itself increasing used
bytes. A bounded local reuse-observation and complete shared-cache replay design
is recorded under `build/benchmarks/20260910-cache-reuse-design/design.md`.
Production cache parameters remain unchanged in this release.

An independent scheduling review identified another bounded follow-up: when
the new resource opportunity is temporarily rejected, the current branch has
no retry hint and can return to the one-minute lifecycle tick. The five-second
resource sampler does not itself advance the history budget's accepted samples.
In the baseline, accepted sequence increased by 11 over 303.186 seconds, with
two observed intervals near 60 seconds; median/max accepted-sample age was
23.086/57.903 seconds. Runtime readiness was true at all 19 observations,
while 18 budget observations remained at level 1. These are observation counts,
not time shares, and retained budget age does not prove the live probe stale.

A future experiment should compare identical pressure traces consumed at five
and sixty seconds. An independent, bounded observation check could refresh the
budget without invoking manifest/catalog reads, pruning, freezer work or lease
acquisition. It would wake the normal pass only after resources are ready and
the latest existing recovery/failure deadline has expired. Pending completion,
later deadline extension, stale samples, cancellation and merged wakeups need
deterministic coverage. This is a proposal; this release does not change those
timers or pressure thresholds. More frequent observations can detect debt
growth earlier as well as recover capacity earlier.
