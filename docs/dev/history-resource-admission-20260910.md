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

## Next cache investigation

Depth-7 flush-only promotions are not sufficient evidence to change cache
policy: a promotion changes eviction ordering without itself increasing used
bytes. A bounded local reuse-observation and complete shared-cache replay design
is recorded under `build/benchmarks/20260910-cache-reuse-design/design.md`.
Production cache parameters remain unchanged in this release.
