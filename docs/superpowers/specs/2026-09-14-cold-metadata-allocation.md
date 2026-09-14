# Cold manifest validation and trusted-reference allocation

## Evidence and objective

The stable `8a0681d3` process was sampled on 2026-09-14 at UTC 05:16:48
for 45.16 seconds (135.58 CPU seconds). Its companion coverage gate used
0.40 CPU seconds, with `findManifestRef` at 0.02 seconds, so the preceding
lookup optimization still holds after cache warmup. `Manifest.Validate`
accounted for 2.66 cumulative CPU seconds and `RecordTrustedSnapshotSegments`
for 0.57 seconds. Nested cumulative values must not be added.

Reduce temporary metadata copying and allocation in these existing operations.
Do not skip validation, change publication frequency, or introduce a trusted
publication bypass. This is an incremental CPU/allocation optimization, not a
new disk reclamation mechanism or a promise that import/cold lag will shrink.

## Implementation

`Manifest.Validate` retains its complete active-reference validation and path
map. Its per-family overlap sort stores only the two transaction bounds used
by that check instead of full `SegmentRef` values. Companion and production
validators iterate by index and check the kind before copying a reference or
looking up its dataset configuration. Dataset configuration lookup is pure;
all applicable validation and error ordering remain intact.

`RecordTrustedSnapshotSegments` still loads the production manifest first for
every nonempty input. For a small batch it maps only eligible history input
references, then scans the complete active catalog to establish exact full
`SegmentRef` membership. When the eligible input count exceeds the active
catalog count, it retains the old full-catalog map strategy. The original
input loop still performs every fresh file identity lookup and cache update,
including duplicates and the successful prefix before the first failure.

## Invariants

- Wire/consensus rules and all on-disk formats are unchanged.
- Active and retired checks, binary companion checks and production checks
  remain enabled, including the public publish and load validation paths.
- Publication bytes, generation, fsync sequence and validated-cache seeding
  remain unchanged. No caller receives a private validation bypass.
- Full reference identity, Stat behavior, checksum field, verification-cache
  persistence, duplicate processing and error side effects are preserved.
- History retention, shared format v3, range pruning, GC and their permanent
  reader guards remain in force. No memory/cache/concurrency budget changes.

## Validation and rollout

Use frozen predecessor function oracles for complete validation/publication
and complete trusted recording, including malformed and boundary inputs,
duplicates, inactive/mismatched references, manifest/file/persistence errors.
Compare encoded manifest/cache outputs and input immutability. Run package
tests and relevant race checks with Go 1.25.5.

Benchmarks compare frozen predecessor and candidate on the same mixed catalog
(70,198 active and 92,667 retired rows), then optionally replay a SHA-pinned
real production manifest. Include complete Validate, Publish plus first Load,
and trusted-recording operations; isolate concurrent benchmark CPU. Report
bytes allocated separately from allocation counts and disclose publication
buffer variability. Microbenchmarks do not prove end-to-end import speedup.

Build the frozen GitHub source on the native server in an isolated release,
using the existing attested upgrade transaction. The immediate compatible
recovery target is `8a0681d3`, writer on or off; retain its permanent reader
marker and the D656, 8058 and af989 ancestors. Validate queue metrics on the
same exact process during candidate/recovery health checks. After activation,
collect a separate complete metrics window, CPU profile, physical `du` and
fixed-height historical balance canary. Never subtract counters across a
restart or equate logical retirement/encoding savings with physical release.
