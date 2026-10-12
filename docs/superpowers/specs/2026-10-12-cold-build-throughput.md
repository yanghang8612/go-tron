# Cold build throughput: cache pairing and chain event replay

The cache-only history topology (selected shared read workers zero, reference
container false) may join the independent V4 event builder. The existing common
maintenance lease, syncing throughput mode, deferred derived work, CPU pool and
fresh CPU/storage/device admission remain required. Shared worker pools and the
reference-container codec topology remain serial. History retains its acquired
read snapshot through both builders' join; cancellation or either failure stops
manifest/stage publication. Immutable file orphans retain the existing safe
content-addressed behavior.

Only FromChain builds acquire a hot key/value snapshot and replay verified event
rows after the dictionary pass. A shallow ChainDB copy preserves ancient fallback
and private cold sidecar routing for external receipt logs. This pins only hot
KV, not a cross-store atomic snapshot. Caller-selected solid ranges, immutable
ancient canonical rows and leased immutable sidecars retain their existing
contracts, as with ChainDBReadView. Unsupported snapshot sources retain the original two-pass identity-authenticated reader; other snapshot
factory errors fail. Generic FromReader behavior is unchanged, including second
pass source changes and errors.

The replay owns protobufs with an 8 MiB charged memory budget: each row consumes
its protobuf encoded size, 512 bytes of fixed row/allocation allowance, and 48
bytes per topic. This bounds small and empty row cardinality too. Exactly at the
budget, the cache remains usable. Exceeding it releases all retained rows and
continues the dictionary scan without caching. The second pass then uses the
same pinned source and original bounded 32 MiB identity cache, retaining existing
validation and second-pass failure behavior. There is no temporary spool or new
encoding. Snapshot close errors return no valid segment reference and prevent
parent manifest/stage publication, even if immutable files have already been
written. Published V4 bytes remain unchanged. Large ranges still read twice;
this deliberately avoids the measured CPU/allocation cost of spool replay.

Debt-only soft pressure keeps the existing bounded seven accepted observations,
30 second recent mature suffix, positive adjacent-step evidence, freshness/reset
rules and 2 GiB debt floor. Its recent net growth must now be at least
`max(256 MiB, floor(recent baseline debt / 100))`. Extending a suffix to establish
positive-step evidence cannot amplify a sub-threshold recent net. A mature
sub-threshold trend may support the existing independent two/three good-sample
recovery. Significant growth keeps the original 20 percent duty; hard stalls,
new stalls, L0 pressure, and fresh high-latency device pressure remain independent
immediate protections. These thresholds are scheduling policy, not an inference
of spare device bandwidth. Small sustained growth below the bounded-window floor
may remain unthrottled by debt alone; hard/L0/stall/device guards still apply.

Verification compares complete V4 bytes, transaction/block/log identity and
unknown protobuf fields, exact memory boundaries and overflow fallback, snapshot
mutation isolation and close failure, first/second source failures, concurrent join/publication boundaries,
cache-only pairing, exact absolute/relative debt thresholds and recovery with
independent safeguards. The fixed input benchmark is copied unchanged to the
old HEAD for native before/after measurement; local timings do not predict live
sync throughput. Results belong under
`build/benchmarks/20261012-cold-build-optimization`.
