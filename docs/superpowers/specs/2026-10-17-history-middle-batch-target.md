# Resource-admitted middle-debt history batches

## Objective and evidence

The existing middle-debt tier deliberately keeps level-1 batch targets at
2 seconds/32 MiB while permitting 60% duty under fresh healthy device/L0
observations. Retain that conservative default. Introduce an additional
Runner-only opportunity to use the existing recovering tier's 4 seconds/64 MiB
range target when the node independently admits current CPU/runtime/memory
headroom. This is a policy adjustment, not correction of a behavior accidentally
omitted from the original tier: the earlier tier test explicitly forbids larger
pure-state batches and remains valid.

In the observed 50-point production baseline, the last completed history sample
reported median density work 2.397 seconds and separately excluded metadata
2.481 seconds. The 18 observations at 60% duty reported 2.375/2.394 seconds.
These are reused completed-job observations, not 50 independent timings. The
roughly equal row and metadata costs suggest amortizing repeated manifest work;
they do not imply near-zero row work. A larger scheduling budget alone proves
no throughput or I/O improvement.

Production signing was independently checked absent from both the actual argv
and exact signing-key environment variable. Production also explicitly disables
the history reference container. The primary controlled workload therefore uses
ordinary compressed history, event V4, no reference container and no catalog
signing. Existing production `manifest.json` was 48,892,678 bytes; the fixture's
smaller genuine catalog can establish repeated-write mechanics, not predict
production parsing time, resident memory or mainnet sync throughput.

## Decision contract

Keep `historyLoadState.targets()` and `batchLimit()` unchanged in meaning.
The Runner may select 4 seconds/64 MiB only when all of these hold at its range
decision: the existing `debtModerateReady` predicate, a nonnil shared heavy-work
gate, and the existing nonnil `BusyHistoryBuildReady` callback returning true.
Recheck `debtModerateReady(time.Now())` after the callback: a slow observation
must not preserve evidence that expired while it ran. Block and txnum limits
share one target/resource observation. Independent event range selection applies
the same guard separately.

The predicate retains fresh engine/device timestamps, low nonnegative device
await at most 2 ms, known healthy L0, absence of hard pressure, and the existing
non-severe debt tier. It also permits the already admitted first healthy recovery
carryover. Storage resets, stalls, severe growth and unknown/stale evidence follow the
existing rules for revoking and reacquiring storage admission; reusing a probe
cannot restore revoked storage eligibility. Missing/false resource evidence
conservatively affects the current decision; a later decision requires its own
current callback returning true.
Missing/denied resource evidence does not block original liveness work.

Preserve configured limits, immediate density shrink, at most 25% growth with
the existing tiny-integer one-unit exception, unsuccessful-attempt caps, normal
history/event admission and all complete-maintenance recovery accounting.
The 60% duty target, recovery deadlines and engine/device guards are unchanged.
No storage format, pruning proof, durability or manifest publication protocol
changes.

`BusyHistoryBuildReady` reports cached CPU/runtime/memory opportunity. It is not
a per-job allocation reservation or RSS limit. Low shared-read resources can
select serial execution rather than reject construction, and not every liveness
path previously consulted the busy-resource callback. The new callback check
specifically protects the additional burst; a larger batch can still increase
peak memory, individual shared-lease hold time and the wait imposed on other
maintenance. Range selection precedes later source work and lease acquisition,
so this is evidence at the decision point, not a promise of memory at the first
allocation. Existing later hard admission remains in force.

The counters `history/budget/middle/history/selected` and
`history/budget/middle/event/selected` count adaptive range decisions given the
larger middle target. A block/txnum pair counts once. A subsequent deadline,
lease, source error or cancellation can prevent work; these counters do not
count successful builds, allocated memory or completed publications. The
initial configured/4 rule and growth/configuration/failure caps can keep the
actual range unchanged even when a decision selects the middle target. Event
planning can also select a target before finding that no actual event gap exists.

## Validation and selection

The ignored evidence and source overlays are under
`build/benchmarks/20261017-compaction-io-followup/cold-target/`. The production change is confined to `history_load.go`; test-only write
instrumentation is excluded. Full validation and final source audit gate release.

Deterministic tests cover current/expired/future and unknown evidence, resource
loss, nil shared gate, reset, stall, severe growth, recovery carryover, a callback
outliving freshness, one resource observation for both bounds, configured/byte/
growth/failure limits, and the selection-counter meaning. Real independent-event
passes drain all four source blocks and verify every row, indexed coverage,
watermark and complete recovery under both resource admission outcomes.

The main real-Runner workload seeds 64 prior blocks as 1,024 actual history trios
plus an event pair, then drains exactly 128 new blocks from a real Pebble source.
Each block has 16 transactions, two independent history changes per transaction,
and event logs with 1,024-byte full-entropy payloads. Source owning readers and
cold public queries must agree for every semantic value/duplicate, and event hashes cover
all positions, identities and protobuf bytes. The terminal history/event coverage
must reach the same fixed endpoint. `StageSnapshotBuild` and
`StageSnapshotEventLogBuild` must equal block 192 and bind its canonical hash.
`StageSnapshotHistory` and `StageSnapshotAccessor` instead store transaction
numbers: both must equal 3,072 and match `manifest.Progress` (seed 1,024).
No pending outer maintenance or derived-event plan may remain. Compact hot
rows omit BlockHash, so the independent oracle cross-checks StateTxRange against
the canonical block before hydrating that missing identity. It retains BlockHash
in comparisons. Segment-local Seq encodings need not equal raw source ordinals;
a sorted semantic-row multiset preserves duplicate counts, and the public
per-key query separately checks all 192 records in order and compares each
canonical identity and Prev value.

The fixture's controlled 250 ms/block density selects ranges only. Timed build,
publication, outer completion, user/sys CPU, actual metadata Write return bytes,
output file sizes, heap samples, process RSS high-water mark, maximum pass and
shared-gate hold are real observations. It expires completed recovery deadlines
between direct OnePass calls; recovery costs remain separately recorded. The
reported active lifecycle wall excludes those pauses and cannot be presented as
complete scheduler throughput. Process RSS includes setup and native buffers;
sampled Go heap is neither exact peak RSS nor a hard limit. Test-only Write
observers intercept the actual manifest/immutable/catalog writers and are absent
from the production candidate. No bytes are inferred by multiplying pass count.

The first six arms used the shared workspace containing unrelated WIP and are
retained explicitly as WIP-context mechanism evidence. A clean HEAD 462da0cb
source tree with only the two approved controller/test files, plus external
instrumentation overlays, supplies the final comparison. All 2,664 source-file
hashes match the independently verified clean tree before and after the six
arms. The source, external overlays, binaries and logs are frozen in
`cold-target/final-freeze-sha256.json`; the per-arm numbers and paired changes
are in `cold-target/publication-clean-summary.json`.

All three alternating AB/BA/AB pairs pass the same 6,144 historical semantic-row
and 3,072 event-row oracles, the complete stage checks above, output-file stat
accounting and absence of pending maintenance or derived event debt. Each begins
with 3,074 genuine segment refs and an 811,771-byte manifest. No signed catalog
or immutable catalog writes occur or contribute claimed savings.

| Observation | Baseline | Resource-admitted target |
| --- | ---: | ---: |
| Completed passes / actual manifest writes, every run | 16 / 16 | 9 / 9 |
| Actual manifest Write bytes, every run | 13,160,144 | 7,362,826 |
| New physical history/event output bytes, every run | 6,556,344 | 6,541,778 |
| Active lifecycle wall median, seconds | 1.311889 | 0.763736 |
| User + system CPU median, seconds | 0.318126 | 0.200485 |
| Total Go allocation median, bytes | 280,524,336 | 176,660,872 |
| Sampled Go heap peak median, bytes | 34,419,368 | 30,340,296 |
| Process RSS high-water median including seed, bytes | 89,341,952 | 87,949,312 |
| Maximum pass median, milliseconds | 90.557 | 89.420 |
| Maximum shared-lease hold median, milliseconds | 90.237 | 88.972 |
| Separately recorded required recovery, seconds | 4.00 | 2.25 |

Actual repeated manifest bytes fall 44.052% in every pair, while physical data
output falls only 0.222%. This supports metadata amortization rather than a
claim of fewer logical source records or a changed encoding. Baseline ranges
are sixteen batches of 8 blocks; admitted ranges are 10, 12, 15, then five
16-block batches and a final 11-block batch. The median paired active-wall
change is -42.02%, CPU -37.95%, and total Go allocation -38.11%, all limited to
this fixture and active lifecycle.

Peak costs are not guaranteed to improve: the third pair's sampled heap peak
increases from 34,978,136 to 36,025,032 bytes (+2.99%) and maximum shared lease
from 90.237 to 90.883 ms (+0.72%). Two process-RSS pairs increase slightly
(+0.13% and +0.92%); this includes setup and is not an isolated batch-memory
measurement. Neither the small fixture nor these observed maxima bound the
possible larger production burst or fairness cost.

The isolated clean verification also passes the complete `core/state/snapshots`,
`core/freezer` and `cmd/gtron` packages and 41 focused scheduling/race tests on
Go 1.25.5. Resolved evidence is under
`build/benchmarks/20261017-random-read-compaction/preparation/clean-validation/`.
The initial command's mistaken `core/chainfreezer` directory is retained as a
setup failure, with only the correct `core/freezer` full test supplemented.
Two test-generated Python bytecode files are explicitly accounted for as
non-source extras; the frozen 2,664 original source hashes and 27 unrelated WIP
hashes remain unchanged.

Select the guarded Runner target on this actual repeated-metadata reduction
and fixed-endpoint correctness evidence, subject to final independent source
and evidence audit. Acceptance cannot promise the complete mainnet catch-up
target: even an ideal doubled row batch with unchanged metadata cost suggests
only about a third more active throughput within this tier, before other work
and scheduling costs. Production validation must separately observe actual
selection counters, full hold/recovery, heap/RSS and cold endpoint progress.
