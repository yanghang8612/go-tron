# Cold build debt tiers and strict event source reuse

The October 13 sample shows growing compaction debt alongside low device latency,
no stalls and unused CPU. Debt growth remains a scheduling signal, but does not
prove device saturation. This change grades that signal without changing formats.

## Debt policy

Retain accepted engine observations, reset/freshness rules, the seven-point
30-second recent window, 90-second gap reset and the October 12 significant-growth
boundary `max(256MiB, baseline/100)`. The extended positive suffix may establish a
trend but cannot increase recent severity. Boundaries are inclusive (`>=`). Severe
growth is four times that boundary; multiplication is overflow checked.

Debt-only significant growth below severe uses 60% duty only with fresh engine
and device observations, device await in [0,2ms], positive L0 threshold and current
L0 strictly below it, no hard limit, no new stall and no device pressure. All
conditions use the same probe result. Unknown, stale, reset or severe evidence
cannot inherit middle-tier admission. Reused observations may retain an existing
middle tier but cannot promote a conservative observation without a newly
accepted sample. Freshness is also checked when consuming admission/duty.

The middle tier stays level 1 with 2-second/32MiB batch targets; its 60% duty takes
precedence over CPU burst duty. It may admit existing cache/pair CPU pipelines,
subject to their independent resource checks. Hard, stall, L0 and device guards
remain immediate. `history/budget/debt_tier` is 0 absent, 1 middle, 2 conservative.

There is no arbitrary absolute debt cap or persistent-growth anchor. Sustained
moderate growth can remain at 60%; existing space/hard protections remain and
online debt, stalls, latency and backlog require observation. These thresholds
are policy, not a disk bandwidth or safety proof.

## Event read validation

`ReadBlockAndTransactionInfosStrict` internally reads canonical raw body bytes,
uses the existing strict borrowed block decoder and checks block number before
reading strict receipts. It never accepts a caller-provided block as authority.
The returned block borrows immutable returned raw bytes. Nested protobuf parsing,
unknown-field handling, full transaction hashes and receipt coverage remain.
No custom projection parser or receipt-field skipping is introduced.

For an explicitly pinned KV view, external-log hydration reuses only that
internally decoded block. Its coverage, block/transaction hash, ordinal, index,
log count and receipt ID checks remain. Live views retain the independent block
read during hydration, including its errors. Cached event identity hits retain
full raw-body SHA authentication and strict receipt reads. Generic FromReader
keeps two passes; the existing 8MiB replay budget/overflow behavior is unchanged.

The snapshot facade preserves ChainDB ancient and private event sidecar routing.
Pinning makes hot point/iterator reads coherent; ancient data and sidecar coverage
retain their existing solid-range/immutable-data contracts, not a cross-store
atomic snapshot. Snapshot close failure still prevents returning a valid ref.

## Verification

Boundary/protection tests cover middle/severe, L0/device/engine unknown or stale,
new stalls, hard pressure, reset, duty ordering and unchanged batch targets.
External receipt tests compare complete protobuf output, verify fewer body reads,
retain live-path reads and reject wrong log identities/counts. Existing ancient,
source mutation, malformed body/receipt, fallback and publication tests remain.
`BenchmarkColdEventDecode` uses complete FromChain V4 output for 64 blocks of
32 transactions with two 2048-byte contracts each, inline and external receipts;
it runs unchanged on ea3cb83b for fixed-input comparisons.

## Commitment branch CPU and allocation follow-up

Branch node hashing keeps the exact `0x01 || nibble || childHash` preimage and
fold statistics. Hardware-supported amd64/arm64 dispatch uses fastkeccak's
one-shot API; purego, other architectures and native CPUs without the library's
required BMI1+BMI2/SHA3 features keep the pooled streaming path. Both paths must
remain allocation free. No crypto implementation or consensus algorithm changes.

An indexed-fill branch encoding prototype regressed the fixed-input encode
microbenchmarks and was rejected. The original append-format encoder remains;
an independent append-format oracle checks sparse masks and leaf variants.

After all partition owners join, compose buffers at most sixteen depth-one
mutations through the existing pooled bufferedBranchStore and flushes once with
batchCount=1/reserveHint=0. The existing owned-arena API creates final immutable
values/keys; this is not a pooled live-layer arena. Depth-one rows have no owner
readers across blocks. Root writes stay after composition and parent-read close.
Flush errors fail the job before root/publication. No FIFO, early descendant
publication, partition ownership, collapse, rewind or snapshot ordering changes.
Existing five-distribution four-block commit+flush benchmarks decide whether
these changes help the complete pipeline; microbenchmarks alone are insufficient.
